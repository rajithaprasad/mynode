const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
require('dotenv').config();

const { pool, testConnection } = require('./config/database');
const { authenticateSocket } = require('./middleware/auth');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
        credentials: true,
    },
    pingTimeout: 60000,
    pingInterval: 25000,
});

// Middleware
app.use(cors());
app.use(helmet());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Store active users
const activeUsers = new Map();

// Authentication middleware for Socket.io
io.use(authenticateSocket);

// ---------- SOCKET.IO EVENTS ----------
io.on('connection', (socket) => {
    const userId = socket.data.userId;
    const userName = socket.data.userName;
    
    console.log(`🔵 User connected: ${userId} (${userName})`);
    
    // Store user connection
    activeUsers.set(userId, socket.id);
    
    // Broadcast user online status
    io.emit('user_online', { userId, userName, status: 'online' });
    
    // ---------- JOIN CHAT ROOM ----------
    socket.on('join_chat', async ({ conversationId }) => {
        const roomName = `chat_${conversationId}`;
        
        // Leave previous rooms
        const rooms = Array.from(socket.rooms);
        rooms.forEach(room => {
            if (room.startsWith('chat_')) {
                socket.leave(room);
            }
        });
        
        // Join new room
        socket.join(roomName);
        socket.data.currentRoom = roomName;
        
        console.log(`📩 User ${userId} joined chat: ${conversationId}`);
        
        // Send chat history (last 50 messages)
        try {
            const messages = await getChatHistory(conversationId, 50);
            socket.emit('chat_history', {
                conversationId,
                messages,
                hasMore: messages.length === 50,
            });
            
            // Mark messages as read
            await markMessagesAsRead(conversationId, userId);
            
            // Notify others that user has joined
            socket.to(roomName).emit('user_joined', {
                userId,
                userName,
            });
        } catch (error) {
            console.error('Error loading chat history:', error);
            socket.emit('error', { message: 'Failed to load chat history' });
        }
    });
    
    // ---------- SEND MESSAGE ----------
    socket.on('send_message', async (data) => {
        try {
            const { conversationId, content, messageType = 'text' } = data;
            
            if (!content || !conversationId) {
                socket.emit('error', { message: 'Missing required fields' });
                return;
            }
            
            // Save message to MySQL
            const message = await saveMessage({
                conversationId,
                senderId: userId,
                content,
                messageType,
            });
            
            // Prepare message for broadcast
            const messageData = {
                id: message.id,
                conversationId,
                senderId: userId,
                senderName: userName,
                senderImage: socket.data.userImage,
                content,
                messageType,
                createdAt: message.createdAt,
                is_read: 0,
            };
            
            // Broadcast to room
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            
            // Update conversation timestamp
            await updateConversationTimestamp(conversationId);
            
        } catch (error) {
            console.error('Error sending message:', error);
            socket.emit('error', { message: 'Failed to send message' });
        }
    });
    
    // ---------- TYPING INDICATOR ----------
    socket.on('typing', ({ conversationId, isTyping }) => {
        const roomName = `chat_${conversationId}`;
        socket.to(roomName).emit('user_typing', {
            userId,
            userName,
            isTyping,
        });
    });
    
    // ---------- MARK MESSAGES AS READ ----------
    socket.on('mark_read', async ({ conversationId }) => {
        try {
            await markMessagesAsRead(conversationId, userId);
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('messages_read', {
                userId,
                conversationId,
            });
        } catch (error) {
            console.error('Error marking as read:', error);
        }
    });
    
    // ---------- DISCONNECT ----------
    socket.on('disconnect', () => {
        console.log(`🔴 User disconnected: ${userId}`);
        
        // Remove from active users
        activeUsers.delete(userId);
        
        // Broadcast offline status
        io.emit('user_offline', { userId, userName, status: 'offline' });
    });
});

// ---------- DATABASE FUNCTIONS ----------

async function getChatHistory(conversationId, limit = 50, offset = 0) {
    const [rows] = await pool.query(
        `SELECT 
            m.id,
            m.conversation_id as conversationId,
            m.sender_id as senderId,
            m.content,
            m.message_type as messageType,
            m.is_read as isRead,
            m.created_at as createdAt,
            u.name as senderName,
            u.profile_image as senderImage
        FROM messages m
        JOIN users u ON m.sender_id = u.id
        WHERE m.conversation_id = ? AND m.is_deleted = 0
        ORDER BY m.created_at DESC
        LIMIT ? OFFSET ?`,
        [conversationId, limit, offset]
    );
    
    return rows.reverse().map(row => ({
        ...row,
        createdAt: row.createdAt.toISOString(),
    }));
}

async function saveMessage({ conversationId, senderId, content, messageType }) {
    const [result] = await pool.query(
        `INSERT INTO messages 
        (conversation_id, sender_id, content, message_type, contains_contact_info)
        VALUES (?, ?, ?, ?, ?)`,
        [conversationId, senderId, content, messageType, 0]
    );
    
    const [rows] = await pool.query(
        `SELECT 
            id,
            conversation_id as conversationId,
            sender_id as senderId,
            content,
            message_type as messageType,
            is_read as isRead,
            created_at as createdAt
        FROM messages 
        WHERE id = ?`,
        [result.insertId]
    );
    
    return {
        ...rows[0],
        createdAt: rows[0].createdAt.toISOString(),
    };
}

async function markMessagesAsRead(conversationId, userId) {
    await pool.query(
        `UPDATE messages 
        SET is_read = 1, read_at = NOW()
        WHERE conversation_id = ? 
        AND sender_id != ?
        AND is_read = 0`,
        [conversationId, userId]
    );
}

async function updateConversationTimestamp(conversationId) {
    await pool.query(
        `UPDATE conversations 
        SET last_message_at = NOW()
        WHERE id = ?`,
        [conversationId]
    );
}

// ---------- START SERVER ----------

const PORT = process.env.PORT || 3000;

async function startServer() {
    await testConnection();
    
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`🚀 WebSocket server running on port ${PORT}`);
        console.log(`📡 Socket.io ready for connections`);
    });
}

startServer().catch(console.error);

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('Shutting down gracefully...');
    server.close(() => {
        pool.end();
        process.exit(0);
    });
});

module.exports = { io, server };
