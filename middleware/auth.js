const jwt = require('jsonwebtoken');
const { pool } = require('../config/database');

async function authenticateSocket(socket, next) {
    try {
        const token = socket.handshake.auth.token;
        
        if (!token) {
            return next(new Error('Authentication required'));
        }
        
        // Verify JWT token
        const decoded = jwt.verify(token, process.env.JWT_SECRET);
        
        // Get user from database
        const [rows] = await pool.query(
            'SELECT id, name, profile_image FROM users WHERE id = ?',
            [decoded.userId]
        );
        
        if (rows.length === 0) {
            return next(new Error('User not found'));
        }
        
        const user = rows[0];
        
        // Attach user data to socket
        socket.data.userId = user.id;
        socket.data.userName = user.name;
        socket.data.userImage = user.profile_image;
        
        next();
    } catch (error) {
        console.error('Auth error:', error);
        next(new Error('Invalid token'));
    }
}

module.exports = { authenticateSocket };