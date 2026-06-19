// server.js - WebSocket Server for Real-time Driver Location Tracking
const WebSocket = require('ws');
const mysql = require('mysql2/promise');
const http = require('http');

// Database configuration
const dbConfig = {
    host: 'srv657.hstgr.io', // or '77.37.35.160'
    user: 'u442108067_rajithawalpola',
    password: '12IEhou:P',
    database: 'u442108067_testdb',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// Create HTTP server
const server = http.createServer((req, res) => {
    // Health check endpoint
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            status: 'healthy', 
            timestamp: new Date().toISOString(),
            connections: wss.clients.size 
        }));
        return;
    }
    
    // Simple status page
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
            <!DOCTYPE html>
            <html>
            <head><title>Drivee WebSocket Server</title></head>
            <body>
                <h1>🚗 Drivee WebSocket Server</h1>
                <p>Status: Running</p>
                <p>Active Connections: ${wss ? wss.clients.size : 0}</p>
                <p>WebSocket URL: ws://${req.headers.host}</p>
                <hr>
                <p>Endpoints:</p>
                <ul>
                    <li><strong>WebSocket:</strong> ws://${req.headers.host}</li>
                    <li><strong>Health:</strong> ${req.headers.host}/health</li>
                </ul>
            </body>
            </html>
        `);
        return;
    }
    
    res.writeHead(404);
    res.end('Not Found');
});

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Store connected drivers
const connectedDrivers = new Map(); // driverId -> { ws, location, lastUpdate }

// Database connection pool
let pool = null;

async function initDatabase() {
    try {
        pool = mysql.createPool(dbConfig);
        console.log('✅ Database connection pool created');
        
        // Test connection
        const connection = await pool.getConnection();
        console.log('✅ Database connected successfully');
        connection.release();
        
        // Create locations table if not exists
        await createLocationsTable();
        
        return true;
    } catch (error) {
        console.error('❌ Database connection failed:', error.message);
        return false;
    }
}

async function createLocationsTable() {
    const createTableSQL = `
        CREATE TABLE IF NOT EXISTS driver_locations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            driver_id INT NOT NULL UNIQUE,
            latitude DECIMAL(10, 8) NOT NULL,
            longitude DECIMAL(11, 8) NOT NULL,
            accuracy DECIMAL(10, 2),
            speed DECIMAL(6, 2),
            heading DECIMAL(6, 2),
            altitude DECIMAL(8, 2),
            status ENUM('online', 'offline', 'busy') DEFAULT 'online',
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_driver_id (driver_id),
            INDEX idx_status (status),
            INDEX idx_last_update (last_update),
            FOREIGN KEY (driver_id) REFERENCES drivers(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `;
    
    try {
        await pool.execute(createTableSQL);
        console.log('✅ Driver locations table ready');
    } catch (error) {
        console.error('❌ Failed to create locations table:', error.message);
        throw error;
    }
}

// Update driver location in database
async function updateDriverLocation(driverId, locationData) {
    if (!pool) {
        console.error('❌ Database not initialized');
        return false;
    }
    
    try {
        const { 
            latitude, 
            longitude, 
            accuracy = null, 
            speed = null, 
            heading = null, 
            altitude = null,
            status = 'online'
        } = locationData;
        
        // Validate coordinates
        if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
            console.error('❌ Invalid coordinates:', { latitude, longitude });
            return false;
        }
        
        // UPSERT query - insert or update
        const query = `
            INSERT INTO driver_locations 
                (driver_id, latitude, longitude, accuracy, speed, heading, altitude, status, last_update)
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON DUPLICATE KEY UPDATE 
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                accuracy = VALUES(accuracy),
                speed = VALUES(speed),
                heading = VALUES(heading),
                altitude = VALUES(altitude),
                status = VALUES(status),
                last_update = NOW()
        `;
        
        await pool.execute(query, [
            driverId,
            latitude,
            longitude,
            accuracy,
            speed,
            heading,
            altitude,
            status
        ]);
        
        // Also update drivers table with last known location
        const updateDriverSQL = `
            UPDATE drivers 
            SET 
                last_latitude = ?,
                last_longitude = ?,
                last_location_update = NOW()
            WHERE id = ?
        `;
        
        await pool.execute(updateDriverSQL, [latitude, longitude, driverId]);
        
        return true;
    } catch (error) {
        console.error('❌ Failed to update location:', error.message);
        return false;
    }
}

// Get nearby drivers
async function getNearbyDrivers(latitude, longitude, radiusKm = 5, excludeDriverId = null) {
    if (!pool) {
        console.error('❌ Database not initialized');
        return [];
    }
    
    try {
        // Calculate distance using Haversine formula
        const query = `
            SELECT 
                d.id,
                d.full_name,
                d.rating,
                d.total_trips,
                dl.latitude,
                dl.longitude,
                dl.status,
                dl.last_update,
                (
                    6371 * ACOS(
                        COS(RADIANS(?)) * COS(RADIANS(dl.latitude)) * 
                        COS(RADIANS(dl.longitude) - RADIANS(?)) + 
                        SIN(RADIANS(?)) * SIN(RADIANS(dl.latitude))
                    )
                ) AS distance_km
            FROM driver_locations dl
            JOIN drivers d ON d.id = dl.driver_id
            WHERE 
                dl.status IN ('online', 'busy')
                AND d.status = 'active'
                ${excludeDriverId ? 'AND d.id != ?' : ''}
                AND TIMESTAMPDIFF(MINUTE, dl.last_update, NOW()) < 5
            HAVING distance_km <= ?
            ORDER BY distance_km ASC
        `;
        
        const params = [latitude, longitude, latitude];
        if (excludeDriverId) {
            params.push(excludeDriverId);
        }
        params.push(radiusKm);
        
        const [rows] = await pool.execute(query, params);
        return rows;
    } catch (error) {
        console.error('❌ Failed to get nearby drivers:', error.message);
        return [];
    }
}

// Broadcast location to all connected clients (except sender)
function broadcastLocation(driverId, locationData, excludeClient = null) {
    const message = JSON.stringify({
        type: 'driver_location_update',
        driver_id: driverId,
        location: locationData,
        timestamp: new Date().toISOString()
    });
    
    wss.clients.forEach((client) => {
        if (client !== excludeClient && client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

// Broadcast nearby drivers to all clients
async function broadcastNearbyDrivers(latitude, longitude, radiusKm = 5) {
    try {
        const drivers = await getNearbyDrivers(latitude, longitude, radiusKm);
        const message = JSON.stringify({
            type: 'nearby_drivers',
            drivers: drivers,
            timestamp: new Date().toISOString()
        });
        
        wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(message);
            }
        });
    } catch (error) {
        console.error('❌ Failed to broadcast nearby drivers:', error.message);
    }
}

// Clean up inactive connections
async function cleanupInactiveDrivers() {
    const now = Date.now();
    const timeout = 60000; // 1 minute timeout
    
    for (const [driverId, data] of connectedDrivers.entries()) {
        if (now - data.lastUpdate > timeout) {
            console.log(`🟡 Cleaning up inactive driver ${driverId}`);
            
            // Update status to offline in database
            if (pool) {
                try {
                    await pool.execute(
                        'UPDATE driver_locations SET status = ? WHERE driver_id = ?',
                        ['offline', driverId]
                    );
                } catch (error) {
                    console.error('❌ Failed to update offline status:', error.message);
                }
            }
            
            connectedDrivers.delete(driverId);
            
            // Broadcast driver offline
            const offlineMessage = JSON.stringify({
                type: 'driver_offline',
                driver_id: driverId,
                timestamp: new Date().toISOString()
            });
            
            wss.clients.forEach((client) => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(offlineMessage);
                }
            });
        }
    }
}

// WebSocket connection handler
wss.on('connection', (ws, req) => {
    console.log('🟢 New client connected');
    let driverId = null;
    let isDriver = false;
    
    // Send welcome message
    ws.send(JSON.stringify({
        type: 'welcome',
        message: 'Connected to Drivee WebSocket Server',
        timestamp: new Date().toISOString()
    }));
    
    // Handle incoming messages
    ws.on('message', async (data) => {
        try {
            const message = JSON.parse(data.toString());
            console.log('📨 Received:', message.type);
            
            switch (message.type) {
                case 'driver_register':
                    // Driver registers with their ID
                    driverId = message.driver_id;
                    isDriver = true;
                    
                    if (!driverId || driverId <= 0) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Invalid driver ID'
                        }));
                        return;
                    }
                    
                    // Store connection
                    connectedDrivers.set(driverId, {
                        ws: ws,
                        location: null,
                        lastUpdate: Date.now()
                    });
                    
                    console.log(`✅ Driver ${driverId} registered`);
                    
                    // Send confirmation
                    ws.send(JSON.stringify({
                        type: 'registration_success',
                        driver_id: driverId,
                        message: 'Driver registered successfully'
                    }));
                    
                    // Send initial nearby drivers
                    if (message.location) {
                        const { latitude, longitude } = message.location;
                        const nearby = await getNearbyDrivers(latitude, longitude, 5, driverId);
                        ws.send(JSON.stringify({
                            type: 'nearby_drivers',
                            drivers: nearby,
                            timestamp: new Date().toISOString()
                        }));
                    }
                    break;
                    
                case 'driver_location_update':
                    // Update driver location
                    driverId = message.driver_id;
                    
                    if (!driverId || !message.location) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Missing driver_id or location data'
                        }));
                        return;
                    }
                    
                    const { latitude, longitude, accuracy, speed, heading, altitude, status } = message.location;
                    
                    // Validate coordinates
                    if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Invalid coordinates'
                        }));
                        return;
                    }
                    
                    // Update connection data
                    if (connectedDrivers.has(driverId)) {
                        const driverData = connectedDrivers.get(driverId);
                        driverData.location = { latitude, longitude };
                        driverData.lastUpdate = Date.now();
                        connectedDrivers.set(driverId, driverData);
                    }
                    
                    // Save to database
                    const locationData = {
                        latitude,
                        longitude,
                        accuracy: accuracy || null,
                        speed: speed || null,
                        heading: heading || null,
                        altitude: altitude || null,
                        status: status || 'online'
                    };
                    
                    const saved = await updateDriverLocation(driverId, locationData);
                    
                    if (saved) {
                        // Broadcast to all other connected clients
                        broadcastLocation(driverId, locationData, ws);
                        
                        // Send confirmation to driver
                        ws.send(JSON.stringify({
                            type: 'location_ack',
                            driver_id: driverId,
                            timestamp: new Date().toISOString()
                        }));
                    } else {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Failed to save location'
                        }));
                    }
                    break;
                    
                case 'get_nearby_drivers':
                    // Get nearby drivers
                    const { lat, lng, radius = 5 } = message;
                    
                    if (!lat || !lng) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Missing latitude or longitude'
                        }));
                        return;
                    }
                    
                    const nearbyDrivers = await getNearbyDrivers(lat, lng, radius);
                    ws.send(JSON.stringify({
                        type: 'nearby_drivers',
                        drivers: nearbyDrivers,
                        timestamp: new Date().toISOString()
                    }));
                    break;
                    
                case 'driver_offline':
                    // Driver going offline
                    const offlineDriverId = message.driver_id;
                    
                    if (offlineDriverId) {
                        if (connectedDrivers.has(offlineDriverId)) {
                            connectedDrivers.delete(offlineDriverId);
                        }
                        
                        // Update database status
                        if (pool) {
                            await pool.execute(
                                'UPDATE driver_locations SET status = ? WHERE driver_id = ?',
                                ['offline', offlineDriverId]
                            );
                        }
                        
                        // Broadcast offline status
                        const offlineMsg = JSON.stringify({
                            type: 'driver_offline',
                            driver_id: offlineDriverId,
                            timestamp: new Date().toISOString()
                        });
                        
                        wss.clients.forEach((client) => {
                            if (client !== ws && client.readyState === WebSocket.OPEN) {
                                client.send(offlineMsg);
                            }
                        });
                        
                        ws.send(JSON.stringify({
                            type: 'offline_confirmed',
                            driver_id: offlineDriverId
                        }));
                    }
                    break;
                    
                default:
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: `Unknown message type: ${message.type}`
                    }));
            }
        } catch (error) {
            console.error('❌ Error processing message:', error.message);
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Failed to process message'
            }));
        }
    });
    
    // Handle disconnection
    ws.on('close', async () => {
        console.log('🔴 Client disconnected');
        
        if (driverId && isDriver) {
            // Remove from connected drivers
            connectedDrivers.delete(driverId);
            
            // Update status to offline in database
            if (pool) {
                try {
                    await pool.execute(
                        'UPDATE driver_locations SET status = ? WHERE driver_id = ?',
                        ['offline', driverId]
                    );
                    console.log(`✅ Driver ${driverId} marked offline`);
                } catch (error) {
                    console.error('❌ Failed to update offline status:', error.message);
                }
            }
            
            // Broadcast driver offline
            const offlineMessage = JSON.stringify({
                type: 'driver_offline',
                driver_id: driverId,
                timestamp: new Date().toISOString()
            });
            
            wss.clients.forEach((client) => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(offlineMessage);
                }
            });
        }
    });
    
    // Handle errors
    ws.on('error', (error) => {
        console.error('❌ WebSocket error:', error.message);
    });
});

// Periodic cleanup (every 30 seconds)
setInterval(cleanupInactiveDrivers, 30000);

// Periodic broadcast of nearby drivers (every 10 seconds)
setInterval(async () => {
    // For each connected driver, broadcast nearby drivers
    for (const [driverId, data] of connectedDrivers.entries()) {
        if (data.location) {
            const { latitude, longitude } = data.location;
            await broadcastNearbyDrivers(latitude, longitude, 5);
        }
    }
}, 10000);

// Start server
const PORT = process.env.PORT || 8080;

// Initialize database before starting
async function startServer() {
    const dbInitialized = await initDatabase();
    
    if (!dbInitialized) {
        console.error('❌ Failed to initialize database. Server will still start but location saving will be disabled.');
    }
    
    server.listen(PORT, () => {
        console.log(`🚗 Drivee WebSocket Server running on port ${PORT}`);
        console.log(`📍 WebSocket URL: ws://localhost:${PORT}`);
        console.log(`📊 Health check: http://localhost:${PORT}/health`);
        console.log(`📈 Active connections: 0`);
        console.log('✅ Server ready');
    });
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('🛑 Shutting down...');
    
    // Mark all drivers offline
    if (pool) {
        try {
            await pool.execute('UPDATE driver_locations SET status = ?', ['offline']);
            console.log('✅ All drivers marked offline');
        } catch (error) {
            console.error('❌ Failed to mark drivers offline:', error.message);
        }
        
        await pool.end();
        console.log('✅ Database connections closed');
    }
    
    // Close WebSocket connections
    wss.close(() => {
        console.log('✅ WebSocket server closed');
        process.exit(0);
    });
});

startServer();
