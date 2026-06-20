// server.js - Updated WebSocket Server (No Auto-Offline)
const WebSocket = require('ws');
const mysql = require('mysql2/promise');
const http = require('http');

// Database configuration
const dbConfig = {
    host: process.env.DB_HOST || 'srv657.hstgr.io',
    user: process.env.DB_USER || 'u442108067_rajithawalpola',
    password: process.env.DB_PASSWORD || '12IEhou:P',
    database: process.env.DB_NAME || 'u442108067_testdb',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

console.log('📊 Database Config:', {
    host: dbConfig.host,
    user: dbConfig.user,
    database: dbConfig.database
});

// Create HTTP server
const server = http.createServer((req, res) => {
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            status: 'healthy', 
            timestamp: new Date().toISOString(),
            connections: wss ? wss.clients.size : 0
        }));
        return;
    }
    
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
                <p>Database: ${dbConfig.database}</p>
                <p>Host: ${dbConfig.host}</p>
                <p style="color:green;">✅ Auto-offline is DISABLED - Drivers stay online until manual disconnect</p>
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
const connectedDrivers = new Map();

// Database connection pool
let pool = null;

async function initDatabase() {
    try {
        pool = mysql.createPool(dbConfig);
        console.log('✅ Database connection pool created');
        
        const connection = await pool.getConnection();
        console.log('✅ Database connected successfully');
        connection.release();
        
        await createLocationsTable();
        return true;
    } catch (error) {
        console.error('❌ Database connection failed:', error.message);
        console.error('Full error:', error);
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
            INDEX idx_last_update (last_update)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `;
    
    try {
        await pool.execute(createTableSQL);
        console.log('✅ Driver locations table ready');
        
        try {
            await pool.execute(`
                ALTER TABLE drivers 
                ADD COLUMN IF NOT EXISTS last_latitude DECIMAL(10, 8) NULL,
                ADD COLUMN IF NOT EXISTS last_longitude DECIMAL(11, 8) NULL,
                ADD COLUMN IF NOT EXISTS last_location_update TIMESTAMP NULL
            `);
            console.log('✅ Drivers table updated with location columns');
        } catch (alterError) {
            console.log('⚠️ Could not alter drivers table (columns may already exist):', alterError.message);
        }
    } catch (error) {
        console.error('❌ Failed to create locations table:', error.message);
        console.error('Full error:', error);
        throw error;
    }
}

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
        
        // Check if driver exists
        const [checkDriver] = await pool.execute(
            'SELECT id FROM drivers WHERE id = ? AND status != "suspended"',
            [driverId]
        );
        
        if (checkDriver.length === 0) {
            console.error('❌ Driver not found or suspended:', driverId);
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
        
        const [result] = await pool.execute(query, [
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
        console.error('Full error:', error);
        return false;
    }
}

async function updateDriverOffline(driverId) {
    if (!pool) {
        console.error('❌ Database not initialized');
        return false;
    }
    
    try {
        // Update driver_locations table status to offline
        const locationQuery = `
            UPDATE driver_locations 
            SET status = 'offline', last_update = NOW() 
            WHERE driver_id = ?
        `;
        await pool.execute(locationQuery, [driverId]);
        console.log(`✅ Driver ${driverId} marked offline in driver_locations`);
        
        // Also update drivers table status
        const driverQuery = `
            UPDATE drivers 
            SET status = 'inactive' 
            WHERE id = ?
        `;
        await pool.execute(driverQuery, [driverId]);
        console.log(`✅ Driver ${driverId} marked offline in drivers`);
        
        return true;
    } catch (error) {
        console.error('❌ Failed to update offline status:', error.message);
        return false;
    }
}

// Broadcast location to all connected clients
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

// WebSocket connection handler
wss.on('connection', (ws, req) => {
    console.log('🟢 New client connected');
    let driverId = null;
    let isDriver = false;
    
    ws.send(JSON.stringify({
        type: 'welcome',
        message: 'Connected to Drivee WebSocket Server',
        timestamp: new Date().toISOString()
    }));
    
    ws.on('message', async (data) => {
        try {
            const message = JSON.parse(data.toString());
            console.log('📨 Received:', message.type);
            
            switch (message.type) {
                case 'driver_register':
                    driverId = message.driver_id;
                    isDriver = true;
                    
                    if (!driverId || driverId <= 0) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Invalid driver ID'
                        }));
                        return;
                    }
                    
                    connectedDrivers.set(driverId, {
                        ws: ws,
                        location: null,
                        lastUpdate: Date.now()
                    });
                    
                    console.log(`✅ Driver ${driverId} registered`);
                    
                    ws.send(JSON.stringify({
                        type: 'registration_success',
                        driver_id: driverId,
                        message: 'Driver registered successfully'
                    }));
                    break;
                    
                case 'driver_location_update':
                    const updateDriverId = message.driver_id;
                    
                    if (!updateDriverId || !message.location) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Missing driver_id or location data'
                        }));
                        return;
                    }
                    
                    const { latitude, longitude, accuracy, speed, heading, altitude, status } = message.location;
                    
                    if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Invalid coordinates'
                        }));
                        return;
                    }
                    
                    // Update connection data
                    if (connectedDrivers.has(updateDriverId)) {
                        const driverData = connectedDrivers.get(updateDriverId);
                        driverData.location = { latitude, longitude };
                        driverData.lastUpdate = Date.now();
                        connectedDrivers.set(updateDriverId, driverData);
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
                    
                    const saved = await updateDriverLocation(updateDriverId, locationData);
                    
                    if (saved) {
                        broadcastLocation(updateDriverId, locationData, ws);
                        
                        ws.send(JSON.stringify({
                            type: 'location_ack',
                            driver_id: updateDriverId,
                            timestamp: new Date().toISOString()
                        }));
                        console.log(`✅ Location saved for driver ${updateDriverId}`);
                    } else {
                        console.error(`❌ Failed to save location for driver ${updateDriverId}`);
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Failed to save location'
                        }));
                    }
                    break;
                    
                case 'get_nearby_drivers':
                    // Simple response for now
                    ws.send(JSON.stringify({
                        type: 'nearby_drivers',
                        drivers: [],
                        timestamp: new Date().toISOString()
                    }));
                    break;
                    
                case 'driver_offline':
                    console.log('🔴 Driver going offline:', message.driver_id);
                    const offlineDriverId = message.driver_id;
                    
                    if (offlineDriverId) {
                        // Remove from connected drivers
                        connectedDrivers.delete(offlineDriverId);
                        
                        // Update database status to offline
                        await updateDriverOffline(offlineDriverId);
                        
                        // Broadcast to other clients
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
                            driver_id: offlineDriverId,
                            message: 'Driver is now offline'
                        }));
                    }
                    break;
                    
                case 'heartbeat':
                    // Respond to heartbeat
                    ws.send(JSON.stringify({
                        type: 'heartbeat_ack',
                        timestamp: Date.now()
                    }));
                    // Update lastUpdate for this driver
                    if (driverId && connectedDrivers.has(driverId)) {
                        const driverData = connectedDrivers.get(driverId);
                        driverData.lastUpdate = Date.now();
                        connectedDrivers.set(driverId, driverData);
                    }
                    break;
                    
                default:
                    console.log('⚠️ Unknown message type:', message.type);
            }
        } catch (error) {
            console.error('❌ Error processing message:', error.message);
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Failed to process message'
            }));
        }
    });
    
    ws.on('close', async () => {
        console.log('🔴 Client disconnected');
        if (driverId && isDriver) {
            connectedDrivers.delete(driverId);
            console.log(`🔴 Driver ${driverId} removed from connected list`);
            
            // Check if this was an intentional disconnect
            // The driver should send 'driver_offline' before disconnecting for intentional offline
            // We'll check if the driver is still in the connected drivers list with a delay
            // If not, mark offline after a short delay to allow for reconnection
            setTimeout(async () => {
                // Check if driver reconnected
                if (!connectedDrivers.has(driverId)) {
                    console.log(`🔴 Driver ${driverId} did not reconnect, marking offline`);
                    await updateDriverOffline(driverId);
                } else {
                    console.log(`✅ Driver ${driverId} reconnected, keeping online`);
                }
            }, 5000); // 5 second delay to allow for reconnection
        }
    });
    
    ws.on('error', (error) => {
        console.error('❌ WebSocket error:', error.message);
    });
});

// Start server
const PORT = process.env.PORT || 8080;

async function startServer() {
    const dbInitialized = await initDatabase();
    
    if (!dbInitialized) {
        console.error('❌ Failed to initialize database. Location saving will be disabled.');
    }
    
    server.listen(PORT, () => {
        console.log(`🚗 Drivee WebSocket Server running on port ${PORT}`);
        console.log(`📍 WebSocket URL: ws://localhost:${PORT}`);
        console.log(`📊 Health check: http://localhost:${PORT}/health`);
        console.log('✅ Server ready');
        console.log('🟢 Auto-offline is DISABLED - Drivers stay online until manual disconnect');
    });
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('🛑 Shutting down...');
    
    // Mark all drivers offline
    if (pool) {
        try {
            await pool.execute('UPDATE driver_locations SET status = ?', ['offline']);
            await pool.execute('UPDATE drivers SET status = ?', ['inactive']);
            console.log('✅ All drivers marked offline');
        } catch (error) {
            console.error('❌ Failed to mark drivers offline:', error.message);
        }
        
        await pool.end();
        console.log('✅ Database connections closed');
    }
    
    wss.close(() => {
        console.log('✅ WebSocket server closed');
        process.exit(0);
    });
});

startServer();
