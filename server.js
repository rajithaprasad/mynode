// server.js - Complete WebSocket Server with FIXED Database Query
const WebSocket = require('ws');
const mysql = require('mysql2/promise');
const http = require('http');

// Database configuration - SAME as your working PHP
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
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    
    if (req.method === 'OPTIONS') {
        res.writeHead(200);
        res.end();
        return;
    }
    
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            status: 'healthy', 
            timestamp: new Date().toISOString(),
            connections: wss ? wss.clients.size : 0
        }));
        return;
    }
    
    if (req.url === '/debug-drivers') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        debugDatabase().then(result => {
            res.end(JSON.stringify(result));
        }).catch(error => {
            res.end(JSON.stringify({
                success: false,
                error: error.message,
                stack: error.stack
            }));
        });
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
                <p><a href="/debug-drivers">🔍 Debug: View All Drivers</a></p>
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
        
        return true;
    } catch (error) {
        console.error('❌ Database connection failed:', error.message);
        console.error('Full error:', error);
        return false;
    }
}

// ============================================================
// DEBUG DATABASE - Shows all tables and data
// ============================================================
async function debugDatabase() {
    if (!pool) {
        return { success: false, error: 'Database not initialized' };
    }
    
    const result = {
        success: true,
        timestamp: new Date().toISOString(),
        tables: {},
        drivers: [],
        count: 0
    };
    
    try {
        // Show all tables
        const [tables] = await pool.execute("SHOW TABLES");
        result.tables.all = tables.map(row => Object.values(row)[0]);
        
        // Show drivers table structure
        try {
            const [columns] = await pool.execute("DESCRIBE drivers");
            result.tables.drivers_structure = columns;
        } catch (e) {
            result.tables.drivers_structure_error = e.message;
        }
        
        // Count all drivers
        const [countResult] = await pool.execute("SELECT COUNT(*) as total FROM drivers");
        result.total_drivers = countResult[0].total;
        
        // Count active drivers
        const [activeCount] = await pool.execute("SELECT COUNT(*) as total FROM drivers WHERE status = 'active'");
        result.active_drivers = activeCount[0].total;
        
        // Get all drivers with their locations - EXACT same query as your PHP
        const query = `
            SELECT 
                d.id,
                d.full_name,
                d.rating,
                d.total_trips,
                d.driver_type,
                d.vehicle_make,
                d.vehicle_model,
                d.vehicle_color,
                d.vehicle_plate,
                d.rate_per_km,
                d.vehicle_rate_per_km,
                d.display_rate,
                d.front_image,
                d.back_image,
                d.side_image,
                d.status as driver_status,
                COALESCE(dl.latitude, d.last_latitude, 0) as latitude,
                COALESCE(dl.longitude, d.last_longitude, 0) as longitude,
                COALESCE(dl.status, 'online') as status,
                dl.last_update
            FROM drivers d
            LEFT JOIN driver_locations dl ON d.id = dl.driver_id
            WHERE d.status = 'active'
            ORDER BY d.full_name ASC
        `;
        
        const [rows] = await pool.execute(query);
        result.drivers = rows;
        result.count = rows.length;
        
        // Also try a simpler query to see if any drivers exist
        const [simpleRows] = await pool.execute("SELECT id, full_name, status FROM drivers LIMIT 10");
        result.simple_drivers = simpleRows;
        
        return result;
    } catch (error) {
        console.error('❌ Debug failed:', error);
        result.success = false;
        result.error = error.message;
        result.stack = error.stack;
        return result;
    }
}

// ============================================================
// GET ALL DRIVERS - EXACTLY MATCHING YOUR PHP API
// ============================================================
async function getAllDrivers() {
    if (!pool) {
        console.error('❌ Database not initialized');
        return [];
    }
    
    try {
        // EXACT same query as your working get-all-drivers.php
        const query = `
            SELECT 
                d.id,
                d.full_name,
                d.rating,
                d.total_trips,
                d.driver_type,
                d.vehicle_make,
                d.vehicle_model,
                d.vehicle_color,
                d.vehicle_plate,
                d.rate_per_km,
                d.vehicle_rate_per_km,
                d.display_rate,
                d.front_image,
                d.back_image,
                d.side_image,
                d.status as driver_status,
                COALESCE(dl.latitude, d.last_latitude, 0) as latitude,
                COALESCE(dl.longitude, d.last_longitude, 0) as longitude,
                COALESCE(dl.status, 'online') as status,
                dl.last_update
            FROM drivers d
            LEFT JOIN driver_locations dl ON d.id = dl.driver_id
            WHERE d.status = 'active'
            ORDER BY d.full_name ASC
        `;
        
        console.log('📝 Executing query...');
        const [rows] = await pool.execute(query);
        
        console.log(`✅ Found ${rows.length} total drivers in database`);
        
        if (rows.length > 0) {
            console.log('📊 First driver:', JSON.stringify(rows[0]));
        }
        
        return rows;
    } catch (error) {
        console.error('❌ Failed to get all drivers:', error.message);
        console.error('❌ Error details:', error);
        return [];
    }
}

// ============================================================
// WEBSOCKET CONNECTION HANDLER
// ============================================================
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
    
    // Immediately send all drivers to the new client
    console.log('📨 Fetching drivers for new client...');
    getAllDrivers().then(drivers => {
        console.log(`📨 Sending ${drivers.length} drivers to new client`);
        ws.send(JSON.stringify({
            type: 'all_drivers',
            drivers: drivers,
            count: drivers.length,
            timestamp: new Date().toISOString()
        }));
    }).catch(error => {
        console.error('❌ Failed to send drivers to new client:', error.message);
        ws.send(JSON.stringify({
            type: 'all_drivers',
            drivers: [],
            count: 0,
            error: error.message,
            timestamp: new Date().toISOString()
        }));
    });
    
    ws.on('message', async (data) => {
        try {
            const message = JSON.parse(data.toString());
            console.log('📨 Received:', message.type);
            
            switch (message.type) {
                case 'get_all_drivers':
                    console.log('🔍 Getting ALL drivers from database...');
                    
                    try {
                        const allDrivers = await getAllDrivers();
                        
                        console.log(`✅ Found ${allDrivers.length} total drivers`);
                        
                        ws.send(JSON.stringify({
                            type: 'all_drivers',
                            drivers: allDrivers,
                            count: allDrivers.length,
                            timestamp: new Date().toISOString()
                        }));
                    } catch (error) {
                        console.error('❌ Failed to get all drivers:', error.message);
                        ws.send(JSON.stringify({
                            type: 'all_drivers',
                            drivers: [],
                            count: 0,
                            error: error.message,
                            timestamp: new Date().toISOString()
                        }));
                    }
                    break;
                    
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
                    
                    if (connectedDrivers.has(updateDriverId)) {
                        const driverData = connectedDrivers.get(updateDriverId);
                        driverData.location = message.location;
                        driverData.lastUpdate = Date.now();
                        connectedDrivers.set(updateDriverId, driverData);
                    }
                    
                    // Broadcast to all clients
                    const locationMsg = JSON.stringify({
                        type: 'driver_location_update',
                        driver_id: updateDriverId,
                        location: message.location,
                        timestamp: new Date().toISOString()
                    });
                    
                    wss.clients.forEach((client) => {
                        if (client !== ws && client.readyState === WebSocket.OPEN) {
                            client.send(locationMsg);
                        }
                    });
                    
                    ws.send(JSON.stringify({
                        type: 'location_ack',
                        driver_id: updateDriverId,
                        timestamp: new Date().toISOString()
                    }));
                    
                    console.log(`✅ Location updated for driver ${updateDriverId}`);
                    break;
                    
                case 'get_nearby_drivers':
                    console.log('🔍 Getting nearby drivers...');
                    const { lat, lng, radius = 100 } = message;
                    
                    if (!lat || !lng) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Missing latitude or longitude'
                        }));
                        return;
                    }
                    
                    try {
                        const allDrivers = await getAllDrivers();
                        
                        // Filter by distance
                        const nearby = allDrivers.filter(d => {
                            const distance = calculateDistance(lat, lng, d.latitude, d.longitude);
                            return distance <= radius;
                        });
                        
                        console.log(`✅ Found ${nearby.length} nearby drivers`);
                        
                        ws.send(JSON.stringify({
                            type: 'nearby_drivers',
                            drivers: nearby,
                            count: nearby.length,
                            timestamp: new Date().toISOString()
                        }));
                    } catch (error) {
                        console.error('❌ Failed to get nearby drivers:', error.message);
                        ws.send(JSON.stringify({
                            type: 'nearby_drivers',
                            drivers: [],
                            count: 0,
                            error: error.message,
                            timestamp: new Date().toISOString()
                        }));
                    }
                    break;
                    
                case 'heartbeat':
                    ws.send(JSON.stringify({
                        type: 'heartbeat_ack',
                        timestamp: Date.now()
                    }));
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
    
    ws.on('close', () => {
        console.log('🔴 Client disconnected');
        if (driverId && isDriver) {
            connectedDrivers.delete(driverId);
            console.log(`🔴 Driver ${driverId} removed from connected list`);
        }
    });
    
    ws.on('error', (error) => {
        console.error('❌ WebSocket error:', error.message);
    });
});

// ============================================================
// CALCULATE DISTANCE
// ============================================================
function calculateDistance(lat1, lng1, lat2, lng2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLng/2) * Math.sin(dLng/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

// ============================================================
// PERIODIC BROADCAST ALL DRIVERS
// ============================================================
setInterval(async () => {
    try {
        const allDrivers = await getAllDrivers();
        
        const message = JSON.stringify({
            type: 'all_drivers',
            drivers: allDrivers,
            count: allDrivers.length,
            timestamp: new Date().toISOString()
        });
        
        let clientsSent = 0;
        wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(message);
                clientsSent++;
            }
        });
        
        if (allDrivers.length > 0) {
            console.log(`📡 Broadcasted ${allDrivers.length} drivers to ${clientsSent} clients`);
        }
    } catch (error) {
        console.error('❌ Failed to broadcast drivers:', error.message);
    }
}, 30000);

// ============================================================
// START SERVER
// ============================================================
const PORT = process.env.PORT || 8080;

async function startServer() {
    const dbInitialized = await initDatabase();
    
    if (!dbInitialized) {
        console.error('❌ Failed to initialize database.');
    }
    
    server.listen(PORT, () => {
        console.log(`🚗 Drivee WebSocket Server running on port ${PORT}`);
        console.log(`📍 WebSocket URL: ws://localhost:${PORT}`);
        console.log(`🔍 Debug: http://localhost:${PORT}/debug-drivers`);
        console.log('✅ Server ready');
    });
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('🛑 Shutting down...');
    if (pool) {
        await pool.end();
        console.log('✅ Database connections closed');
    }
    wss.close(() => {
        console.log('✅ WebSocket server closed');
        process.exit(0);
    });
});

startServer();
