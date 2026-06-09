const WebSocket = require('ws');
const mysql = require('mysql2');
const http = require('http');

// Database connection
const db = mysql.createPool({
    host: 'srv657.hstgr.io',
    port: 3306,
    user: 'u442108067_rajithawalpola',
    password: '12IEhou:P',
    database: 'u442108067_testdb',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 60000,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0
});

// Test database connection on startup
db.getConnection((err, connection) => {
    if (err) {
        console.error('❌ DATABASE CONNECTION FAILED:', err.message);
        console.error('Error code:', err.code);
        console.error('Check:');
        console.error('1. Remote MySQL is enabled in cPanel');
        console.error('2. User has proper privileges');
        console.error('3. Password is correct');
    } else {
        console.log('✅ Database connected successfully!');
        connection.release();
        
        // Test the driver_locations table
        connection.query('SELECT COUNT(*) as count FROM driver_locations', (err, results) => {
            if (err) {
                console.error('❌ Table check failed:', err.message);
            } else {
                console.log(`✅ driver_locations table has ${results[0].count} records`);
            }
        });
    }
});

// Create HTTP server
const server = http.createServer((req, res) => {
    if (req.url === '/healthz') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
    } else if (req.url === '/db-test') {
        // Test database endpoint
        db.query('SELECT 1 as test, NOW() as time', (err, results) => {
            if (err) {
                res.writeHead(500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: false, error: err.message }));
            } else {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: true, message: 'Database connected', time: results[0].time }));
            }
        });
    } else {
        res.writeHead(404);
        res.end();
    }
});

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Store connected clients
const clients = new Map();
const passengerClients = new Set();

console.log('WebSocket server starting...');

wss.on('connection', (ws, req) => {
    console.log('New client connected');
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            console.log('Received:', data.type, data.driver_id ? `Driver: ${data.driver_id}` : '');
            
            switch (data.type) {
                case 'driver_auth':
                    clients.set(data.driver_id, { ws, type: 'driver', driverId: data.driver_id });
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'driver' }));
                    console.log(`✅ Driver ${data.driver_id} authenticated`);
                    break;
                    
                case 'driver_location':
                    console.log(`📍 Location update for driver ${data.driver_id}: ${data.latitude}, ${data.longitude}`);
                    
                    const now = new Date();
                    const query = `
                        INSERT INTO driver_locations (driver_id, driver_type, latitude, longitude, heading, speed, is_online, last_update) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE 
                        driver_type = VALUES(driver_type),
                        latitude = VALUES(latitude),
                        longitude = VALUES(longitude),
                        heading = VALUES(heading),
                        speed = VALUES(speed),
                        is_online = VALUES(is_online),
                        last_update = VALUES(last_update)
                    `;
                    
                    try {
                        const [result] = await db.promise().execute(query, [
                            data.driver_id, 
                            data.driver_type || 'bike', 
                            data.latitude, 
                            data.longitude, 
                            data.heading || 0, 
                            data.speed || 0, 
                            data.is_online ? 1 : 0, 
                            now
                        ]);
                        console.log(`✅ Database updated for driver ${data.driver_id}`);
                        
                        // Broadcast to passengers
                        passengerClients.forEach(client => {
                            if (client.readyState === WebSocket.OPEN) {
                                client.send(JSON.stringify({
                                    type: 'driver_location_update',
                                    driver: {
                                        id: data.driver_id,
                                        type: data.driver_type,
                                        latitude: data.latitude,
                                        longitude: data.longitude,
                                        heading: data.heading
                                    }
                                }));
                            }
                        });
                    } catch (dbError) {
                        console.error('❌ Database error:', dbError.message);
                        console.error('SQL:', query);
                        ws.send(JSON.stringify({ type: 'db_error', message: dbError.message }));
                    }
                    break;
                    
                case 'passenger_auth':
                    passengerClients.add(ws);
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'passenger' }));
                    console.log('✅ Passenger authenticated');
                    break;
                    
                case 'driver_offline':
                    try {
                        await db.promise().execute('UPDATE driver_locations SET is_online = 0 WHERE driver_id = ?', [data.driver_id]);
                        console.log(`✅ Driver ${data.driver_id} went offline`);
                    } catch (err) {
                        console.error('Offline update error:', err.message);
                    }
                    break;
                    
                default:
                    console.log('Unknown message type:', data.type);
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    });
    
    ws.on('close', () => {
        console.log('Client disconnected');
        for (const [id, client] of clients.entries()) {
            if (client.ws === ws) {
                clients.delete(id);
                break;
            }
        }
        passengerClients.delete(ws);
    });
    
    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });
});

// Clean up old driver locations every minute
setInterval(async () => {
    try {
        const [result] = await db.promise().execute(`
            UPDATE driver_locations 
            SET is_online = 0 
            WHERE last_update < DATE_SUB(NOW(), INTERVAL 2 MINUTE)
        `);
        if (result.affectedRows > 0) {
            console.log(`🧹 Cleaned up ${result.affectedRows} stale driver locations`);
        }
    } catch (error) {
        console.error('Cleanup error:', error);
    }
}, 60000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ WebSocket server running on port ${PORT}`);
    console.log(`✅ Health check: https://mynode-savj.onrender.com/healthz`);
    console.log(`✅ DB Test: https://mynode-savj.onrender.com/db-test`);
});
