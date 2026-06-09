const WebSocket = require('ws');
const mysql = require('mysql2');
const http = require('http');

// Database connection
const db = mysql.createPool({
    host: 'srv657.hstgr.io',
    port: 3306,
    user: 'u442108067_rajithawalpola',      // Replace with your username
    password: '12IEhou:P',  // Replace with your password
    database: 'u442108067_testdb',   // Replace with your database name
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Create HTTP server
const server = http.createServer((req, res) => {
    // Health check endpoint
    if (req.url === '/healthz') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
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
    console.log('New client connected from:', req.socket.remoteAddress);
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            console.log('Received:', data.type, data.driver_id ? `Driver: ${data.driver_id}` : '');
            
            switch (data.type) {
                case 'driver_auth':
                    clients.set(data.driver_id, { ws, type: 'driver', driverId: data.driver_id });
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'driver' }));
                    console.log(`Driver ${data.driver_id} authenticated`);
                    break;
                    
                case 'driver_location':
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
                        await db.promise().execute(query, [
                            data.driver_id, data.driver_type, data.latitude, data.longitude, 
                            data.heading, data.speed, data.is_online ? 1 : 0, now
                        ]);
                        console.log(`Location updated for driver ${data.driver_id}`);
                        
                        // Broadcast to passengers
                        const driverQuery = await db.promise().execute('SELECT name FROM users WHERE id = ?', [data.driver_id]);
                        const driverName = driverQuery[0][0]?.name || 'Driver';
                        
                        passengerClients.forEach(client => {
                            if (client.readyState === WebSocket.OPEN) {
                                client.send(JSON.stringify({
                                    type: 'driver_location_update',
                                    driver: {
                                        id: data.driver_id,
                                        name: driverName,
                                        type: data.driver_type,
                                        latitude: data.latitude,
                                        longitude: data.longitude,
                                        heading: data.heading,
                                        speed: data.speed
                                    }
                                }));
                            }
                        });
                    } catch (error) {
                        console.error('Database error:', error);
                    }
                    break;
                    
                case 'passenger_auth':
                    passengerClients.add(ws);
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'passenger' }));
                    console.log('Passenger authenticated');
                    break;
                    
                case 'driver_offline':
                    const offlineQuery = `UPDATE driver_locations SET is_online = 0 WHERE driver_id = ?`;
                    await db.promise().execute(offlineQuery, [data.driver_id]);
                    console.log(`Driver ${data.driver_id} went offline`);
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
        await db.promise().execute(`
            UPDATE driver_locations 
            SET is_online = 0 
            WHERE last_update < DATE_SUB(NOW(), INTERVAL 2 MINUTE)
        `);
    } catch (error) {
        console.error('Cleanup error:', error);
    }
}, 60000);

// Get port from environment variable
const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ WebSocket server running on port ${PORT}`);
    console.log(`✅ Health check: http://localhost:${PORT}/healthz`);
});
