const WebSocket = require('ws');
const mysql = require('mysql2');
const http = require('http');

// Database connection
const db = mysql.createPool({
    host: 'srv657.hstgr.io',
    port: 3306,
    user: 'your_db_user',
    password: 'your_db_password',
    database: 'your_db_name',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const server = http.createServer();
const wss = new WebSocket.Server({ server });

// Store connected clients and their subscriptions
const clients = new Map(); // driverId -> { ws, type }
const passengerClients = new Set();

console.log('WebSocket server starting...');

// Broadcast to all passengers
function broadcastToPassengers(data) {
    passengerClients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

// Send update to specific driver
function sendToDriver(driverId, data) {
    const client = clients.get(driverId);
    if (client && client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(JSON.stringify(data));
    }
}

// Update driver location in database and broadcast
async function updateDriverLocation(driverId, driverType, latitude, longitude, heading, speed, isOnline) {
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
        await db.promise().execute(query, [driverId, driverType, latitude, longitude, heading, speed, isOnline ? 1 : 0, now]);
        
        // Get driver name
        const [driver] = await db.promise().execute('SELECT name FROM users WHERE id = ?', [driverId]);
        const driverName = driver[0]?.name || 'Driver';
        
        // Broadcast to all passengers
        broadcastToPassengers({
            type: 'driver_location_update',
            driver: {
                id: driverId,
                name: driverName,
                type: driverType,
                latitude: latitude,
                longitude: longitude,
                heading: heading,
                speed: speed,
                is_online: isOnline,
                last_update: now.toISOString()
            }
        });
        
        return true;
    } catch (error) {
        console.error('Database error:', error);
        return false;
    }
}

// Get nearby drivers for passenger
async function getNearbyDrivers(latitude, longitude, radius = 5) {
    const query = `
        SELECT 
            d.driver_id,
            d.driver_type,
            d.latitude,
            d.longitude,
            d.heading,
            d.speed,
            d.last_update,
            u.name as driver_name,
            u.profile_image,
            (6371 * acos(cos(radians(?)) * cos(radians(d.latitude)) * 
            cos(radians(d.longitude) - radians(?)) + 
            sin(radians(?)) * sin(radians(d.latitude)))) AS distance
        FROM driver_locations d
        LEFT JOIN users u ON d.driver_id = u.id
        WHERE d.is_online = 1 
            AND d.last_update > DATE_SUB(NOW(), INTERVAL 2 MINUTE)
        HAVING distance < ?
        ORDER BY distance ASC
    `;
    
    try {
        const [drivers] = await db.promise().execute(query, [latitude, longitude, latitude, longitude, radius]);
        return drivers;
    } catch (error) {
        console.error('Error getting nearby drivers:', error);
        return [];
    }
}

// WebSocket connection handling
wss.on('connection', (ws, req) => {
    console.log('New client connected');
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            console.log('Received:', data.type);
            
            switch (data.type) {
                case 'driver_auth':
                    // Driver authentication
                    const driverId = data.driver_id;
                    clients.set(driverId, { ws, type: 'driver', driverId });
                    console.log(`Driver ${driverId} connected`);
                    
                    // Send confirmation
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'driver' }));
                    break;
                    
                case 'driver_location':
                    // Update driver location
                    await updateDriverLocation(
                        data.driver_id,
                        data.driver_type,
                        data.latitude,
                        data.longitude,
                        data.heading || 0,
                        data.speed || 0,
                        data.is_online || true
                    );
                    break;
                    
                case 'passenger_auth':
                    // Passenger authentication
                    passengerClients.add(ws);
                    console.log('Passenger connected');
                    
                    // Send confirmation
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'passenger' }));
                    
                    // Send initial nearby drivers
                    if (data.latitude && data.longitude) {
                        const drivers = await getNearbyDrivers(data.latitude, data.longitude);
                        ws.send(JSON.stringify({
                            type: 'initial_drivers',
                            drivers: drivers
                        }));
                    }
                    break;
                    
                case 'passenger_location':
                    // Passenger location update for nearby search
                    const nearbyDrivers = await getNearbyDrivers(data.latitude, data.longitude);
                    ws.send(JSON.stringify({
                        type: 'nearby_drivers',
                        drivers: nearbyDrivers
                    }));
                    break;
                    
                case 'driver_offline':
                    // Driver goes offline
                    await updateDriverLocation(data.driver_id, data.driver_type, data.latitude, data.longitude, 0, 0, false);
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
        
        // Remove from clients
        for (const [id, client] of clients.entries()) {
            if (client.ws === ws) {
                clients.delete(id);
                break;
            }
        }
        
        // Remove from passenger clients
        passengerClients.delete(ws);
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

// Start server
const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`WebSocket server running on port ${PORT}`);
});
