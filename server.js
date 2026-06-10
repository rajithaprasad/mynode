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

// Helper function to calculate distance between two coordinates
function calculateDistance(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

// Helper function to get nearby drivers
async function getNearbyDrivers(latitude, longitude, radius = 10) {
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
        LIMIT 200
    `;
    
    try {
        const [drivers] = await db.promise().execute(query, [latitude, longitude, latitude, longitude, radius]);
        return drivers;
    } catch (error) {
        console.error('Error getting nearby drivers:', error);
        return [];
    }
}

// Store connected clients
const clients = new Map(); // driver_id -> { ws, type }
const passengerWsMap = new Map(); // passenger_id -> ws
const allConnections = new Set(); // All WebSocket connections for debugging

console.log('WebSocket server starting...');
console.log('Available driver IDs will be stored in clients map');

// Test database connection
db.getConnection((err, connection) => {
    if (err) {
        console.error('❌ DATABASE CONNECTION FAILED:', err.message);
    } else {
        console.log('✅ Database connected successfully!');
        connection.release();
    }
});

// Create HTTP server for health checks
const server = http.createServer((req, res) => {
    if (req.url === '/healthz') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
    } else if (req.url === '/clients') {
        // Debug endpoint to see connected clients
        const driverIds = Array.from(clients.keys());
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            drivers: driverIds,
            passengerCount: passengerWsMap.size,
            totalConnections: allConnections.size
        }));
    } else {
        res.writeHead(404);
        res.end();
    }
});

// Create WebSocket server
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
    console.log('New client connected');
    allConnections.add(ws);
    let clientType = null;
    let clientId = null;
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            console.log('Received:', data.type, data.driver_id ? `Driver: ${data.driver_id}` : data.passenger_id ? `Passenger: ${data.passenger_id}` : '');
            
            switch (data.type) {
                // ============ DRIVER AUTH ============
                case 'driver_auth':
                    clientType = 'driver';
                    clientId = data.driver_id;
                    clients.set(data.driver_id, { ws, type: 'driver', driverId: data.driver_id });
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'driver' }));
                    console.log(`✅ Driver ${data.driver_id} authenticated`);
                    console.log(`📋 Current drivers in map: ${Array.from(clients.keys()).join(', ')}`);
                    break;
                    
                // ============ DRIVER LOCATION ============
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
                            data.driver_id, 
                            data.driver_type || 'car', 
                            data.latitude, 
                            data.longitude, 
                            data.heading || 0, 
                            data.speed || 0, 
                            data.is_online ? 1 : 0, 
                            now
                        ]);
                        
                        let driverName = 'Driver';
                        try {
                            const [userResult] = await db.promise().execute('SELECT name FROM users WHERE id = ?', [data.driver_id]);
                            if (userResult.length > 0 && userResult[0].name) {
                                driverName = userResult[0].name;
                            }
                        } catch (nameErr) {}
                        
                        // Broadcast to all passengers
                        for (const [pid, passengerWs] of passengerWsMap) {
                            if (passengerWs.readyState === WebSocket.OPEN) {
                                passengerWs.send(JSON.stringify({
                                    type: 'driver_location_update',
                                    driver: {
                                        driver_id: data.driver_id,
                                        driver_name: driverName,
                                        driver_type: data.driver_type || 'car',
                                        latitude: data.latitude,
                                        longitude: data.longitude,
                                        heading: data.heading || 0,
                                        speed: data.speed || 0,
                                        last_update: now.toISOString()
                                    }
                                }));
                            }
                        }
                    } catch (dbError) {
                        console.error('Database error:', dbError.message);
                    }
                    break;
                    
                // ============ PASSENGER AUTH ============
                case 'passenger_auth':
                    clientType = 'passenger';
                    clientId = data.passenger_id;
                    passengerWsMap.set(data.passenger_id || 1, ws);
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'passenger' }));
                    console.log(`✅ Passenger ${data.passenger_id || 1} authenticated`);
                    
                    if (data.latitude && data.longitude) {
                        const drivers = await getNearbyDrivers(data.latitude, data.longitude);
                        ws.send(JSON.stringify({
                            type: 'initial_drivers',
                            drivers: drivers
                        }));
                    }
                    break;
                    
                // ============ PASSENGER LOCATION UPDATE ============
                case 'passenger_location':
                    if (data.latitude && data.longitude) {
                        const nearbyDrivers = await getNearbyDrivers(data.latitude, data.longitude);
                        ws.send(JSON.stringify({
                            type: 'initial_drivers',
                            drivers: nearbyDrivers
                        }));
                    }
                    break;
                    
                // ============ PASSENGER REQUESTS RIDE TO SPECIFIC DRIVER ============
                case 'request_ride_to_driver':
                    console.log(`🚗 Passenger requesting ride from driver ${data.driver_id}`);
                    console.log(`📋 Available drivers: ${Array.from(clients.keys()).join(', ')}`);
                    
                    // Check if driver exists in clients map
                    const targetDriver = clients.get(data.driver_id);
                    if (!targetDriver) {
                        console.log(`❌ Driver ${data.driver_id} not found in clients map!`);
                        ws.send(JSON.stringify({
                            type: 'ride_request_failed',
                            message: `Driver ${data.driver_id} is not online`
                        }));
                        break;
                    }
                    
                    console.log(`✅ Driver ${data.driver_id} found, sending request...`);
                    
                    // Calculate distance between pickup and dropoff
                    const distance = calculateDistance(
                        data.pickup_lat, data.pickup_lng,
                        data.dropoff_lat, data.dropoff_lng
                    );
                    
                    // Save ride request to database
                    const insertRide = `
                        INSERT INTO ride_requests (
                            passenger_id, driver_id, pickup_lat, pickup_lng, pickup_address, 
                            dropoff_lat, dropoff_lng, dropoff_address, status, distance,
                            passenger_name, passenger_phone
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    `;
                    
                    const [result] = await db.promise().execute(insertRide, [
                        data.passenger_id || 1,
                        data.driver_id,
                        data.pickup_lat, data.pickup_lng, data.pickup_address || '',
                        data.dropoff_lat, data.dropoff_lng, data.dropoff_address || '',
                        distance.toFixed(2),
                        data.passenger_name || 'Guest',
                        data.passenger_phone || ''
                    ]);
                    
                    const rideRequestId = result.insertId;
                    
                    // Send request to the selected driver
                    const driverWs = targetDriver.ws;
                    if (driverWs && driverWs.readyState === WebSocket.OPEN) {
                        driverWs.send(JSON.stringify({
                            type: 'new_ride_request',
                            ride_request_id: rideRequestId,
                            passenger_id: data.passenger_id || 1,
                            passenger_name: data.passenger_name || 'Guest',
                            pickup_lat: data.pickup_lat,
                            pickup_lng: data.pickup_lng,
                            pickup_address: data.pickup_address || 'Pickup location',
                            dropoff_lat: data.dropoff_lat,
                            dropoff_lng: data.dropoff_lng,
                            dropoff_address: data.dropoff_address || 'Dropoff location',
                            distance: distance.toFixed(1)
                        }));
                        console.log(`✅ Ride request sent to driver ${data.driver_id}`);
                        
                        ws.send(JSON.stringify({
                            type: 'ride_request_sent',
                            ride_request_id: rideRequestId,
                            message: 'Ride request sent to driver'
                        }));
                    } else {
                        console.log(`❌ Driver ${data.driver_id} WebSocket not open`);
                        ws.send(JSON.stringify({
                            type: 'ride_request_failed',
                            message: 'Driver connection lost'
                        }));
                    }
                    break;
                    
                // ============ DRIVER SENDS BID ============
                case 'driver_send_bid':
                    console.log(`💰 Driver ${data.driver_id} sent bid of ₹${data.bid_amount} for ride ${data.ride_request_id}`);
                    
                    const insertBid = `
                        INSERT INTO driver_bids (ride_request_id, driver_id, bid_amount, status)
                        VALUES (?, ?, ?, 'pending')
                    `;
                    await db.promise().execute(insertBid, [data.ride_request_id, data.driver_id, data.bid_amount]);
                    
                    await db.promise().execute(
                        `UPDATE ride_requests SET status = 'bidding' WHERE id = ?`,
                        [data.ride_request_id]
                    );
                    
                    const [driverInfo] = await db.promise().execute('SELECT name FROM users WHERE id = ?', [data.driver_id]);
                    const driverName = driverInfo[0]?.name || 'Driver';
                    
                    // Find passenger by ride request
                    const [rideInfo] = await db.promise().execute('SELECT passenger_id FROM ride_requests WHERE id = ?', [data.ride_request_id]);
                    const passengerId = rideInfo[0]?.passenger_id || 1;
                    
                    sendToPassenger(passengerId, {
                        type: 'new_bid_received',
                        ride_request_id: data.ride_request_id,
                        driver_id: data.driver_id,
                        driver_name: driverName,
                        bid_amount: data.bid_amount,
                        pickup_address: data.pickup_address,
                        dropoff_address: data.dropoff_address,
                        distance: data.distance
                    });
                    break;
                    
                // ============ PASSENGER ACCEPTS BID ============
                case 'passenger_accept_bid':
                    console.log(`✅ Passenger accepted bid from driver ${data.driver_id} for ₹${data.bid_amount}`);
                    
                    await db.promise().execute(
                        `UPDATE driver_bids SET status = 'accepted' 
                         WHERE ride_request_id = ? AND driver_id = ?`,
                        [data.ride_request_id, data.driver_id]
                    );
                    
                    await db.promise().execute(
                        `UPDATE driver_bids SET status = 'declined' 
                         WHERE ride_request_id = ? AND driver_id != ?`,
                        [data.ride_request_id, data.driver_id]
                    );
                    
                    await db.promise().execute(
                        `UPDATE ride_requests SET status = 'accepted', driver_id = ? WHERE id = ?`,
                        [data.driver_id, data.ride_request_id]
                    );
                    
                    await db.promise().execute(
                        `INSERT INTO active_rides (ride_request_id, passenger_id, driver_id, bid_amount, status)
                         SELECT ?, passenger_id, ?, ?, 'active' FROM ride_requests WHERE id = ?`,
                        [data.ride_request_id, data.driver_id, data.bid_amount, data.ride_request_id]
                    );
                    
                    sendToDriver(data.driver_id, {
                        type: 'bid_accepted',
                        ride_request_id: data.ride_request_id,
                        message: 'Your bid has been accepted! Proceed to pickup.'
                    });
                    
                    sendToPassenger(data.passenger_id, {
                        type: 'ride_confirmed',
                        driver_id: data.driver_id,
                        driver_name: data.driver_name,
                        bid_amount: data.bid_amount,
                        message: 'Ride confirmed! Driver is on the way.'
                    });
                    break;
                    
                // ============ PASSENGER DECLINES BID ============
                case 'passenger_decline_bid':
                    console.log(`❌ Passenger declined bid from driver ${data.driver_id}`);
                    
                    await db.promise().execute(
                        `UPDATE driver_bids SET status = 'declined' 
                         WHERE ride_request_id = ? AND driver_id = ?`,
                        [data.ride_request_id, data.driver_id]
                    );
                    
                    sendToDriver(data.driver_id, {
                        type: 'bid_declined',
                        ride_request_id: data.ride_request_id,
                        message: 'Your bid was declined'
                    });
                    break;
                    
                // ============ DRIVER OFFLINE ============
                case 'driver_offline':
                    try {
                        await db.promise().execute('UPDATE driver_locations SET is_online = 0 WHERE driver_id = ?', [data.driver_id]);
                        console.log(`✅ Driver ${data.driver_id} went offline`);
                        clients.delete(data.driver_id);
                        
                        for (const [pid, passengerWs] of passengerWsMap) {
                            if (passengerWs.readyState === WebSocket.OPEN) {
                                passengerWs.send(JSON.stringify({
                                    type: 'driver_offline',
                                    driver_id: data.driver_id
                                }));
                            }
                        }
                    } catch (err) {
                        console.error('Offline update error:', err.message);
                    }
                    break;
                    
                // ============ PING ============
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
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
        allConnections.delete(ws);
        if (clientType === 'driver' && clientId) {
            clients.delete(clientId);
            console.log(`Removed driver ${clientId} from clients map`);
        }
        if (clientType === 'passenger') {
            for (const [id, clientWs] of passengerWsMap.entries()) {
                if (clientWs === ws) {
                    passengerWsMap.delete(id);
                    break;
                }
            }
        }
    });
    
    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });
});

function sendToDriver(driverId, data) {
    const client = clients.get(driverId);
    if (client && client.ws && client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(JSON.stringify(data));
        console.log(`📤 Sent to driver ${driverId}:`, data.type);
        return true;
    }
    console.log(`❌ Failed to send to driver ${driverId} - not found or not open`);
    return false;
}

function sendToPassenger(passengerId, data) {
    const passengerWs = passengerWsMap.get(passengerId);
    if (passengerWs && passengerWs.readyState === WebSocket.OPEN) {
        passengerWs.send(JSON.stringify(data));
        console.log(`📤 Sent to passenger ${passengerId}:`, data.type);
        return true;
    }
    console.log(`❌ Failed to send to passenger ${passengerId} - not found`);
    return false;
}

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
    console.log(`✅ Debug clients: https://mynode-savj.onrender.com/clients`);
});
