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

const clients = new Map();
const passengerWsMap = new Map();

console.log('WebSocket server starting...');

db.getConnection((err, connection) => {
    if (err) {
        console.error('❌ DATABASE CONNECTION FAILED:', err.message);
    } else {
        console.log('✅ Database connected successfully!');
        connection.release();
    }
});

const server = http.createServer((req, res) => {
    if (req.url === '/healthz') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocket.Server({ server });

function sendToDriver(driverId, data) {
    const client = clients.get(driverId);
    if (client && client.ws && client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(JSON.stringify(data));
        console.log(`✅ Sent to driver ${driverId}:`, data.type);
        return true;
    }
    console.log(`❌ Driver ${driverId} not found`);
    return false;
}

function sendToPassenger(passengerId, data) {
    const passengerWs = passengerWsMap.get(passengerId);
    if (passengerWs && passengerWs.readyState === WebSocket.OPEN) {
        passengerWs.send(JSON.stringify(data));
        console.log(`✅ Sent to passenger ${passengerId}:`, data.type);
        return true;
    }
    console.log(`❌ Passenger ${passengerId} not found`);
    return false;
}

wss.on('connection', (ws, req) => {
    console.log('New client connected');
    let clientType = null;
    let clientId = null;
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            console.log('Received:', data.type);
            
            switch (data.type) {
                case 'driver_auth':
                    clientType = 'driver';
                    clientId = data.driver_id;
                    clients.set(data.driver_id, { ws, type: 'driver', driverId: data.driver_id });
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'driver' }));
                    console.log(`✅ Driver ${data.driver_id} authenticated`);
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
                            data.driver_id, data.driver_type || 'car', data.latitude, data.longitude,
                            data.heading || 0, data.speed || 0, data.is_online ? 1 : 0, now
                        ]);
                        
                        let driverName = 'Driver';
                        try {
                            const [userResult] = await db.promise().execute('SELECT name FROM users WHERE id = ?', [data.driver_id]);
                            if (userResult.length > 0 && userResult[0].name) driverName = userResult[0].name;
                        } catch (nameErr) {}
                        
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
                    
                case 'passenger_auth':
                    clientType = 'passenger';
                    clientId = data.passenger_id;
                    passengerWsMap.set(data.passenger_id || 1, ws);
                    ws.send(JSON.stringify({ type: 'auth_success', role: 'passenger' }));
                    console.log(`✅ Passenger ${data.passenger_id || 1} authenticated`);
                    if (data.latitude && data.longitude) {
                        const drivers = await getNearbyDrivers(data.latitude, data.longitude);
                        ws.send(JSON.stringify({ type: 'initial_drivers', drivers: drivers }));
                    }
                    break;
                    
                case 'passenger_location':
                    if (data.latitude && data.longitude) {
                        const nearbyDrivers = await getNearbyDrivers(data.latitude, data.longitude);
                        ws.send(JSON.stringify({ type: 'initial_drivers', drivers: nearbyDrivers }));
                    }
                    break;
                    
                case 'request_ride_to_driver':
                    console.log(`🚗 Passenger requesting ride from driver ${data.driver_id}`);
                    const targetDriver = clients.get(data.driver_id);
                    if (!targetDriver) {
                        ws.send(JSON.stringify({ type: 'ride_request_failed', message: 'Driver is not online' }));
                        break;
                    }
                    
                    const distance = calculateDistance(data.pickup_lat, data.pickup_lng, data.dropoff_lat, data.dropoff_lng);
                    
                    const insertRide = `
                        INSERT INTO ride_requests (passenger_id, driver_id, pickup_lat, pickup_lng, pickup_address, 
                            dropoff_lat, dropoff_lng, dropoff_address, status, distance, passenger_name, passenger_phone)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    `;
                    const [result] = await db.promise().execute(insertRide, [
                        data.passenger_id || 1, data.driver_id,
                        data.pickup_lat, data.pickup_lng, data.pickup_address || '',
                        data.dropoff_lat, data.dropoff_lng, data.dropoff_address || '',
                        distance.toFixed(2), data.passenger_name || 'Guest', data.passenger_phone || ''
                    ]);
                    
                    const rideRequestId = result.insertId;
                    
                    sendToDriver(data.driver_id, {
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
                    });
                    
                    ws.send(JSON.stringify({ type: 'ride_request_sent', ride_request_id: rideRequestId, message: 'Ride request sent to driver' }));
                    break;
                    
                case 'driver_send_bid':
                    console.log(`💰 Driver ${data.driver_id} sent bid of ₹${data.bid_amount}`);
                    
                    await db.promise().execute(`UPDATE ride_requests SET status = 'bidding', bid_amount = ? WHERE id = ?`, [data.bid_amount, data.ride_request_id]);
                    await db.promise().execute(`INSERT INTO driver_bids (ride_request_id, driver_id, bid_amount, status) VALUES (?, ?, ?, 'pending')`, [data.ride_request_id, data.driver_id, data.bid_amount]);
                    
                    const [driverInfo] = await db.promise().execute('SELECT name FROM users WHERE id = ?', [data.driver_id]);
                    const driverName = driverInfo[0]?.name || 'Driver';
                    
                    sendToPassenger(data.passenger_id, {
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
                    
                case 'passenger_accept_bid':
                    console.log(`✅ Passenger accepted bid from driver ${data.driver_id} for ₹${data.bid_amount}`);
                    
                    // Update bid status
                    await db.promise().execute(`UPDATE driver_bids SET status = 'accepted' WHERE ride_request_id = ? AND driver_id = ?`, [data.ride_request_id, data.driver_id]);
                    await db.promise().execute(`UPDATE driver_bids SET status = 'declined' WHERE ride_request_id = ? AND driver_id != ?`, [data.ride_request_id, data.driver_id]);
                    
                    // Update ride request
                    await db.promise().execute(`UPDATE ride_requests SET status = 'accepted', driver_id = ? WHERE id = ?`, [data.driver_id, data.ride_request_id]);
                    
                    // Get ride details
                    const [rideDetails] = await db.promise().execute(
                        `SELECT passenger_id, pickup_lat, pickup_lng, pickup_address, dropoff_lat, dropoff_lng, dropoff_address FROM ride_requests WHERE id = ?`,
                        [data.ride_request_id]
                    );
                    const ride = rideDetails[0];
                    
                    // Insert into active_rides
                    await db.promise().execute(
                        `INSERT INTO active_rides (ride_request_id, passenger_id, driver_id, bid_amount, status, 
                            pickup_lat, pickup_lng, pickup_address, dropoff_lat, dropoff_lng, dropoff_address)
                         VALUES (?, ?, ?, ?, 'accepted', ?, ?, ?, ?, ?, ?)`,
                        [data.ride_request_id, ride.passenger_id, data.driver_id, data.bid_amount,
                         ride.pickup_lat, ride.pickup_lng, ride.pickup_address,
                         ride.dropoff_lat, ride.dropoff_lng, ride.dropoff_address]
                    );
                    
                    // Send to driver
                    const driverSent = sendToDriver(data.driver_id, {
                        type: 'ride_assigned',
                        ride: {
                            ride_request_id: data.ride_request_id,
                            passenger_id: ride.passenger_id,
                            pickup_lat: ride.pickup_lat,
                            pickup_lng: ride.pickup_lng,
                            pickup_address: ride.pickup_address,
                            dropoff_lat: ride.dropoff_lat,
                            dropoff_lng: ride.dropoff_lng,
                            dropoff_address: ride.dropoff_address,
                            bid_amount: data.bid_amount
                        }
                    });
                    
                    console.log(`Driver send result: ${driverSent}`);
                    
                    // Send to passenger
                    sendToPassenger(data.passenger_id, {
                        type: 'ride_confirmed',
                        driver_id: data.driver_id,
                        driver_name: driverName,
                        bid_amount: data.bid_amount,
                        message: 'Ride confirmed! Driver is on the way.'
                    });
                    break;
                    
                case 'passenger_decline_bid':
                    console.log(`❌ Passenger declined bid from driver ${data.driver_id}`);
                    await db.promise().execute(`UPDATE driver_bids SET status = 'declined' WHERE ride_request_id = ? AND driver_id = ?`, [data.ride_request_id, data.driver_id]);
                    sendToDriver(data.driver_id, { type: 'bid_declined', message: 'Your bid was declined' });
                    break;
                    
                case 'driver_start_ride':
                    console.log(`🚗 Driver ${data.driver_id} started ride`);
                    await db.promise().execute(`UPDATE ride_requests SET status = 'started', started_at = NOW() WHERE id = ?`, [data.ride_request_id]);
                    await db.promise().execute(`UPDATE active_rides SET status = 'started', started_at = NOW() WHERE ride_request_id = ?`, [data.ride_request_id]);
                    sendToPassenger(data.passenger_id, { type: 'ride_started', message: 'Driver has started the ride' });
                    break;
                    
                case 'driver_complete_ride':
                    console.log(`✅ Driver ${data.driver_id} completed ride`);
                    await db.promise().execute(`UPDATE ride_requests SET status = 'completed', completed_at = NOW() WHERE id = ?`, [data.ride_request_id]);
                    await db.promise().execute(`UPDATE active_rides SET status = 'completed', completed_at = NOW() WHERE ride_request_id = ?`, [data.ride_request_id]);
                    sendToPassenger(data.passenger_id, { type: 'ride_completed', message: 'Ride completed successfully' });
                    break;
                    
                case 'driver_offline':
                    await db.promise().execute('UPDATE driver_locations SET is_online = 0 WHERE driver_id = ?', [data.driver_id]);
                    clients.delete(data.driver_id);
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
        if (clientType === 'driver' && clientId) clients.delete(clientId);
        if (clientType === 'passenger') {
            for (const [id, clientWs] of passengerWsMap.entries()) {
                if (clientWs === ws) passengerWsMap.delete(id);
            }
        }
    });
});

setInterval(async () => {
    try {
        await db.promise().execute(`UPDATE driver_locations SET is_online = 0 WHERE last_update < DATE_SUB(NOW(), INTERVAL 2 MINUTE)`);
    } catch (error) {
        console.error('Cleanup error:', error);
    }
}, 60000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ WebSocket server running on port ${PORT}`);
});
