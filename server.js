// server.js - Complete WebSocket Server with Live Location Updates & Booking Subscriptions
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
            connections: wss ? wss.clients.size : 0,
            connectedDrivers: connectedDrivers.size
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
    
    if (req.url === '/connected-drivers') {
        const drivers = [];
        connectedDrivers.forEach((data, id) => {
            drivers.push({
                driver_id: id,
                location: data.location,
                lastUpdate: data.lastUpdate,
                isOnline: data.isOnline
            });
        });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            success: true,
            count: drivers.length,
            drivers: drivers
        }));
        return;
    }
    
    // Add endpoint to manually trigger booking status broadcast
    if (req.url && req.url.startsWith('/broadcast-booking')) {
        const url = new URL(req.url, `http://${req.headers.host}`);
        const bookingId = url.searchParams.get('booking_id');
        const status = url.searchParams.get('status');
        const driverName = url.searchParams.get('driver_name');
        
        console.log(`📨 Broadcast request received: booking_id=${bookingId}, status=${status}, driver_name=${driverName}`);
        
        if (bookingId && status) {
            broadcastBookingStatus(bookingId, status, driverName);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: true,
                message: 'Booking status broadcasted',
                booking_id: bookingId,
                status: status
            }));
        } else {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: false,
                message: 'Missing booking_id or status'
            }));
        }
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
                <p>Connected Drivers: ${connectedDrivers.size}</p>
                <p>WebSocket URL: ws://${req.headers.host}</p>
                <hr>
                <p>Database: ${dbConfig.database}</p>
                <p>Host: ${dbConfig.host}</p>
                <p><a href="/debug-drivers">🔍 Debug: View All Drivers</a></p>
                <p><a href="/connected-drivers">🔗 Connected Drivers</a></p>
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

// Store connected drivers with their WebSocket connections
const connectedDrivers = new Map();
// Store all clients (passengers) for broadcasting
const allClients = new Set();

// Database connection pool
let pool = null;
let driverColumns = [];

// ============================================================
// GET TABLE COLUMNS
// ============================================================
async function getTableColumns(tableName) {
    try {
        const [columns] = await pool.execute(`SHOW COLUMNS FROM ${tableName}`);
        return columns.map(col => col.Field);
    } catch (error) {
        console.error(`❌ Failed to get columns for ${tableName}:`, error.message);
        return [];
    }
}

// ============================================================
// INIT DATABASE
// ============================================================
async function initDatabase() {
    try {
        pool = mysql.createPool(dbConfig);
        console.log('✅ Database connection pool created');
        
        const connection = await pool.getConnection();
        console.log('✅ Database connected successfully');
        connection.release();
        
        // Get driver table columns
        driverColumns = await getTableColumns('drivers');
        console.log('📊 Driver table columns:', driverColumns);
        
        return true;
    } catch (error) {
        console.error('❌ Database connection failed:', error.message);
        console.error('Full error:', error);
        return false;
    }
}

// ============================================================
// BUILD DYNAMIC SELECT QUERY
// ============================================================
function buildDriverSelectQuery() {
    const baseColumns = [
        'd.id',
        'd.full_name',
        'd.rating',
        'd.total_trips',
        'd.status as driver_status'
    ];
    
    const optionalColumns = [
        'driver_type',
        'rate_per_km',
        'vehicle_rate_per_km',
        'display_rate',
        'profile_image'
    ];
    
    const selectColumns = [...baseColumns];
    
    optionalColumns.forEach(col => {
        if (driverColumns.includes(col)) {
            selectColumns.push(`d.${col}`);
        } else {
            if (col === 'driver_type') selectColumns.push("'own_vehicle' as driver_type");
            else if (col === 'rate_per_km') selectColumns.push("40 as rate_per_km");
            else if (col === 'vehicle_rate_per_km') selectColumns.push("40 as vehicle_rate_per_km");
            else if (col === 'display_rate') selectColumns.push("40 as display_rate");
            else if (col === 'profile_image') selectColumns.push("NULL as profile_image");
        }
    });
    
    // Add vehicle columns with COALESCE to handle NULLs
    selectColumns.push(`
        COALESCE(v.make, 'Toyota') as vehicle_make,
        COALESCE(v.model, 'Camry') as vehicle_model,
        COALESCE(v.year, '2021') as vehicle_year,
        COALESCE(v.color, 'Pearl White') as vehicle_color,
        COALESCE(v.plate, 'WP CAR-7823') as vehicle_plate,
        v.front_image,
        v.back_image,
        v.side_image,
        v.image_url
    `);
    
    // Add location columns
    selectColumns.push(`
        COALESCE(dl.latitude, d.last_latitude, 0) as latitude,
        COALESCE(dl.longitude, d.last_longitude, 0) as longitude,
        COALESCE(dl.status, 'offline') as status,
        dl.last_update
    `);
    
    return selectColumns.join(', ');
}

// ============================================================
// DEBUG DATABASE
// ============================================================
async function debugDatabase() {
    if (!pool) {
        return { success: false, error: 'Database not initialized' };
    }
    
    const result = {
        success: true,
        timestamp: new Date().toISOString(),
        columns: driverColumns,
        drivers: [],
        count: 0
    };
    
    try {
        const [activeCount] = await pool.execute("SELECT COUNT(*) as total FROM drivers WHERE status = 'active'");
        result.active_drivers = activeCount[0].total;
        
        const selectColumns = buildDriverSelectQuery();
        const query = `
            SELECT ${selectColumns}
            FROM drivers d
            LEFT JOIN driver_locations dl ON d.id = dl.driver_id
            LEFT JOIN vehicles v ON d.vehicle_id = v.id
            WHERE d.status = 'active'
            ORDER BY d.full_name ASC
        `;
        
        console.log('📝 Debug query:', query);
        
        const [rows] = await pool.execute(query);
        result.drivers = rows;
        result.count = rows.length;
        
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
// GET ALL DRIVERS - DYNAMIC QUERY
// ============================================================
async function getAllDrivers() {
    if (!pool) {
        console.error('❌ Database not initialized');
        return [];
    }
    
    try {
        const selectColumns = buildDriverSelectQuery();
        const query = `
            SELECT ${selectColumns}
            FROM drivers d
            LEFT JOIN driver_locations dl ON d.id = dl.driver_id
            LEFT JOIN vehicles v ON d.vehicle_id = v.id
            WHERE d.status = 'active'
            ORDER BY d.full_name ASC
        `;
        
        console.log('📝 Executing query...');
        const [rows] = await pool.execute(query);
        
        console.log(`✅ Found ${rows.length} total drivers in database`);
        
        // Enhance with live location if available
        const enhancedRows = rows.map(row => {
            const driverId = row.id;
            if (connectedDrivers.has(driverId)) {
                const liveData = connectedDrivers.get(driverId);
                if (liveData.location) {
                    return {
                        ...row,
                        latitude: liveData.location.latitude || row.latitude,
                        longitude: liveData.location.longitude || row.longitude,
                        status: liveData.location.status || row.status,
                        is_live: true
                    };
                }
            }
            return row;
        });
        
        if (enhancedRows.length > 0) {
            console.log('📊 First driver:', JSON.stringify(enhancedRows[0]));
        }
        
        return enhancedRows;
    } catch (error) {
        console.error('❌ Failed to get all drivers:', error.message);
        console.error('❌ Error details:', error);
        return [];
    }
}

// ============================================================
// SEND DRIVERS TO ALL CLIENTS
// ============================================================
async function broadcastAllDrivers() {
    try {
        const allDrivers = await getAllDrivers();
        
        // Enhance with live locations from connected drivers
        const enhancedDrivers = allDrivers.map(driver => {
            if (connectedDrivers.has(driver.id)) {
                const liveData = connectedDrivers.get(driver.id);
                if (liveData && liveData.location) {
                    return {
                        ...driver,
                        latitude: liveData.location.latitude || driver.latitude,
                        longitude: liveData.location.longitude || driver.longitude,
                        status: liveData.location.status || driver.status,
                        is_live: true,
                        last_location_update: liveData.lastUpdate
                    };
                }
            }
            return driver;
        });
        
        const message = JSON.stringify({
            type: 'all_drivers',
            drivers: enhancedDrivers,
            count: enhancedDrivers.length,
            timestamp: new Date().toISOString()
        });
        
        let clientsSent = 0;
        allClients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(message);
                clientsSent++;
            }
        });
        
        if (enhancedDrivers.length > 0) {
            console.log(`📡 Broadcasted ${enhancedDrivers.length} drivers to ${clientsSent} clients`);
        }
    } catch (error) {
        console.error('❌ Failed to broadcast drivers:', error.message);
    }
}

// ============================================================
// BROADCAST LOCATION UPDATE TO ALL CLIENTS
// ============================================================
function broadcastLocationUpdate(driverId, location) {
    const message = JSON.stringify({
        type: 'driver_location_update',
        driver_id: driverId,
        location: location,
        timestamp: new Date().toISOString()
    });
    
    let clientsSent = 0;
    allClients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
            clientsSent++;
        }
    });
    
    console.log(`📍 Location update for driver ${driverId} sent to ${clientsSent} clients`);
}

// ============================================================
// BROADCAST BOOKING STATUS TO SUBSCRIBED CLIENTS
// ============================================================
function broadcastBookingStatus(bookingId, status, driverName) {
    console.log(`📨 Broadcasting booking status: booking_id=${bookingId}, status=${status}, driver_name=${driverName}`);
    
    const message = JSON.stringify({
        type: 'booking_status_update',
        booking_id: bookingId,
        status: status,
        driver_name: driverName || null,
        timestamp: new Date().toISOString()
    });
    
    let clientsSent = 0;
    let bookingSubscribedClients = 0;
    let userSubscribedClients = 0;
    
    allClients.forEach((client) => {
        let shouldSend = false;
        let subscriptionType = '';
        
        // Check if client has booking subscriptions
        if (client.bookingSubscriptions && client.bookingSubscriptions.length > 0) {
            if (client.bookingSubscriptions.includes(bookingId)) {
                shouldSend = true;
                subscriptionType = 'booking';
                bookingSubscribedClients++;
            }
        }
        
        // Check if client has user subscriptions
        if (client.userSubscriptions && client.userSubscriptions.length > 0) {
            // For user subscriptions, we also send the update
            // The client will filter based on their bookings
            shouldSend = true;
            subscriptionType = 'user';
            userSubscribedClients++;
        }
        
        if (shouldSend && client.readyState === WebSocket.OPEN) {
            client.send(message);
            clientsSent++;
            console.log(`📨 Sent to ${subscriptionType} subscribed client ${client.clientId || 'unknown'}`);
        }
    });
    
    console.log(`📨 Booking status ${status} for ${bookingId} sent to ${clientsSent} clients (${bookingSubscribedClients} booking subs, ${userSubscribedClients} user subs)`);
}

// ============================================================
// UPDATE DRIVER LOCATION IN DATABASE
// ============================================================
async function updateDriverLocationInDB(driverId, location) {
    if (!pool) return;
    
    try {
        const { latitude, longitude, status = 'online' } = location;
        
        // Check if driver_locations record exists
        const [existing] = await pool.execute(
            "SELECT id FROM driver_locations WHERE driver_id = ?",
            [driverId]
        );
        
        if (existing.length > 0) {
            // Update existing record
            await pool.execute(
                `UPDATE driver_locations 
                 SET latitude = ?, longitude = ?, status = ?, last_update = NOW() 
                 WHERE driver_id = ?`,
                [latitude, longitude, status, driverId]
            );
        } else {
            // Insert new record
            await pool.execute(
                `INSERT INTO driver_locations (driver_id, latitude, longitude, status, last_update) 
                 VALUES (?, ?, ?, ?, NOW())`,
                [driverId, latitude, longitude, status]
            );
        }
        
        console.log(`✅ Location saved to database for driver ${driverId}`);
    } catch (error) {
        console.error(`❌ Failed to save location for driver ${driverId}:`, error.message);
    }
}

// ============================================================
// WEBSOCKET CONNECTION HANDLER
// ============================================================
wss.on('connection', (ws, req) => {
    console.log('🟢 New client connected');
    let driverId = null;
    let isDriver = false;
    let clientId = Math.random().toString(36).substring(7);
    
    // Initialize subscription arrays
    ws.bookingSubscriptions = [];
    ws.userSubscriptions = [];
    ws.clientId = clientId;
    
    // Add to all clients
    allClients.add(ws);
    console.log(`📊 Total clients: ${allClients.size}`);
    
    // Send welcome message
    ws.send(JSON.stringify({
        type: 'welcome',
        message: 'Connected to Drivee WebSocket Server',
        clientId: clientId,
        timestamp: new Date().toISOString()
    }));
    
    // Immediately send all drivers
    console.log('📨 Fetching drivers for new client...');
    getAllDrivers().then(drivers => {
        const message = JSON.stringify({
            type: 'all_drivers',
            drivers: drivers,
            count: drivers.length,
            timestamp: new Date().toISOString()
        });
        ws.send(message);
        console.log(`📨 Sent ${drivers.length} drivers to new client`);
    }).catch(error => {
        console.error('❌ Failed to send drivers:', error.message);
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
            console.log(`📨 Received [${message.type}] from ${isDriver ? 'Driver' : 'Client'} (${clientId})`);
            
            switch (message.type) {
                case 'get_all_drivers':
                    console.log('🔍 Getting ALL drivers...');
                    const allDrivers = await getAllDrivers();
                    ws.send(JSON.stringify({
                        type: 'all_drivers',
                        drivers: allDrivers,
                        count: allDrivers.length,
                        timestamp: new Date().toISOString()
                    }));
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
                    
                    // Store driver with their WebSocket
                    connectedDrivers.set(driverId, {
                        ws: ws,
                        location: null,
                        lastUpdate: Date.now(),
                        isOnline: true,
                        clientId: clientId
                    });
                    
                    console.log(`✅ Driver ${driverId} registered (${clientId})`);
                    console.log(`📊 Connected drivers: ${connectedDrivers.size}`);
                    
                    // Send registration success
                    ws.send(JSON.stringify({
                        type: 'registration_success',
                        driver_id: driverId,
                        message: 'Driver registered successfully'
                    }));
                    
                    // Broadcast updated driver list to all clients
                    await broadcastAllDrivers();
                    break;
                    
                case 'subscribe_booking':
                    const bookingId = message.booking_id;
                    console.log(`📨 Client ${clientId} subscribed to booking: ${bookingId}`);
                    // Store the subscription
                    if (!ws.bookingSubscriptions) {
                        ws.bookingSubscriptions = [];
                    }
                    if (!ws.bookingSubscriptions.includes(bookingId)) {
                        ws.bookingSubscriptions.push(bookingId);
                    }
                    ws.send(JSON.stringify({
                        type: 'subscription_success',
                        booking_id: bookingId,
                        message: 'Subscribed to booking updates'
                    }));
                    console.log(`📨 Client ${clientId} booking subscriptions: ${ws.bookingSubscriptions.join(', ')}`);
                    break;
                    
                case 'subscribe_user_bookings':
                    const userId = message.user_id;
                    console.log(`📨 Client ${clientId} subscribed to user bookings: ${userId}`);
                    // Store the subscription
                    if (!ws.userSubscriptions) {
                        ws.userSubscriptions = [];
                    }
                    if (!ws.userSubscriptions.includes(userId)) {
                        ws.userSubscriptions.push(userId);
                    }
                    ws.send(JSON.stringify({
                        type: 'subscription_success',
                        user_id: userId,
                        message: 'Subscribed to user booking updates'
                    }));
                    console.log(`📨 Client ${clientId} user subscriptions: ${ws.userSubscriptions.join(', ')}`);
                    break;
                    
                case 'unsubscribe_booking':
                    const unsubBookingId = message.booking_id;
                    if (ws.bookingSubscriptions) {
                        ws.bookingSubscriptions = ws.bookingSubscriptions.filter(
                            id => id !== unsubBookingId
                        );
                    }
                    ws.send(JSON.stringify({
                        type: 'unsubscription_success',
                        booking_id: unsubBookingId,
                        message: 'Unsubscribed from booking updates'
                    }));
                    break;
                    
                case 'unsubscribe_user_bookings':
                    const unsubUserId = message.user_id;
                    if (ws.userSubscriptions) {
                        ws.userSubscriptions = ws.userSubscriptions.filter(
                            id => id !== unsubUserId
                        );
                    }
                    ws.send(JSON.stringify({
                        type: 'unsubscription_success',
                        user_id: unsubUserId,
                        message: 'Unsubscribed from user booking updates'
                    }));
                    break;
                    
                case 'driver_location_update':
                    const updateDriverId = message.driver_id;
                    const location = message.location;
                    
                    if (!updateDriverId || !location) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Missing driver_id or location data'
                        }));
                        return;
                    }
                    
                    // Update in memory
                    if (connectedDrivers.has(updateDriverId)) {
                        const driverData = connectedDrivers.get(updateDriverId);
                        driverData.location = location;
                        driverData.lastUpdate = Date.now();
                        connectedDrivers.set(updateDriverId, driverData);
                        console.log(`📍 Location updated for driver ${updateDriverId}: ${location.latitude}, ${location.longitude}`);
                    } else {
                        console.log(`⚠️ Driver ${updateDriverId} not registered, but storing location`);
                        connectedDrivers.set(updateDriverId, {
                            ws: ws,
                            location: location,
                            lastUpdate: Date.now(),
                            isOnline: true,
                            clientId: clientId
                        });
                    }
                    
                    // SAVE TO DATABASE - CRITICAL FOR PERSISTENCE
                    await updateDriverLocationInDB(updateDriverId, location);
                    
                    // BROADCAST TO ALL CLIENTS (Passengers)
                    broadcastLocationUpdate(updateDriverId, location);
                    
                    // Send acknowledgment to driver
                    ws.send(JSON.stringify({
                        type: 'location_ack',
                        driver_id: updateDriverId,
                        timestamp: new Date().toISOString()
                    }));
                    break;
                    
                case 'driver_offline':
                    const offlineDriverId = message.driver_id;
                    if (offlineDriverId && connectedDrivers.has(offlineDriverId)) {
                        const driverData = connectedDrivers.get(offlineDriverId);
                        driverData.isOnline = false;
                        driverData.location = { ...driverData.location, status: 'offline' };
                        connectedDrivers.set(offlineDriverId, driverData);
                        
                        // Update database
                        await updateDriverLocationInDB(offlineDriverId, { 
                            ...driverData.location, 
                            status: 'offline' 
                        });
                        
                        // Broadcast offline status
                        broadcastLocationUpdate(offlineDriverId, { 
                            ...driverData.location, 
                            status: 'offline' 
                        });
                        
                        console.log(`🔴 Driver ${offlineDriverId} went offline`);
                    }
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
                message: 'Failed to process message: ' + error.message
            }));
        }
    });
    
    ws.on('close', () => {
        console.log(`🔴 Client ${clientId} disconnected`);
        
        // Remove from all clients
        allClients.delete(ws);
        
        // If it was a driver, clean up
        if (driverId && isDriver) {
            // Don't delete immediately - keep the driver in memory but mark offline
            if (connectedDrivers.has(driverId)) {
                const driverData = connectedDrivers.get(driverId);
                driverData.isOnline = false;
                driverData.ws = null;
                connectedDrivers.set(driverId, driverData);
                console.log(`🔴 Driver ${driverId} disconnected but keeping location`);
            }
        }
        
        console.log(`📊 Total clients: ${allClients.size}`);
        console.log(`📊 Connected drivers: ${connectedDrivers.size}`);
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
// PERIODIC BROADCAST ALL DRIVERS (for new passengers)
// ============================================================
setInterval(async () => {
    try {
        await broadcastAllDrivers();
    } catch (error) {
        console.error('❌ Failed to broadcast drivers:', error.message);
    }
}, 10000); // Every 10 seconds

// ============================================================
// CLEANUP STALE DRIVER CONNECTIONS
// ============================================================
setInterval(() => {
    const now = Date.now();
    const staleTimeout = 60000; // 60 seconds
    
    connectedDrivers.forEach((data, id) => {
        if (!data.ws && (now - data.lastUpdate) > staleTimeout) {
            connectedDrivers.delete(id);
            console.log(`🧹 Removed stale driver ${id}`);
        }
    });
}, 30000);

// ============================================================
// START SERVER
// ============================================================
const PORT = process.env.PORT || 8080;

async function startServer() {
    const dbInitialized = await initDatabase();
    
    if (!dbInitialized) {
        console.warn('⚠️ Database not initialized, but server will continue');
    }
    
    server.listen(PORT, () => {
        console.log(`🚗 Drivee WebSocket Server running on port ${PORT}`);
        console.log(`📍 WebSocket URL: ws://localhost:${PORT}`);
        console.log(`🔍 Debug: http://localhost:${PORT}/debug-drivers`);
        console.log(`🔗 Connected Drivers: http://localhost:${PORT}/connected-drivers`);
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
