const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: 8080 });

// Store all drivers
const drivers = new Map();

// Store all connected clients
const clients = new Set();

// Generate some initial drivers
for (let i = 1; i <= 50; i++) {
  drivers.set(i, {
    id: i,
    name: `Driver ${i}`,
    vehicle: i % 3 === 0 ? "Sedan" : i % 2 === 0 ? "SUV" : "Hatchback",
    type: i % 3 === 0 ? "Car" : i % 2 === 0 ? "Car" : "Bike",
    price: 100 + Math.floor(Math.random() * 200),
    rating: 4 + Math.random(),
    location: {
      lat: 28.6139 + (Math.random() - 0.5) * 0.1,
      lng: 77.209 + (Math.random() - 0.5) * 0.1,
      heading: Math.random() * 360,
      speed: Math.random() * 60,
      lastUpdate: new Date()
    }
  });
}

// Broadcast to all clients
function broadcast(data) {
  const message = JSON.stringify(data);
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

wss.on('connection', (ws, req) => {
  console.log('Client connected');
  clients.add(ws);
  
  // Send current drivers list to new client
  ws.send(JSON.stringify({
    type: 'drivers_update',
    drivers: Array.from(drivers.values())
  }));
  
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      
      if (data.type === 'driver_location') {
        // Update driver location
        const driver = drivers.get(data.driverId);
        if (driver) {
          driver.location = {
            ...data.location,
            lastUpdate: new Date()
          };
          drivers.set(data.driverId, driver);
          
          // Broadcast update to all clients
          broadcast({
            type: 'driver_location_update',
            driver: driver
          });
        } else if (data.driverId) {
          // New driver joining
          const newDriver = {
            id: data.driverId,
            name: `Driver ${data.driverId}`,
            vehicle: "Sedan",
            type: "Car",
            price: 150,
            rating: 4.5,
            location: {
              ...data.location,
              lastUpdate: new Date()
            }
          };
          drivers.set(data.driverId, newDriver);
          broadcast({
            type: 'driver_location_update',
            driver: newDriver
          });
        }
      }
    } catch (err) {
      console.error('Error parsing message:', err);
    }
  });
  
  ws.on('close', () => {
    console.log('Client disconnected');
    clients.delete(ws);
  });
});

console.log('WebSocket server running on ws://localhost:8080');
