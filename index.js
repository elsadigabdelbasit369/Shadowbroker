const WebSocket = require('ws');

// مثال بسيط لخادم WebSocket
const wss = new WebSocket.Server({ port: process.env.WEBSOCKET_PORT || 8080 });

wss.on('connection', function connection(ws) {
  console.log('Client connected to WebSocket');
  
  ws.on('message', function incoming(message) {
    console.log('received: %s', message);
  });
  
  ws.send('Connected to Shadowbroker WebSocket');
});

console.log(`WebSocket server running on port ${process.env.WEBSOCKET_PORT || 8080}`);