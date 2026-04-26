const http = require('http');
const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 8080;
const AUTH_USER = process.env.AUTH_USER || 'dandon';
const AUTH_PASS = process.env.AUTH_PASS || 'pulk';
const AUTH_STR = `${AUTH_USER}:${AUTH_PASS}`;
const AUTH_BASE64 = Buffer.from(AUTH_STR).toString('base64');

const server = http.createServer((req, res) => {
  if (req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('pong');
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  let targetSocket = null;
  let isConnected = false;
  let isConnecting = false;
  let isAuthorized = false;
  let connectBuffer = [];

  const authHeader = req.headers['proxy-authorization'] || req.headers['authorization'];
  if (authHeader) {
    if (authHeader === `Basic ${AUTH_BASE64}` || authHeader === AUTH_STR) {
      isAuthorized = true;
    }
  }

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (!isConnected) {
      if (isConnecting) {
        connectBuffer.push(data);
        return;
      }

      if (isBinary) {
        ws.close(4003, 'Expected JSON handshake');
        return;
      }

      try {
        const msg = JSON.parse(data.toString());
        if (!isAuthorized) {
          if (msg.auth === AUTH_STR) {
            isAuthorized = true;
          } else {
            ws.close(4001, 'Unauthorized');
            return;
          }
        }

        const { host, port } = msg;
        const portNum = parseInt(port, 10);
        if (!host || isNaN(portNum)) {
          ws.close(4002, 'Invalid target');
          return;
        }

        isConnecting = true;
        targetSocket = net.connect(portNum, host, () => {
          isConnected = true;
          isConnecting = false;
          
          while (connectBuffer.length > 0) {
            targetSocket.write(connectBuffer.shift());
          }

          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ status: 'connected', target: `${host}:${portNum}` }));
          }
        });

        targetSocket.on('data', (chunk) => {
          if (ws.readyState === WebSocket.OPEN) ws.send(chunk, { binary: true });
        });

        targetSocket.on('end', () => { ws.close(1000, 'Target closed'); });
        targetSocket.on('error', (err) => {
          isConnecting = false;
          if (targetSocket) targetSocket.destroy();
          ws.close(1011, `Target error: ${err.message}`);
        });

      } catch (err) {
        isConnecting = false;
        ws.close(4000, 'Handshake failed');
      }
    } else {
      if (targetSocket && targetSocket.writable) {
        targetSocket.write(data);
      }
    }
  });

  ws.on('close', () => {
    if (targetSocket) targetSocket.destroy();
  });
});

const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

server.listen(PORT, () => {
  console.log(`WebSocket bridge server is running on port ${PORT}`);
});
