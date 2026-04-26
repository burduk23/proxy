const http = require('http');
const WebSocket = require('ws');
const net = require('net');

const VERSION = '1.1.0';
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

  // Get real IP from Render/Proxy headers
  const remoteAddr = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`[WS v${VERSION}] New connection from ${remoteAddr}`);

  const authHeader = req.headers['proxy-authorization'] || req.headers['authorization'];
  if (authHeader) {
    if (authHeader === `Basic ${AUTH_BASE64}` || authHeader === AUTH_STR) {
      isAuthorized = true;
    }
  }

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (isConnected) {
      if (targetSocket && targetSocket.writable) {
        targetSocket.write(data);
      }
      return;
    }

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
          console.log(`[WS] Unauthorized attempt from ${remoteAddr}`);
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

      console.log(`[WS] ${remoteAddr} -> Connecting to ${host}:${portNum}`);
      isConnecting = true;
      
      targetSocket = net.connect({
        host: host,
        port: portNum,
        timeout: 15000
      });

      targetSocket.on('connect', () => {
        console.log(`[Target] Connected to ${host}:${portNum}`);
        
        // Send status FIRST
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ status: 'connected', target: `${host}:${portNum}` }));
        }

        isConnected = true;
        isConnecting = false;
        
        // Flush buffer
        while (connectBuffer.length > 0) {
          const chunk = connectBuffer.shift();
          if (targetSocket.writable) targetSocket.write(chunk);
        }
      });

      targetSocket.on('data', (chunk) => {
        if (ws.readyState === WebSocket.OPEN) ws.send(chunk, { binary: true });
      });

      targetSocket.on('end', () => {
        console.log(`[Target] ${host}:${portNum} closed`);
        if (ws.readyState === WebSocket.OPEN) ws.close(1000, 'Target closed');
      });

      targetSocket.on('error', (err) => {
        console.error(`[Target] Error ${host}:${portNum}: ${err.message}`);
        isConnecting = false;
        if (targetSocket) targetSocket.destroy();
        if (ws.readyState === WebSocket.OPEN) {
          const errMsg = err.message.substring(0, 100);
          ws.close(1011, `Target error: ${errMsg}`);
        }
      });

      targetSocket.on('timeout', () => {
        console.log(`[Target] Timeout ${host}:${portNum}`);
        if (targetSocket) targetSocket.destroy();
        if (ws.readyState === WebSocket.OPEN) ws.close(1006, 'Target timeout');
      });

    } catch (err) {
      isConnecting = false;
      if (ws.readyState === WebSocket.OPEN) ws.close(4000, 'Handshake failed');
    }
  });

  ws.on('close', (code) => {
    console.log(`[WS] Closed for ${remoteAddr} (Code: ${code})`);
    if (targetSocket) targetSocket.destroy();
  });
});

const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`WebSocket bridge server v${VERSION} running on port ${PORT}`);
});
