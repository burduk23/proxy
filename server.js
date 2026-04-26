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

  const remoteAddr = req.socket.remoteAddress;
  console.log(`[WS] New connection from ${remoteAddr}`);

  // Auth check via headers
  const authHeader = req.headers['proxy-authorization'] || req.headers['authorization'];
  if (authHeader) {
    if (authHeader === `Basic ${AUTH_BASE64}` || authHeader === AUTH_STR) {
      isAuthorized = true;
    }
  }

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    // 1. If already connected, pipe data directly to target
    if (isConnected) {
      if (targetSocket && targetSocket.writable) {
        targetSocket.write(data);
      }
      return;
    }

    // 2. If connection is in progress, buffer the data
    if (isConnecting) {
      connectBuffer.push(data);
      return;
    }

    // 3. Handshake phase (must be JSON)
    if (isBinary) {
      ws.close(4003, 'Expected JSON handshake');
      return;
    }

    try {
      const msg = JSON.parse(data.toString());
      
      // Auth check via JSON if not already authorized
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
      
      // Create target socket
      targetSocket = net.connect({
        host: host,
        port: portNum,
        timeout: 15000
      });

      targetSocket.on('connect', () => {
        console.log(`[Target] ${host}:${portNum} connected`);
        
        // Send status FIRST to ensure tunnel is ready before data arrives
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ status: 'connected', target: `${host}:${portNum}` }));
        }

        isConnected = true;
        isConnecting = false;
        
        // Flush buffered data from client to target
        if (connectBuffer.length > 0) {
          console.log(`[Target] Flushing ${connectBuffer.length} chunks to ${host}:${portNum}`);
          while (connectBuffer.length > 0) {
            const chunk = connectBuffer.shift();
            if (targetSocket.writable) targetSocket.write(chunk);
          }
        }
      });

      targetSocket.on('data', (chunk) => {
        if (isConnected && ws.readyState === WebSocket.OPEN) {
          ws.send(chunk, { binary: true });
        }
      });

      targetSocket.on('end', () => {
        console.log(`[Target] ${host}:${portNum} closed connection`);
        if (ws.readyState === WebSocket.OPEN) ws.close(1000, 'Target closed');
      });

      targetSocket.on('error', (err) => {
        console.error(`[Target] Error for ${host}:${portNum}: ${err.message}`);
        isConnecting = false;
        if (targetSocket) targetSocket.destroy();
        if (ws.readyState === WebSocket.OPEN) {
          // Truncate error message to fit in close frame
          const errMsg = err.message.substring(0, 100);
          ws.close(1011, `Target error: ${errMsg}`);
        }
      });

      targetSocket.on('timeout', () => {
        console.log(`[Target] Timeout for ${host}:${portNum}`);
        if (targetSocket) targetSocket.destroy();
        if (ws.readyState === WebSocket.OPEN) ws.close(1006, 'Target timeout');
      });

      targetSocket.on('close', () => {
        if (ws.readyState === WebSocket.OPEN) ws.close(1000, 'Target socket closed');
      });

    } catch (err) {
      console.error(`[WS] Handshake error from ${remoteAddr}: ${err.message}`);
      isConnecting = false;
      if (ws.readyState === WebSocket.OPEN) ws.close(4000, 'Handshake failed');
    }
  });

  ws.on('close', (code, reason) => {
    console.log(`[WS] Connection closed for ${remoteAddr} (Code: ${code})`);
    if (targetSocket) {
      targetSocket.destroy();
      targetSocket = null;
    }
  });

  ws.on('error', (err) => {
    console.error(`[WS] Error for ${remoteAddr}: ${err.message}`);
    if (targetSocket) {
      targetSocket.destroy();
      targetSocket = null;
    }
  });
});

// Heartbeat to keep connection alive
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log(`[WS] Terminating inactive client`);
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => {
  clearInterval(interval);
});

server.listen(PORT, () => {
  console.log(`WebSocket bridge server is running on port ${PORT}`);
});
