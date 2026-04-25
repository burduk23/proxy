const http = require('http');
const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 8080;
const AUTH_USER = process.env.AUTH_USER || 'dandon';
const AUTH_PASS = process.env.AUTH_PASS || 'pulk';
const AUTH_STR = `${AUTH_USER}:${AUTH_PASS}`;
const AUTH_BASE64 = Buffer.from(AUTH_STR).toString('base64');

/**
 * HTTP Server for health checks and hosting the WebSocket server
 */
const server = http.createServer((req, res) => {
  // Health check endpoint for cronjobs/monitoring
  if (req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('pong');
    return;
  }
  
  // Default response for other HTTP requests
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

/**
 * WebSocket Server
 */
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  let targetSocket = null;
  let isConnected = false;
  let isConnecting = false;
  let isAuthorized = false;

  // Check authorization in headers (Proxy-Authorization or Authorization)
  const authHeader = req.headers['proxy-authorization'] || req.headers['authorization'];
  if (authHeader) {
    if (authHeader === `Basic ${AUTH_BASE64}` || authHeader === AUTH_STR) {
      isAuthorized = true;
    }
  }

  // Heartbeat state
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (data, isBinary) => {
    // Handshake phase: Auth and Target selection
    if (!isConnected) {
      if (isConnecting) return;

      if (isBinary) {
        ws.close(4003, 'Expected JSON handshake');
        return;
      }

      try {
        const msg = JSON.parse(data.toString());
        
        // Check auth if not already authorized via headers
        if (!isAuthorized) {
          if (msg.auth === AUTH_STR) {
            isAuthorized = true;
          } else {
            console.log('Unauthorized connection attempt');
            ws.close(4001, 'Unauthorized');
            return;
          }
        }

        // Connect to target host and port
        const { host, port } = msg;
        const portNum = parseInt(port, 10);
        
        if (typeof host !== 'string' || !host || isNaN(portNum) || portNum < 1 || portNum > 65535) {
          ws.close(4002, 'Invalid target (host string and port 1-65535 required)');
          return;
        }

        isConnecting = true;
        console.log(`Connecting to target: ${host}:${portNum}`);
        
        targetSocket = net.connect(portNum, host, () => {
          isConnected = true;
          isConnecting = false;
          console.log(`Connected to ${host}:${portNum}`);
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ status: 'connected', target: `${host}:${portNum}` }));
          }
        });

        // Forward data from TCP target to WebSocket client
        targetSocket.on('data', (chunk) => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(chunk, { binary: true });
          }
        });

        targetSocket.on('end', () => {
          console.log(`Target ${host}:${portNum} closed connection`);
          ws.close();
        });

        targetSocket.on('error', (err) => {
          console.error(`Target socket error (${host}:${portNum}):`, err.message);
          isConnecting = false;
          if (targetSocket) {
            targetSocket.destroy();
            targetSocket = null;
          }
          ws.close();
        });

      } catch (err) {
        console.error('Handshake error:', err.message);
        isConnecting = false;
        ws.close(4000, 'Handshake failed');
      }
    } else {
      // Data phase: Forward data from WebSocket client to TCP target
      if (targetSocket && targetSocket.writable) {
        targetSocket.write(data);
      }
    }
  });

  ws.on('close', () => {
    if (targetSocket) {
      targetSocket.destroy();
      targetSocket = null;
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
    if (targetSocket) {
      targetSocket.destroy();
      targetSocket = null;
    }
  });
});

/**
 * Heartbeat mechanism to prevent Render from closing idle connections (30s timeout)
 */
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log('Terminating inactive WebSocket connection');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000); // 25 seconds

wss.on('close', () => {
  clearInterval(interval);
});

/**
 * Global Error Handling
 */
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
});

/**
 * Start Server
 */
server.listen(PORT, () => {
  console.log(`WebSocket bridge server is running on port ${PORT}`);
  console.log(`Auth: ${AUTH_USER}:${AUTH_PASS}`);
});
