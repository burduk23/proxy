const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');

const VERSION = '1.1.2';
const PORT = process.env.PORT || 10000; // По умолчанию Render использует 10000 или PORT

const server = http.createServer((req, res) => {
  if (req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('pong');
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`WebSocket Bridge Server v${VERSION} is running.`);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  let targetSocket = null;
  let isConnected = false;
  let isConnecting = false;
  let connectBuffer = [];

  const remoteAddr = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`[v${VERSION}] New connection from ${remoteAddr}`);

  ws.on('message', (data, isBinary) => {
    if (isConnected) {
      if (targetSocket?.writable) targetSocket.write(data);
      return;
    }
    if (isConnecting) {
      connectBuffer.push(data);
      return;
    }

    try {
      const msg = JSON.parse(data.toString());
      const { host, port, auth } = msg;
      
      // Здесь можно добавить проверку auth, если нужно

      isConnecting = true;
      targetSocket = net.connect({ host, port: parseInt(port), timeout: 15000 }, () => {
        console.log(`[Target] Connected to ${host}:${port}`);
        if (ws.readyState === 1) { // WebSocket.OPEN
          ws.send(JSON.stringify({ status: 'connected' }));
        }
        isConnected = true;
        isConnecting = false;
        while (connectBuffer.length > 0) {
          targetSocket.write(connectBuffer.shift());
        }
      });

      targetSocket.on('data', (chunk) => {
        if (ws.readyState === 1) ws.send(chunk, { binary: true });
      });

      targetSocket.on('error', (err) => {
        console.error(`[Target Error] ${err.message}`);
        ws.close(1011, 'Target connection error');
      });

      targetSocket.on('end', () => ws.close(1000, 'Target closed'));

    } catch (e) {
      ws.close(4000, 'Handshake failed');
    }
  });

  ws.on('close', () => targetSocket?.destroy());
});

server.on('error', (err) => {
  console.error('[HTTP Server Error]', err);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server v${VERSION} listening on 0.0.0.0:${PORT}`);
});
