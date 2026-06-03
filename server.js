// TellPoker WebSocket relay server
// Pure message router — no game logic. Clients handle everything.
// Deploy to Railway: railway init && railway up

const { WebSocketServer } = require('ws');

const port = process.env.PORT || 8080;
const wss  = new WebSocketServer({ port });

// roomCode → [ { ws, name } ]
const rooms = new Map();

wss.on('connection', (ws) => {
  let roomCode    = null;
  let playerName  = 'Player';

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // First message must be a join request
    if (!roomCode) {
      if (msg.type !== 'join' || !msg.room) return;
      roomCode   = String(msg.room).toUpperCase().slice(0, 8);
      playerName = msg.name || 'Player';

      if (!rooms.has(roomCode)) rooms.set(roomCode, []);
      rooms.get(roomCode).push({ ws, name: playerName });

      ws.send(JSON.stringify({ type: 'joined', room: roomCode,
                               peers: rooms.get(roomCode).length - 1 }));

      // Notify existing peers that someone new arrived
      broadcast(roomCode, ws, { type: 'peer_joined', name: playerName });
      return;
    }

    // Relay all other messages to every other peer in the room
    broadcastRaw(roomCode, ws, raw.toString());
  });

  ws.on('close', () => {
    if (!roomCode || !rooms.has(roomCode)) return;
    const remaining = rooms.get(roomCode).filter(p => p.ws !== ws);
    remaining.length ? rooms.set(roomCode, remaining)
                     : rooms.delete(roomCode);
    broadcast(roomCode, ws, { type: 'peer_left', name: playerName });
  });

  ws.on('error', () => ws.terminate());
});

function broadcast(code, sender, obj) {
  broadcastRaw(code, sender, JSON.stringify(obj));
}

function broadcastRaw(code, sender, raw) {
  const room = rooms.get(code);
  if (!room) return;
  for (const peer of room) {
    if (peer.ws !== sender && peer.ws.readyState === 1) {
      peer.ws.send(raw);
    }
  }
}

console.log(`TellPoker relay listening on :${port}`);
