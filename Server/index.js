const WebSocket = require('ws');
const { encode, decode } = require('@msgpack/msgpack');

const PORT = 10000;
const MAX_PLAYERS_PER_ROOM = 50;
const HEARTBEAT_INTERVAL = 30 * 1000;

const HDR_CONTROL = 0x01;
const HDR_TURNCHUNK_PART = 0x02;

const BUFFERED_AMOUNT_LIMIT = 256 * 1024;
const EMPTY_ROOM_CHECK_INTERVAL = 600 * 1000;

// Anti-desync: message queue settings
const MAX_QUEUE_SIZE = 100;
const QUEUE_PROCESS_INTERVAL = 50;
const MAX_QUEUE_OVERFLOW_COUNT = 10;

// Debug logging (set to false to reduce console output)
const DEBUG_LOGGING = true;

const ALLOWED_RE = /^[\p{L}\p{N}._-]+$/u;

const rooms = new Map();
let cachedLobbySnapshot = ['RO', []];

// Statistics
const stats = {
  totalConnections: 0,
  totalDisconnects: 0,
  messagesQueued: 0,
  messagesSent: 0,
  queueOverflows: 0
};

const wss = new WebSocket.Server({
  port: PORT,
  perMessageDeflate: false,
}, () => {
  console.log(`WebSocket server is running on port ${PORT}`);
});

// ---- helpers ----
function validateName(name, type = "Name") {
  if (typeof name !== 'string') return 'E00';
  const trimmed = name.trim();
  if (trimmed.length === 0) return 'E01';

  if (type === 'Nickname') {
    if (trimmed.length < 4) return 'E02';
    if (trimmed.length > 25) return 'E03';
  } else {
    if (trimmed.length > 24) return 'E07';
  }
  if (!ALLOWED_RE.test(trimmed)) return 'E04';
  return null;
}

function validatePassword(pass) {
  if (!pass) return null;
  if (typeof pass !== 'string') return 'E08';
  if (pass.length > 24) return 'E09';
  if (!ALLOWED_RE.test(pass)) return 'E10';
  return null;
}

function validateScenario(s) {
  if (typeof s !== 'string') return 'E23';
  const trimmed = s.trim();
  if (trimmed.length === 0) return 'E24';
  if (trimmed.length > 64) return 'E25';
  if (!ALLOWED_RE.test(trimmed)) return 'E26';
  return null;
}

// ---- rooms snapshot ----
function updateRoomSnapshot() {
  const allRooms = [];
  for (const [roomName, room] of rooms) {
    const hasPass = room.password ? 1 : 0;
    const playersCount = room.players ? room.players.size : 0;
    const scenario = room.scenarioName || "Game not started";
    allRooms.push([roomName, room.host, hasPass, playersCount, scenario]);
  }
  cachedLobbySnapshot = ['RO', allRooms];
}

// ---- player leave ----
function handleLeave(ws, state = {}) {
  const roomName = state.room;
  const nick = state.player;
  if (!roomName || !nick) return;

  const room = rooms.get(roomName);
  if (!room) return;

  const wasHost = room.host === nick;
  if (room.players.delete(nick)) {
    // Clear ready status when player leaves
    if (room.turnReady) {
      room.turnReady.delete(nick);
    }
    
    broadcast(roomName, ['l', nick]);

    if (room.players.size === 0) {
      rooms.delete(roomName);
      updateRoomSnapshot();
      return;
    }

    if (wasHost) {
      const nextHost = room.players.keys().next().value;
      if (nextHost) {
        room.host = nextHost;
        const nextWs = room.players.get(nextHost);
        if (nextWs && nextWs.readyState === WebSocket.OPEN) {
          safeSend(nextWs, ['host'], true); // Use queue for critical message
        }
      }
    }

    updateRoomSnapshot();
  }
}

// ---- handlers ----
const handlers = {
  hr: (ws, state, [nick, roomName, pass]) => {
    let err = validateName(nick, "Nickname");
    if (err) return safeSend(ws, ['e', err]);

    err = validateName(roomName, "Room name");
    if (err) return safeSend(ws, ['e', err]);

    err = validatePassword(pass);
    if (err) return safeSend(ws, ['e', err]);

    if (rooms.has(roomName)) return safeSend(ws, ['e', 'E11']);

    pass = pass || "";
    const players = new Map([[nick, ws]]);
    rooms.set(roomName, {
      players,
      password: pass,
      host: nick,
      scenarioName: "Game not started",
      turnReady: new Map() // Track who is ready for next turn
    });

    state.room = roomName;
    state.player = nick;
    ws.state = state;

    updateRoomSnapshot();
    safeSend(ws, ['roomhosted'], true); // Use queue for critical message
  },

  j: (ws, state, [nick, roomName, pass]) => {
    let err = validateName(nick, "Nickname");
    if (err) return safeSend(ws, ['e', err]);

    err = validateName(roomName, "Room name");
    if (err) return safeSend(ws, ['e', err]);

    err = validatePassword(pass);
    if (err) return safeSend(ws, ['e', err]);

    const room = rooms.get(roomName);
    if (!room) return safeSend(ws, ['e', 'E12']);

    pass = pass || "";
    if (room.password !== pass) return safeSend(ws, ['e', 'E13']);
    if (room.players.has(nick)) return safeSend(ws, ['e', 'E14']);
    if (room.players.size >= MAX_PLAYERS_PER_ROOM) return safeSend(ws, ['e', 'E15']);

    room.players.set(nick, ws);
    state.room = roomName;
    state.player = nick;
    ws.state = state;

    updateRoomSnapshot();
    broadcast(roomName, ['j', roomName, nick]);
  },

  l: (ws, state, [nick]) => {
    handleLeave(ws, state);
  },

  cmd: (ws, state, rest) => {
    const room = rooms.get(state.room);
    if (!room) return;
    // CRITICAL FIX: Broadcast commands to ALL players for synchronization
    if (DEBUG_LOGGING) {
      const dataSize = rest.length;
      const preview = dataSize > 0 ? JSON.stringify(rest.slice(0, 5)).substring(0, 100) : '[]';
      console.log(`[CMD] From ${state.player}, room: ${state.room}, dataSize: ${dataSize}, preview: ${preview}`);
    }
    
    // Broadcast to ALL players
    broadcast(state.room, ['cmd', ...rest], true);
  },

  // Handle cmdi (immediate commands) - same as cmd
  cmdi: (ws, state, rest) => {
    const room = rooms.get(state.room);
    if (!room) return;
    if (DEBUG_LOGGING) {
      const dataSize = rest.length;
      const preview = dataSize > 0 ? JSON.stringify(rest.slice(0, 5)).substring(0, 100) : '[]';
      console.log(`[CMDI] From ${state.player}, room: ${state.room}, dataSize: ${dataSize}, preview: ${preview}`);
    }
    
    // Broadcast as cmdi to ALL players
    broadcast(state.room, ['cmdi', ...rest], true);
  },

  getrooms: (ws) => {
    safeSend(ws, cachedLobbySnapshot);
  },

  setscenario: (ws, state, [scenarioName]) => {
    const roomName = state.room;
    if (!roomName) return safeSend(ws, ['e', 'E16']);
    const room = rooms.get(roomName);
    if (!room) return safeSend(ws, ['e', 'E17']);
    if (room.host !== state.player) return safeSend(ws, ['e', 'E27']);

    if (typeof scenarioName !== 'string') {
      room.scenarioName = 'Unknown';
    } else {
      const trimmed = scenarioName.trim();
      const err = validateScenario(trimmed);
      room.scenarioName = err ? 'Unknown' : trimmed;
    }

    updateRoomSnapshot();
  },

  k: (ws, state, [targetNick]) => {
    const roomName = state.room;
    if(!roomName) return;
    const room = rooms.get(roomName);
    if (!room) return;
    if (room.host === state.player && state.player !== targetNick && room.players.has(targetNick)) {
      const targetWs = room.players.get(targetNick);
      broadcast(roomName, ['k', targetNick], state.player);
      room.players.delete(targetNick);
      targetWs.state.room = null;
      targetWs.state.player = null;
      updateRoomSnapshot();
    }
  },

  // Turn ready system - allow cancellation before all players are ready
  turnReady: (ws, state) => {
    const roomName = state.room;
    const nick = state.player;
    if (!roomName || !nick) return;
    
    const room = rooms.get(roomName);
    if (!room) return;
    
    // Mark player as ready
    room.turnReady.set(nick, true);
    
    console.log(`[TURN-READY] ${nick} is ready in ${roomName} (${room.turnReady.size}/${room.players.size})`);
    
    // Broadcast ready status to all players
    const readyList = Array.from(room.turnReady.keys());
    broadcast(roomName, ['turnReadyStatus', readyList], true);
    
    // Check if all players are ready
    if (room.turnReady.size === room.players.size) {
      console.log(`[TURN-ADVANCE] All players ready in ${roomName}, advancing turn`);
      room.turnReady.clear();
      // Broadcast turn advance to all
      broadcast(roomName, ['turnAdvance'], true);
    }
  },

  cancelTurnReady: (ws, state) => {
    const roomName = state.room;
    const nick = state.player;
    if (!roomName || !nick) return;
    
    const room = rooms.get(roomName);
    if (!room) return;
    
    // Only allow cancel if not all players are ready yet
    if (room.turnReady.size < room.players.size) {
      room.turnReady.delete(nick);
      
      console.log(`[TURN-CANCEL] ${nick} cancelled ready in ${roomName} (${room.turnReady.size}/${room.players.size})`);
      
      // Broadcast updated ready status
      const readyList = Array.from(room.turnReady.keys());
      broadcast(roomName, ['turnReadyStatus', readyList], true);
    }
  },

  // Request game state sync (for when armies don't display)
  requestSync: (ws, state) => {
    const roomName = state.room;
    const nick = state.player;
    if (!roomName || !nick) return;
    
    const room = rooms.get(roomName);
    if (!room) return;
    
    console.log(`[SYNC-REQUEST] ${nick} requested game state sync in ${roomName}`);
    
    // Ask host to send current game state to this player
    const hostWs = room.players.get(room.host);
    if (hostWs && hostWs.readyState === WebSocket.OPEN) {
      safeSend(hostWs, ['syncRequest', nick], true);
    }
  }
};

// ---- message queue (anti-desync) ----
function enqueueMessage(ws, buffer, priority = false) {
  if (!ws || !ws.state || !ws.state.messageQueue) return false;
  
  // Check queue size
  if (ws.state.messageQueue.length >= MAX_QUEUE_SIZE) {
    ws.state.queueOverflowCount = (ws.state.queueOverflowCount || 0) + 1;
    stats.queueOverflows++;
    
    // If queue overflows too many times, disconnect the client
    if (ws.state.queueOverflowCount >= MAX_QUEUE_OVERFLOW_COUNT) {
      console.log(`[CRITICAL] Client ${ws.state.player || 'unknown'} disconnected due to queue overflow (${ws.state.queueOverflowCount} times)`);
      handleLeave(ws, ws.state);
      ws.terminate();
      return false;
    }
    return false;
  }
  
  // Add to queue (priority messages go to the front)
  if (priority) {
    ws.state.messageQueue.unshift(buffer);
  } else {
    ws.state.messageQueue.push(buffer);
  }
  
  stats.messagesQueued++;
  
  // Try to process queue immediately
  processClientQueue(ws);
  return true;
}

function processClientQueue(ws) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  if (!ws.state || !ws.state.messageQueue) return;
  if (ws.state.isSending) return; // Already sending
  if (ws.state.messageQueue.length === 0) return;
  
  // Check if we can send
  if (ws.bufferedAmount > BUFFERED_AMOUNT_LIMIT) return;
  
  // Log if queue is getting large
  if (ws.state.messageQueue.length > 50) {
    console.log(`Warning: Large queue for ${ws.state.player || 'unknown'}: ${ws.state.messageQueue.length} messages`);
  }
  
  ws.state.isSending = true;
  
  try {
    // Send as many messages as possible
    let sentCount = 0;
    while (ws.state.messageQueue.length > 0 && ws.bufferedAmount <= BUFFERED_AMOUNT_LIMIT) {
      const buffer = ws.state.messageQueue.shift();
      ws.send(buffer, { binary: true });
      sentCount++;
      stats.messagesSent++;
      
      // Reset overflow counter on successful send
      ws.state.queueOverflowCount = 0;
    }
    
    // Log if sent many messages at once
    if (sentCount > 10) {
      console.log(`Sent ${sentCount} queued messages to ${ws.state.player || 'unknown'}`);
    }
  } catch (err) {
    console.log(`Error processing queue for ${ws.state.player || 'unknown'}:`, err.message);
  } finally {
    ws.state.isSending = false;
  }
}

// ---- sending ----
function safeSend(ws, arr, useQueue = false) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  if (!Array.isArray(arr)) return;
  
  try {
    const payload = Buffer.from(encode(arr));
    const framed = Buffer.concat([Buffer.from([HDR_CONTROL]), payload]);
    
    // If queue is enabled and client has pending messages or high buffer, use queue
    if (useQueue && ws.state && ws.state.messageQueue) {
      if (ws.state.messageQueue.length > 0 || ws.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
        return enqueueMessage(ws, framed);
      }
    }
    
    // Direct send if buffer is okay
    if (ws.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
      if (useQueue && ws.state && ws.state.messageQueue) {
        return enqueueMessage(ws, framed);
      }
      // Drop message if no queue available and buffer is full
      return;
    }
    
    // Try direct send
    try {
      ws.send(framed, { binary: true });
    } catch (sendErr) {
      // If direct send fails and queue is available, try queuing
      if (useQueue && ws.state && ws.state.messageQueue) {
        enqueueMessage(ws, framed);
      }
    }
  } catch (err) {
    console.log(`Error in safeSend: ${err.message}`);
  }
}

function broadcast(roomName, arr, useQueue = true) {
  if (!Array.isArray(arr)) return;
  const room = rooms.get(roomName);
  if (!room) return;
  
  try {
    const payload = Buffer.from(encode(arr));
    const framed = Buffer.concat([Buffer.from([HDR_CONTROL]), payload]);
    
    let sentCount = 0;
    let queuedCount = 0;
    let failedCount = 0;
    
    for (const client of room.players.values()) {
      if (client.readyState !== WebSocket.OPEN) {
        failedCount++;
        continue;
      }
      
      // Use queue for guaranteed delivery
      if (useQueue && client.state && client.state.messageQueue) {
        // If queue has messages or buffer is high, add to queue
        if (client.state.messageQueue.length > 0 || client.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
          if (enqueueMessage(client, framed)) {
            queuedCount++;
          } else {
            failedCount++;
          }
        } else {
          // Try direct send
          try {
            client.send(framed, { binary: true });
            sentCount++;
          } catch (err) {
            // If direct send fails, add to queue
            if (enqueueMessage(client, framed)) {
              queuedCount++;
            } else {
              failedCount++;
            }
          }
        }
      } else {
        // Legacy behavior without queue
        if (client.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
          failedCount++;
          continue;
        }
        try {
          client.send(framed, { binary: true });
          sentCount++;
        } catch (err) {
          failedCount++;
        }
      }
    }
    
    // Log if there were issues with broadcast or for debug
    if (DEBUG_LOGGING && failedCount > 0) {
      console.log(`[BROADCAST-RESULT] Room: ${roomName}, sent=${sentCount}, queued=${queuedCount}, failed=${failedCount}`);
    }
  } catch (err) {
    console.log(`[ERROR] Error preparing broadcast to room ${roomName}: ${err.message}`);
  }
}

// ---- connection ----
wss.on('connection', ws => {
  stats.totalConnections++;
  
  const state = { 
    room: null, 
    player: null, 
    countryID: null,
    messageQueue: [],
    isSending: false,
    queueOverflowCount: 0,
    connectedAt: Date.now()
  };
  ws.state = state;

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  
  console.log(`[INFO] New connection (total: ${stats.totalConnections}, active: ${wss.clients.size})`);

  ws.on('message', (data, isBinary) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length < 1) return;
    const hdr = buf[0];

    if (hdr === HDR_CONTROL) {
      try {
        const decoded = decode(buf.slice(1));
        if (Array.isArray(decoded) && decoded.length >= 1 && typeof decoded[0] === 'string') {
          const [type, ...rest] = decoded;
          
          // DEBUG: Log all control messages with details
          if (DEBUG_LOGGING) {
            const preview = rest.length > 0 ? JSON.stringify(rest.slice(0, 3)) : '[]';
            console.log(`[MSG] Player: ${state.player || 'unknown'}, Type: ${type}, Room: ${state.room || 'none'}, Data: ${preview}`);
          }
          
          const handler = handlers[type];
          if (handler) {
            try { handler(ws, state, rest); } catch (hErr) {
              console.log(`[ERROR] Handler error for ${type}: ${hErr.message}`);
            }
            return;
          }
          if (state.room && rooms.has(state.room)) {
            if (DEBUG_LOGGING) {
              console.log(`[BROADCAST] Type: ${type} from ${state.player} to all in room ${state.room}`);
            }
            broadcast(state.room, decoded, true);
            return;
          }
        }
      } catch (err) {
        console.log(`[ERROR] Message decode error: ${err.message}`);
      }
    } else if (hdr === HDR_TURNCHUNK_PART) {
      // Critical: turnchunk must be delivered to ALL players for synchronization
      const room = rooms.get(state.room);
      if (!room) return;
      
      if (DEBUG_LOGGING) {
        console.log(`[TURNCHUNK] From ${state.player}, size: ${buf.length} bytes, room: ${state.room}`);
      }
      
      let sentCount = 0;
      let queuedCount = 0;
      
      for (const client of room.players.values()) {
        if (client.readyState !== WebSocket.OPEN) continue;
        
        // Use queue for guaranteed delivery of turnchunk
        if (client.state && client.state.messageQueue) {
          if (client.state.messageQueue.length > 0 || client.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
            if (enqueueMessage(client, buf, true)) { // High priority
              queuedCount++;
            }
          } else {
            try {
              client.send(buf, { binary: true });
              sentCount++;
            } catch (err) {
              if (enqueueMessage(client, buf, true)) {
                queuedCount++;
              }
            }
          }
        } else {
          // Legacy fallback
          if (client.bufferedAmount > BUFFERED_AMOUNT_LIMIT) continue;
          try {
            client.send(buf, { binary: true });
            sentCount++;
          } catch (err) {}
        }
      }
      
      if (DEBUG_LOGGING) {
        console.log(`[TURNCHUNK-RESULT] Sent: ${sentCount}, Queued: ${queuedCount}`);
      }
    }
  });

  ws.on('close', () => {
    stats.totalDisconnects++;
    const duration = Date.now() - (state.connectedAt || 0);
    console.log(`[INFO] Client disconnected (player: ${state.player || 'unknown'}, duration: ${Math.floor(duration / 1000)}s, active: ${wss.clients.size - 1})`);
    handleLeave(ws, state);
  });
  
  ws.on('error', err => {
    stats.totalDisconnects++;
    console.log(`[ERROR] Client error (player: ${state.player || 'unknown'}): ${err.message}`);
    handleLeave(ws, state);
  });
});

// ---- heartbeat ---
setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.isAlive) {
          handleLeave(ws, ws.state || {});
          return ws.terminate(); 
        }

        ws.isAlive = false; 
        ws.ping();         
    });
}, HEARTBEAT_INTERVAL);

// ---- queue processor (anti-desync) ---
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN && ws.state && ws.state.messageQueue) {
      processClientQueue(ws);
    }
  });
}, QUEUE_PROCESS_INTERVAL);

// ---- cleanup ---
setInterval(() => {
  try {
    let changed = false;
    for (const [roomName, room] of Array.from(rooms)) {
      let hasOpen = false;
      for (const client of room.players.values()) {
        if (client && client.readyState === WebSocket.OPEN) { hasOpen = true; break; }
      }
      if (!hasOpen) {
        rooms.delete(roomName);
        changed = true;
      }
    }
    if (changed) updateRoomSnapshot();
  } catch (err) {}
}, EMPTY_ROOM_CHECK_INTERVAL);

// ---- statistics logger ---
setInterval(() => {
  const queueSizes = [];
  wss.clients.forEach(ws => {
    if (ws.state && ws.state.messageQueue) {
      queueSizes.push(ws.state.messageQueue.length);
    }
  });
  
  const avgQueueSize = queueSizes.length > 0 ? Math.round(queueSizes.reduce((a, b) => a + b, 0) / queueSizes.length) : 0;
  const maxQueueSize = queueSizes.length > 0 ? Math.max(...queueSizes) : 0;
  
  console.log(`[STATS] Active: ${wss.clients.size}, Rooms: ${rooms.size}, ` +
              `Queued: ${stats.messagesQueued}, Sent: ${stats.messagesSent}, ` +
              `Overflows: ${stats.queueOverflows}, AvgQueue: ${avgQueueSize}, MaxQueue: ${maxQueueSize}`);
}, 60 * 1000); // Every minute

updateRoomSnapshot();

console.log('[INFO] Server initialized and ready');
