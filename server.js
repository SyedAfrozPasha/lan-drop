/**
 * Local P2P File Transfer — Signaling Server
 * Run: node server.js
 * Then open http://<your-local-ip>:3000 on all devices (same WiFi)
 *
 * Dependencies: npm install express ws
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const os = require("os");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = 3000;

// ── Serve the frontend ──────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, "public")));

// ── Room & peer state ───────────────────────────────────────────────────────
// rooms: { roomId -> Map<peerId, { ws, name, joinedAt }> }
const rooms = new Map();

function getRoomList(roomId) {
  const room = rooms.get(roomId);
  if (!room) return [];
  return [...room.entries()].map(([id, p]) => ({ id, name: p.name }));
}

function broadcast(roomId, msg, excludeId = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const data = JSON.stringify(msg);
  for (const [pid, peer] of room.entries()) {
    if (pid !== excludeId && peer.ws.readyState === WebSocket.OPEN) {
      peer.ws.send(data);
    }
  }
}

function sendTo(roomId, targetId, msg) {
  const room = rooms.get(roomId);
  if (!room) return;
  const peer = room.get(targetId);
  if (peer && peer.ws.readyState === WebSocket.OPEN) {
    peer.ws.send(JSON.stringify(msg));
  }
}

// ── WebSocket signaling ─────────────────────────────────────────────────────
wss.on("connection", (ws) => {
  let myId = null;
  let myRoom = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {
      // Client joins a room with a display name
      case "join": {
        myId = `peer-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
        myRoom = msg.room || "default";
        const name = (msg.name || "Anonymous").slice(0, 32);

        if (!rooms.has(myRoom)) rooms.set(myRoom, new Map());
        rooms.get(myRoom).set(myId, { ws, name });

        // Confirm join + send existing peers list
        ws.send(
          JSON.stringify({
            type: "joined",
            id: myId,
            room: myRoom,
            peers: getRoomList(myRoom).filter((p) => p.id !== myId),
          }),
        );

        // Notify others
        broadcast(
          myRoom,
          {
            type: "peer-joined",
            peer: { id: myId, name },
          },
          myId,
        );
        break;
      }

      // WebRTC signaling relay: offer / answer / ice-candidate
      case "offer":
      case "answer":
      case "ice-candidate": {
        if (!myId || !myRoom) break;
        sendTo(myRoom, msg.to, { ...msg, from: myId });
        break;
      }

      // File transfer metadata notification (not the file itself)
      case "file-incoming": {
        if (!myId || !myRoom) break;
        sendTo(myRoom, msg.to, { ...msg, from: myId });
        break;
      }
    }
  });

  ws.on("close", () => {
    if (!myId || !myRoom) return;
    const room = rooms.get(myRoom);
    if (room) {
      room.delete(myId);
      if (room.size === 0) rooms.delete(myRoom);
      else broadcast(myRoom, { type: "peer-left", id: myId }, null);
    }
  });
});

// ── Start & print LAN IPs ───────────────────────────────────────────────────
server.listen(PORT, "0.0.0.0", () => {
  const nets = os.networkInterfaces();
  const ips = [];
  for (const ifaces of Object.values(nets)) {
    for (const i of ifaces) {
      if (i.family === "IPv4" && !i.internal) ips.push(i.address);
    }
  }
  console.log("\n🚀  P2P File Transfer Server running!\n");
  console.log("  Local:   http://localhost:" + PORT);
  ips.forEach((ip) =>
    console.log(`  Network: http://${ip}:${PORT}  ← share this`),
  );
  console.log("\n  Open the Network URL on all devices (same WiFi)\n");
});
