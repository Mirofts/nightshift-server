// Night Shift: Haunted Mall - matchmaking + relay server (zero dependencies, Node >= 18).
// - Serves the game files (optional) and a WebSocket endpoint at /ws
// - Matchmaking: players queue, get grouped in rooms of up to 6; a room starts when full or after FILL_MS (bots fill the gaps)
// - Host-authoritative relay: the first player of a room simulates the match (ghosts, bots, items) and streams snapshots;
//   the other players send their own movement + actions. Works the same on PC, Mac, iOS, Android (it's all the web build).
// Run:  node server/server.js            (PORT=8080 by default, STATIC=1 to also serve the game from the parent folder)
import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const PORT = +(process.env.PORT || 8080);
const ROOM_SIZE = 6;
const FILL_MS = +(process.env.FILL_MS || 6000); // wait this long for humans, then start with bots
const STATIC = process.env.STATIC !== '0';
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.json': 'application/json', '.mp3': 'audio/mpeg', '.mp4': 'video/mp4', '.webm': 'video/webm', '.ttf': 'font/ttf', '.txt': 'text/plain' };

const STATS = { out: 0, since: Date.now(), matches: 0 };
// ------------------------------------------------------------------ tiny WebSocket implementation (RFC 6455, text frames)
function wsAccept(req, socket) {
  const key = req.headers['sec-websocket-key']; if (!key) { socket.destroy(); return null; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${acc}\r\n\r\n`);
  socket.setNoDelay(true);
  const conn = { socket, buf: Buffer.alloc(0), open: true, onmsg: null, onclose: null, frags: [] };
  conn.send = (obj) => { if (!conn.open) return; const data = Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj)); let head; if (data.length < 126) head = Buffer.from([0x81, data.length]); else if (data.length < 65536) { head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(data.length, 2); } else { head = Buffer.alloc(10); head[0] = 0x81; head[1] = 127; head.writeBigUInt64BE(BigInt(data.length), 2); } try { socket.write(Buffer.concat([head, data])); STATS.out += head.length + data.length; } catch (e) {} };
  conn.close = () => { if (!conn.open) return; conn.open = false; try { socket.write(Buffer.from([0x88, 0])); socket.end(); } catch (e) {} if (conn.onclose) conn.onclose(); };
  socket.on('data', (chunk) => {
    conn.buf = Buffer.concat([conn.buf, chunk]);
    while (conn.buf.length >= 2) {
      const b0 = conn.buf[0], b1 = conn.buf[1], op = b0 & 15, fin = b0 & 128, masked = b1 & 128; let len = b1 & 127, off = 2;
      if (len === 126) { if (conn.buf.length < 4) return; len = conn.buf.readUInt16BE(2); off = 4; } else if (len === 127) { if (conn.buf.length < 10) return; len = Number(conn.buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) { conn.close(); return; }
      const need = off + (masked ? 4 : 0) + len; if (conn.buf.length < need) return;
      let payload = conn.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const m = conn.buf.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      conn.buf = conn.buf.subarray(need);
      if (op === 8) { conn.close(); return; }
      if (op === 9) { try { socket.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); } catch (e) {} continue; }
      if (op === 1 || op === 0) { conn.frags.push(payload); if (fin) { const txt = Buffer.concat(conn.frags).toString('utf8'); conn.frags = []; let msg; try { msg = JSON.parse(txt); } catch (e) { continue; } if (conn.onmsg) conn.onmsg(msg); } }
    }
  });
  socket.on('close', () => { if (conn.open) { conn.open = false; if (conn.onclose) conn.onclose(); } });
  socket.on('error', () => {});
  return conn;
}

// ------------------------------------------------------------------ matchmaking + rooms
let nextId = 1; const rooms = new Map(); const queue = [];
const clean = (s, n = 14) => String(s || '').replace(/[^\w\- .]/g, '').slice(0, n) || 'Player';
const CHAR_OK = ['yellow', 'purple', 'teal', 'pink', 'green', 'blue'];
function newRoom(level, proto) { const r = { id: 'r' + (nextId++), level, proto, players: [], state: 'lobby', created: Date.now(), seed: Math.floor(Math.random() * 1e6), hostId: null, timer: null }; rooms.set(r.id, r); r.timer = setTimeout(() => startRoom(r), FILL_MS); return r; }
function lobbyInfo(r) { return { t: 'lobby', room: r.id, players: r.players.map((p) => p.profile), startsIn: Math.max(0, FILL_MS - (Date.now() - r.created)), size: ROOM_SIZE }; }
function broadcast(r, msg, except) { for (const p of r.players) if (p !== except) p.conn.send(msg); }
function join(p, level, proto) {
  const open = [...rooms.values()].filter((x) => x.state === 'lobby' && x.players.length < ROOM_SIZE && x.proto === proto);
  let r = open.find((x) => x.level === level) || open[0];
  if (!r) r = newRoom(level, proto);
  r.players.push(p); p.room = r; if (!r.hostId) r.hostId = p.id;
  broadcast(r, lobbyInfo(r));
  if (r.players.length >= ROOM_SIZE) startRoom(r);
}
function startRoom(r) {
  if (r.state !== 'lobby' || !r.players.length) return; clearTimeout(r.timer); r.state = 'play';
  STATS.matches++; broadcast(r, { t: 'start', room: r.id, seed: r.seed, level: r.level, hostId: r.hostId, players: r.players.map((p) => p.profile) });
}
function leave(p) {
  const r = p.room; if (!r) return; p.room = null; r.players = r.players.filter((x) => x !== p);
  if (!r.players.length) { clearTimeout(r.timer); rooms.delete(r.id); return; }
  if (r.hostId === p.id) { r.hostId = r.players[0].id; broadcast(r, { t: 'host', hostId: r.hostId, reason: 'hostleft' }); }
  broadcast(r, { t: 'left', id: p.id });
  if (r.state === 'lobby') broadcast(r, lobbyInfo(r));
}

// ------------------------------------------------------------------ http + ws
const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/health') { res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' }); res.end(JSON.stringify({ ok: true, rooms: rooms.size, players: [...rooms.values()].reduce((n, r) => n + r.players.length, 0), matches: STATS.matches, sentMB: +(STATS.out / 1048576).toFixed(2), upHours: +((Date.now() - STATS.since) / 3600000).toFixed(2) })); return; }
  if (!STATIC) { res.writeHead(404); res.end(); return; }
  let f = path.join(ROOT, url === '/' ? 'index.html' : url); if (!f.startsWith(ROOT)) { res.writeHead(403); res.end(); return; }
  fs.readFile(f, (err, data) => { if (err) { if (url === '/') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('Night Shift online server is running.'); return; } res.writeHead(404); res.end('not found'); return; } res.writeHead(200, { 'content-type': MIME[path.extname(f)] || 'application/octet-stream', 'cache-control': 'no-cache' }); res.end(data); });
});
server.on('upgrade', (req, socket) => {
  if (!(req.url || '').startsWith('/ws')) { socket.destroy(); return; }
  const conn = wsAccept(req, socket); if (!conn) return;
  const p = { id: 'p' + (nextId++), conn, room: null, profile: null, last: Date.now() };
  conn.send({ t: 'hello', id: p.id });
  conn.onmsg = (m) => {
    p.last = Date.now();
    switch (m.t) {
      case 'queue': if (p.room) leave(p); p.profile = { id: p.id, name: clean(m.name), char: CHAR_OK.includes(m.char) ? m.char : 'yellow', hat: clean(m.hat, 12), platform: clean(m.platform, 12) }; join(p, +m.level || 1, +m.proto || 0); break;
      case 'cancel': leave(p); break;
      case 'ping': conn.send({ t: 'pong', ts: m.ts }); break;
      // gameplay relay: clients -> host ("in"), host -> clients ("snap", "ev"), chat to everyone
      case 'in': case 'act': { const r = p.room; if (!r) break; const h = r.players.find((x) => x.id === r.hostId); if (h && h !== p) h.conn.send({ ...m, from: p.id }); break; }
      case 'snap': case 'ev': case 'end': { const r = p.room; if (!r || r.hostId !== p.id) break; broadcast(r, m, p); if (m.t === 'end') r.state = 'done'; break; }
      case 'chat': { const r = p.room; if (!r) break; broadcast(r, { t: 'chat', from: p.id, text: String(m.text || '').slice(0, 80) }, p); break; }
    }
  };
  conn.onclose = () => leave(p);
});
setInterval(() => { for (const r of rooms.values()) for (const p of r.players) if (Date.now() - p.last > 30000) p.conn.close(); }, 5000);
server.listen(PORT, () => console.log(`Night Shift server on http://localhost:${PORT}  (ws: /ws)`));
