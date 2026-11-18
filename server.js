// Breach Zero room server: serves index.html and relays room presence over WebSocket.
// Anyone who opens the site can create or join rooms. Rooms live in memory.
const http = require('http'), fs = require('fs'), path = require('path');
let WebSocketServer;
try { ({ WebSocketServer } = require('ws')); }
catch (e) { console.error("FATAL: the 'ws' package is missing. Run: npm install"); process.exit(1); }
process.on('uncaughtException', e => console.error('uncaught:', e));
process.on('unhandledRejection', e => console.error('unhandled:', e));
const PORT = Number(process.env.PORT) || 8080;
const MAX_CONN = 400, MAX_PER_IP = 8, MAX_ROOMS = 300, ROOM_CAP = 32;
let page;
try { page = fs.readFileSync(path.join(__dirname, 'index.html')); }
catch (e) { console.error('index.html not found next to server.js (' + __dirname + '). Serving a notice instead.'); page = Buffer.from('index.html is missing from the deployment.'); }
const srv = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
  res.end(page);
});
const wss = new WebSocketServer({ server: srv, maxPayload: 8192 });
const rooms = new Map();            // room -> Map(id -> {ws, p})
const perIp = new Map();
const dirty = new Set();
const NAME = /^[a-z0-9_][a-z0-9_.-]{0,47}$/;
const KEY = /^[A-Za-z_][A-Za-z0-9_]{0,31}$/;
const BAD = new Set(['__proto__', 'constructor', 'prototype']);
let n = 0;

function leave(ws, name) {
  const r = rooms.get(name);
  if (!r || !r.delete(ws.id)) return;
  ws.rooms.delete(name);
  if (r.size === 0) rooms.delete(name); else dirty.add(name);
}
function broadcast(name) {
  const r = rooms.get(name); if (!r) return;
  const list = [...r].map(([id, m]) => ({ id, p: m.p }));
  const msg = JSON.stringify({ t: 'peers', room: name, list });
  for (const m of r.values()) if (m.ws.readyState === 1) m.ws.send(msg);
}

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (wss.clients.size > MAX_CONN || (perIp.get(ip) || 0) >= MAX_PER_IP) return ws.close(1013);
  perIp.set(ip, (perIp.get(ip) || 0) + 1);
  ws.id = 'p' + (++n).toString(36) + Math.random().toString(36).slice(2, 5);
  ws.rooms = new Set(); ws.tokens = 80; ws.last = Date.now(); ws.alive = true;
  ws.send(JSON.stringify({ t: 'hi', id: ws.id }));
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    const now = Date.now();
    ws.tokens = Math.min(80, ws.tokens + (now - ws.last) / 1000 * 60); ws.last = now;
    if (--ws.tokens < 0) return;                       // ~60 msgs/s cap
    let d; try { d = JSON.parse(raw); } catch { return; }
    if (!d || typeof d.room !== 'string' || !NAME.test(d.room)) return;
    if (d.t === 'join') {
      if (ws.rooms.has(d.room) || ws.rooms.size >= 4) return;
      let r = rooms.get(d.room);
      if (!r) { if (rooms.size >= MAX_ROOMS) return; rooms.set(d.room, r = new Map()); }
      if (r.size >= ROOM_CAP) return;
      r.set(ws.id, { ws, p: {} }); ws.rooms.add(d.room); dirty.add(d.room);
    } else if (d.t === 'p') {
      const m = rooms.get(d.room)?.get(ws.id);
      if (!m || !d.p || typeof d.p !== 'object') return;
      const next = { ...m.p };
      for (const k of Object.keys(d.p)) {
        if (BAD.has(k) || !KEY.test(k)) continue;
        if (d.p[k] === null) delete next[k]; else next[k] = d.p[k];
      }
      if (JSON.stringify(next).length > 4096) return;
      m.p = next; dirty.add(d.room);
    } else if (d.t === 'leave') leave(ws, d.room);
  });
  ws.on('close', () => {
    for (const r of [...ws.rooms]) leave(ws, r);
    const c = (perIp.get(ip) || 1) - 1; if (c <= 0) perIp.delete(ip); else perIp.set(ip, c);
  });
  ws.on('error', () => {});
});
setInterval(() => { for (const name of dirty) broadcast(name); dirty.clear(); }, 50);   // 20 Hz snapshots
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 30000);
srv.listen(PORT, '0.0.0.0', () => console.log('Breach Zero listening on 0.0.0.0:' + PORT + ', index.html ' + page.length + ' bytes'));
