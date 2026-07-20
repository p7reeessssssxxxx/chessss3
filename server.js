'use strict';
// oxy chess relay
// - POST /push { token, fen }  : in-game client pushes the current position
// - GET  /view/:token          : private live board viewer (open on your phone / 2nd screen)
// - GET  /state/:token         : JSON snapshot (polling fallback)
// - WS   /ws?token=...         : live push to the viewer
// Stockfish runs natively (see Dockerfile). Sessions are keyed by an unguessable token.

const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');

const PORT = process.env.PORT || 8080;
const STOCKFISH_PATH = process.env.STOCKFISH_PATH || 'stockfish';
const DEPTH = parseInt(process.env.SF_DEPTH || '16', 10);
const POOL_SIZE = parseInt(process.env.SF_POOL || '2', 10);
const SESSION_TTL_MS = 1000 * 60 * 60 * 6; // drop idle sessions after 6h

// ---------------------------------------------------------------- Stockfish
class Engine {
  constructor() {
    this.busy = false;
    this.buf = '';
    this.onLine = null;
    this._spawn();
  }
  _spawn() {
    this.proc = spawn(STOCKFISH_PATH, [], { stdio: ['pipe', 'pipe', 'ignore'] });
    this.proc.stdout.on('data', (d) => {
      this.buf += d.toString();
      let i;
      while ((i = this.buf.indexOf('\n')) >= 0) {
        const line = this.buf.slice(0, i).trim();
        this.buf = this.buf.slice(i + 1);
        if (this.onLine) this.onLine(line);
      }
    });
    this.proc.on('exit', () => { setTimeout(() => this._spawn(), 500); });
    this.send('uci');
    this.send('setoption name Threads value 1');
    this.send('isready');
  }
  send(cmd) { try { this.proc.stdin.write(cmd + '\n'); } catch (_) {} }
  analyze(fen, depth) {
    return new Promise((resolve) => {
      let cp = null, mate = null, pv = null, done = false;
      const finish = (bestmove) => {
        if (done) return; done = true; this.onLine = null;
        resolve({ bestmove, cp, mate, pv });
      };
      const timer = setTimeout(() => finish(null), 8000);
      this.onLine = (line) => {
        if (line.startsWith('info') && line.includes(' pv ')) {
          const c = line.match(/score cp (-?\d+)/);
          const m = line.match(/score mate (-?\d+)/);
          if (m) { mate = parseInt(m[1], 10); cp = null; }
          else if (c) { cp = parseInt(c[1], 10); mate = null; }
          const p = line.match(/ pv (.+)$/);
          if (p) pv = p[1].split(' ').slice(0, 12);
        } else if (line.startsWith('bestmove')) {
          clearTimeout(timer);
          finish((line.split(/\s+/)[1] || '').replace(/[^a-h1-8qrbnQRBN]/g, '') || null);
        }
      };
      this.send('position fen ' + fen);
      this.send('go depth ' + depth);
    });
  }
}

const pool = [];
const waiters = [];
for (let i = 0; i < POOL_SIZE; i++) pool.push(new Engine());
function acquire() {
  return new Promise((resolve) => {
    const e = pool.find((x) => !x.busy);
    if (e) { e.busy = true; resolve(e); } else waiters.push(resolve);
  });
}
function release(e) {
  const w = waiters.shift();
  if (w) { w(e); } else { e.busy = false; }
}
async function analyze(fen, depth) {
  const e = await acquire();
  try { return await e.analyze(fen, depth); }
  finally { release(e); }
}

// ---------------------------------------------------------------- sessions
const sessions = new Map(); // token -> { fen, bestmove, cp, mate, pv, updatedAt }
const viewers = new Map();  // token -> Set<ws>

setInterval(() => {
  const now = Date.now();
  for (const [t, rec] of sessions) if (now - rec.updatedAt > SESSION_TTL_MS && !(viewers.get(t) || {}).size) sessions.delete(t);
}, 60000);

function validFen(f) {
  if (typeof f !== 'string' || f.length > 100) return false;
  return /^([pnbrqkPNBRQK1-8]+\/){7}[pnbrqkPNBRQK1-8]+ [wb] (-|[KQkq]+) (-|[a-h][36]) \d+ \d+$/.test(f)
      || /^([pnbrqkPNBRQK1-8]+\/){7}[pnbrqkPNBRQK1-8]+ [wb] /.test(f); // lenient tail
}
function validToken(t) { return typeof t === 'string' && /^[A-Za-z0-9_-]{6,64}$/.test(t); }

function broadcast(token, rec) {
  const set = viewers.get(token);
  if (!set) return;
  const msg = JSON.stringify(rec);
  for (const ws of set) { try { ws.send(msg); } catch (_) {} }
}

// ---------------------------------------------------------------- http
const app = express();
app.use(express.json({ limit: '16kb' }));

app.get('/', (_req, res) => res.type('text').send('oxy chess relay up'));

app.post('/push', async (req, res) => {
  const { token, fen } = req.body || {};
  if (!validToken(token) || !validFen(fen)) return res.status(400).json({ error: 'bad token/fen' });
  const prev = sessions.get(token);
  if (prev && prev.fen === fen && prev.bestmove) return res.json({ ok: true, cached: true });
  // record the raw fen immediately so the viewer flips instantly; analysis fills in
  const rec = { fen, bestmove: null, cp: null, mate: null, pv: null, updatedAt: Date.now() };
  sessions.set(token, rec);
  broadcast(token, rec);
  res.json({ ok: true });
  try {
    const r = await analyze(fen, DEPTH);
    const cur = sessions.get(token);
    if (!cur || cur.fen !== fen) return; // position moved on; drop stale result
    Object.assign(cur, r, { depth: DEPTH, updatedAt: Date.now() });
    broadcast(token, cur);
  } catch (_) {}
});

app.get('/state/:token', (req, res) => {
  const rec = sessions.get(req.params.token);
  res.json(rec || { waiting: true });
});

app.get('/view/:token', (req, res) => {
  if (!validToken(req.params.token)) return res.status(400).send('bad token');
  res.sendFile(path.join(__dirname, 'public', 'view.html'));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws, req) => {
  let token = null;
  try { token = new URL(req.url, 'http://x').searchParams.get('token'); } catch (_) {}
  if (!validToken(token)) { ws.close(); return; }
  if (!viewers.has(token)) viewers.set(token, new Set());
  viewers.get(token).add(ws);
  const rec = sessions.get(token);
  if (rec) { try { ws.send(JSON.stringify(rec)); } catch (_) {} }
  ws.on('close', () => { const s = viewers.get(token); if (s) s.delete(ws); });
  ws.on('error', () => {});
});

server.listen(PORT, () => console.log('oxy chess relay listening on ' + PORT + ' (depth ' + DEPTH + ', pool ' + POOL_SIZE + ')'));
