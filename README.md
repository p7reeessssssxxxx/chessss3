# oxy chess relay

A tiny service that powers the **oxy chess** assistant. The in-game client reads your live board and
POSTs the position (FEN) here; this service runs **Stockfish** on it and serves a **private live board
view** you open on your phone / second screen. You move the pieces yourself — nothing is automated in-game.

```
Roblox (oxy chess.lua)            this service (Railway)              your phone
  read board -> FEN  ── POST /push ─►  Stockfish analysis  ── WS ──►  /view/<token>
                                       stored per private token       live board + best-move arrow + eval
```

## What's here
- `server.js` — Express + WebSocket. `POST /push`, `GET /view/:token`, `GET /state/:token`, `WS /ws?token=`.
- `Dockerfile` — Node 20 + native `stockfish` (Debian package).
- `public/view.html` — the private viewer (self-contained, mobile-friendly, live updates).
- `package.json`

## Deploy to Railway
1. Push this folder to a GitHub repo (or use `railway up` from the Railway CLI in this folder).
2. On [railway.app](https://railway.app): **New Project → Deploy from repo** (or the CLI).
3. Railway auto-detects the **Dockerfile** and builds it (this is what installs Stockfish — do **not** switch it to Nixpacks).
4. Once deployed, open the service → **Settings → Networking → Generate Domain**. You get a URL like
   `https://oxychess-production.up.railway.app`.
5. That URL is your `RELAY_URL`.

No database, no secrets. Sessions live in memory, keyed by an unguessable token, and expire after 6h idle.

### Optional env vars
| var | default | meaning |
|-----|---------|---------|
| `SF_DEPTH` | `16` | Stockfish search depth (higher = stronger/slower; 14–18 is a good range) |
| `SF_POOL`  | `3`  | number of Stockfish processes (concurrent users) |
| `PORT`     | `8080` (Railway sets this) | listen port |

## Use it
1. Put your URL in `oxy chess.lua` (top: `RELAY_URL = "https://your-url.up.railway.app"`),
   or set `getgenv().OXY_CHESS_RELAY = "https://your-url.up.railway.app"` before running it.
2. Run `oxy chess.lua` while seated at a chess board. It prints **and copies** your private link:
   `https://your-url.up.railway.app/view/oxy<token>`
3. Open that link on your phone. The board mirrors your game live; the **best move** (arrow + eval) updates
   each time the position changes. Only someone with your exact token link can see it.

## Notes
- The link is private per run (a fresh random token each time unless you pin `getgenv().OXY_CHESS_TOKEN`).
- "Anyone can do this": every person who runs the client gets their own token → their own board view. One
  Railway service handles many independent sessions.
- Stockfish `score cp` is normalized to White's point of view in the viewer, so `+` always means White is better.
