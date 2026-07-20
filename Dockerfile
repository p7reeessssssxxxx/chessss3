# oxy chess relay — Node + native Stockfish
FROM node:20-slim

# Stockfish (native binary) from Debian repos — installs to /usr/games/stockfish
RUN apt-get update \
    && apt-get install -y --no-install-recommends stockfish ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# make it reachable as `stockfish` on PATH
RUN ln -sf /usr/games/stockfish /usr/local/bin/stockfish || true

WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY . .

ENV STOCKFISH_PATH=stockfish
ENV SF_DEPTH=16
ENV SF_POOL=3
EXPOSE 8080
CMD ["node", "server.js"]
