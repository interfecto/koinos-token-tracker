# Koinos Token Tracker

Lightweight Go microservice that indexes token balances, transfers, and holder rankings for the Koinos blockchain. Plugs directly into the node's AMQP message bus — no external dependencies.

**Live demo:** [koinosscan.com](https://koinosscan.com) | **API:** [api.koinosscan.com](https://api.koinosscan.com)

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Koinos Node                        │
│                                                      │
│  chain ─── block_store ─── p2p ─── block_producer   │
│    │              │                                  │
│    └──── AMQP (RabbitMQ) ────┐                      │
│                               │                      │
│                    ┌──────────┴──────────┐           │
│                    │  koinos-token-tracker│           │
│                    │                      │           │
│                    │  ┌── Syncer ◄─ AMQP RPC        │
│                    │  │   (GetBlocksByHeight)        │
│                    │  │                              │
│                    │  ├── Indexer                     │
│                    │  │   (extract addresses,        │
│                    │  │    parse token events,        │
│                    │  │    track balances)            │
│                    │  │                              │
│                    │  ├── SQLite Store               │
│                    │  │   (addresses, balances,       │
│                    │  │    transfers, blocks)         │
│                    │  │                              │
│                    │  └── REST API (:8090)           │
│                    │      (Swagger UI, JSON)          │
│                    └─────────────────────┘           │
└─────────────────────────────────────────────────────┘
```

## What It Tracks

| Data | Count | Source |
|------|-------|--------|
| Addresses | ~384,000 | Block headers, tx headers, event impacted fields |
| KOIN balances | ~9,250 holders | Transfer/mint/burn events (KCS-4 + KCS-1) |
| VHP balances | ~194 holders | Transfer/mint/burn events + REST reconciliation |
| Token transfers | ~55M records | All KOIN/VHP events from genesis |
| Block metadata | ~34.5M blocks | Height, timestamp, producer, tx count |

## Key Design Decisions

- **Confirmed-only indexing:** Only processes blocks up to the Last Irreversible Block (LIB = head - 60). Balances lag ~3 minutes but are guaranteed fork-safe.
- **Event-sourced balances:** Tracks balance changes from on-chain transfer/mint/burn events rather than querying contract state.
- **KCS-4 migration handling:** Old KOIN contract (KCS-1) events affect balances (migration did proper burn/mint). Old VHP events are stored for transfer history only — VHP balances are reconciled via REST API because the migration pre-loaded state without events.
- **Migration-height reset:** At block 24,804,034 (KCS-4 migration), VHP balances are reset to zero to prevent double-counting from the old contract.

## Quick Start

### Run alongside a Koinos node

```bash
# Build
go build -o koinos-token-tracker cmd/koinos-token-tracker/main.go

# Run (assumes node AMQP on localhost:5672)
./koinos-token-tracker \
  --basedir /home/koinos/.koinos \
  --amqp amqp://guest:guest@localhost:5672/ \
  --port 8090
```

### Docker

```bash
docker build -t koinos/koinos-token-tracker .
docker run -v /home/koinos/.koinos:/koinos \
  --network host \
  koinos/koinos-token-tracker --basedir=/koinos
```

### Add to docker-compose.yml

```yaml
token_tracker:
  image: koinos/koinos-token-tracker:latest
  restart: always
  profiles: ["token_tracker", "api", "all"]
  depends_on:
    - amqp
    - chain
    - block_store
  volumes:
    - "${BASEDIR}:/koinos"
  ports:
    - "${TOKEN_TRACKER_PORT:-8090}:8090"
  command: --basedir=/koinos
```

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--basedir, -d` | `~/.koinos` | Base directory for data |
| `--amqp, -a` | `amqp://guest:guest@localhost:5672/` | AMQP connection URL |
| `--port, -p` | `8080` | HTTP API listen port |
| `--log-level` | `info` | Log level (debug, info, warn, error) |
| `--reset` | `false` | Delete database and re-sync from genesis |
| `--reconcile` | `false` | After sync, reconcile VHP balances against REST API |
| `--rest-url` | `http://127.0.0.1:3000` | REST API URL for reconciliation |
| `--version, -v` | | Print version and exit |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Explorer UI |
| `GET /docs` | Swagger API documentation |
| `GET /v1/indexer/status` | Sync status, holder counts |
| `GET /v1/indexer/stats` | Chain statistics |
| `GET /v1/indexer/addresses?limit=50&offset=0` | All addresses (paginated) |
| `GET /v1/indexer/address/{address}` | Address balances and first-seen info |
| `GET /v1/indexer/holders/{token}?limit=50` | Top token holders ranked by balance |
| `GET /v1/indexer/transfers/{address}?limit=50` | Token transfer history for an address |
| `GET /v1/indexer/blocks?from=N&to=M` | Block metadata |
| `GET /v1/indexer/tokens` | Tracked token contracts |
| `GET /openapi.json` | OpenAPI 3.0 spec |

## How Sync Works

1. **Startup:** Reads last indexed height from SQLite
2. **Historical catch-up:** Calls `chain.GetHeadInfo` via AMQP to get LIB height, then fetches blocks in batches of 1,000 from `block_store.GetBlocksByHeight`
3. **Live mode:** Polls every 10 seconds, syncs new irreversible blocks
4. **Per block:** Extracts addresses from headers + events, parses token transfer/mint/burn events, applies balance deltas, stores transfer records

## Storage

- **Database:** SQLite (pure Go via `modernc.org/sqlite`, no CGO)
- **Size:** ~28GB with full transfer history from genesis
- **Tables:** `sync_state`, `addresses`, `balances`, `transfers`, `blocks`, `tokens`
- **Single connection:** `MaxOpenConns(1)` with mutex-protected batch transactions

## Token Contract Addresses

| Token | Contract | Tracks |
|-------|----------|--------|
| KOIN (KCS-4) | `19GYjDBVXU7keLbYvMLazsGQn3GTWHjHkK` | Balances + Transfers |
| VHP (KCS-4) | `12Y5vW6gk8GceH53YfRkRre2Rrcsgw7Naq` | Balances + Transfers |
| KOIN (KCS-1) | `15DJN4a8SgrbGhhGksSBASiSYjGnMU8dGL` | Balances + Transfers |
| VHP (KCS-1) | `1AdzuXSpC6K9qtXdCBgD5NUpDNwHjMgrc9` | Transfers only |

## License

MIT
