# Data Ingress

Data ingestion workspace for two live market-data pipelines:

- Python collectors for Polymarket event streams
- A Rust service for centralized exchange order book ingestion

The repository also includes database migrations, metadata tracking in Postgres, and a QuestDB archiver that exports completed partitions to Parquet.

## Architecture

### Components

- `services/polymarket/*/entrypoint.py`
  Runs interval-specific Polymarket collectors for `5m`, `15m`, `1h`, `4h`, `Daily`, and `stocks`.
- `shared/ingestion.py`
  Contains the base Polymarket collector logic: event discovery, websocket streaming, batching, and QuestDB ingestion.
- `crates/app`
  Runs the Rust ingestion service for exchange order books and trade-by-trade data.
- `crates/connectors`
  Provides ingestion clients and controllers used by the Rust runtime.
- `crates/exchanges`
  Exchange adapters for Binance, Bybit, and additional exchange modules under development.
- `crates/exchanges-common`
  Shared models, traits, telemetry, error types, and task orchestration for the Rust stack.
- `migrations/questdb`
  Defines QuestDB tables for Polymarket and exchange live data.
- `migrations/postgres`
  Defines Postgres metadata tables used for registry, audit, and archive bookkeeping.
- `scripts/cron/archiver.py`
  Compares QuestDB partitions with Postgres metadata and triggers archive exports.

### Runtime Topology

```text
Polymarket Gamma API ─┐
                      ├─> Python collectors ───────────────┐
Polymarket WS API  ───┘                                    │
                                                           ├─> QuestDB
Binance / Bybit WS APIs ─> Rust app / connectors ──────────┘
                                   │
                                   └─> Postgres pipeline audit / archive metadata

QuestDB completed partitions ─> archiver ─> Parquet exports + Postgres archive records
```

### Storage Model

QuestDB tables created by the current migrations:

- `poly_live_price`
- `poly_live_tbt`
- `exchanges_live_price`
- `exchanges_live_tbt`

Postgres tables created by the current migrations:

- `table_registry`
- `archives`
- `pipeline_audit`

## Data Flow

### 1. Polymarket collectors

Each collector:

1. Builds an event slug for the configured time window.
2. Fetches market metadata from the Gamma API.
3. Extracts CLOB token IDs for the active markets.
4. Subscribes to the Polymarket websocket feed.
5. Writes snapshot-style order book rows to `poly_live_price`.
6. Writes price-change rows to `poly_live_tbt`.

The shared collector implementation also pre-warms the next time window before the current one expires.

### 2. Rust exchange ingestion

The Rust app:

1. Starts a task supervisor and worker channels.
2. Connects to Postgres and QuestDB.
3. Starts exchange connectors for currently wired adapters:
   - Binance
   - Bybit
4. Normalizes exchange messages into a shared command/event pipeline.
5. Writes exchange snapshots and trade-by-trade records into QuestDB.
6. Emits audit and error events through the internal telemetry pipeline.

### 3. Archive workflow

The archiver:

1. Reads BRONZE tables from Postgres `table_registry`.
2. Detects completed QuestDB partitions not yet recorded in `archives`.
3. Triggers QuestDB `COPY ... TO ...` exports in Parquet format.
4. Stores exported file metadata and row counts in Postgres.

Operational note:
QuestDB performs the actual file export, so the target archive path must exist and be writable from the QuestDB host environment.

## Repository Layout

```text
.
├── crates/
│   ├── app/                # Rust runtime entrypoint
│   ├── connectors/         # Ingestion clients/controllers
│   ├── exchanges/          # Exchange adapters
│   └── exchanges-common/   # Shared Rust domain types
├── services/polymarket/    # Python collectors by time window
├── shared/                 # Shared Python ingestion helpers
├── common/                 # Shared Python config/logging
├── migrations/
│   ├── postgres/
│   └── questdb/
├── scripts/
│   ├── cron/               # Archiver and scheduled jobs
│   └── migrate.py          # Migration runner
├── docs/                   # Supplemental operational docs
├── docker-compose.yml      # Docker-based stack entrypoint
└── portainer-compose.yml   # Portainer-friendly deployment spec
```

## Prerequisites

### Local development

- Python `>=3.10`
- `uv`
- Rust toolchain
- Docker and Docker Compose if you want containerized execution
- Reachable QuestDB instance
- Reachable Postgres instance

### Infrastructure assumptions

The compose files in this repository do not provision QuestDB or Postgres. They assume:

- those databases already exist
- the application containers can reach them through environment variables
- a Docker network named `polymarket-backend` already exists

Create the network if needed:

```bash
docker network create polymarket-backend
```

## Configuration

Environment variables used across the Python and Rust services:

| Variable | Purpose |
| --- | --- |
| `GAMMA_ENDPOINT` | Polymarket metadata endpoint |
| `WS_URL` | Polymarket websocket base URL |
| `ALL_PROXY` | Optional outbound proxy for Python collectors |
| `QUEST_DB_HOST` | QuestDB host |
| `QUEST_DB_PORT` | QuestDB HTTP/ingress port |
| `QUEST_DB_PWP` | QuestDB Postgres wire protocol port |
| `QUEST_DB_ILP` | QuestDB ILP port used by the Rust app config |
| `POSTGRES_DB_HOST` | Postgres host |
| `POSTGRES_DB_PORT` | Postgres port |
| `POSTGRES_DB_USER` | Postgres username |
| `POSTGRES_DB_PASSWORD` | Postgres password |
| `POSTGRES_DB_NAME` | Postgres database name |

Notes:

- Python config loads `.env` automatically through `python-dotenv`.
- Rust config also loads `.env` via `dotenv`.
- `TOPIC` and `ROTATION_INTERVAL` appear in the compose files, but the current interval-specific collector entrypoints define their own topics and rotation windows in code.

Example `.env` skeleton:

```env
GAMMA_ENDPOINT=https://gamma-api.polymarket.com
WS_URL=wss://ws-subscriptions-clob.polymarket.com

QUEST_DB_HOST=localhost
QUEST_DB_PORT=9000
QUEST_DB_PWP=8812
QUEST_DB_ILP=9009

POSTGRES_DB_HOST=localhost
POSTGRES_DB_PORT=5432
POSTGRES_DB_USER=postgres
POSTGRES_DB_PASSWORD=kms
POSTGRES_DB_NAME=postgres

# Optional
ALL_PROXY=
```

## Running Locally

### 1. Install Python dependencies

```bash
uv sync
```

### 2. Run database migrations

Apply everything:

```bash
uv run python scripts/migrate.py --direction up --target all
```

Rollback everything:

```bash
uv run python scripts/migrate.py --direction down --target all
```

### 3. Run a single Polymarket collector

Examples:

```bash
uv run python -m services.polymarket.15m.entrypoint
uv run python -m services.polymarket.1h.entrypoint
uv run python -m services.polymarket.stocks.entrypoint
```

### 4. Run the Rust ingestion service

Build:

```bash
cargo build --workspace
```

Run:

```bash
cargo run -p app
```

### 5. Run the archiver

```bash
uv run python -m scripts.cron.archiver
```

The archiver runs an immediate backfill scan on startup and then schedules a daily run at `00:05 UTC`.

## Docker Compose Deployment

Use `docker-compose.yml` when you want the repo-managed services running together in containers.

Start the stack:

```bash
docker compose up --build
```

Services defined there:

- `polymarket-live-5m-crypto-data-collector`
- `polymarket-live-15m-crypto-data-collector`
- `polymarket-live-1h-crypto-data-collector`
- `polymarket-live-4h-crypto-data-collector`
- `polymarket-live-daily-crypto-data-collector`
- `polymarket-live-daily-stocks-data-collector`
- `questdb-archiver`
- `local-orderbook-service`

Volumes used by the compose setup:

- `./data:/app/data`
- `./logs:/app/logs`

Operational notes:

- The Python services use `Dockerfile.uv`.
- The Rust service uses `Dockerfile.rust`.
- `.env` is loaded through `env_file`.
- QuestDB and Postgres are expected to be external to this compose file.

## Portainer Deployment

Use `portainer-compose.yml` when deploying the application stack through Portainer.

Key differences from local Docker Compose:

- environment variables are declared explicitly instead of using `env_file`
- the service list currently starts at `15m`
- the `5m` collector is not present in the current Portainer spec

Portainer services currently defined:

- `polymarket-live-15m-crypto-data-collector`
- `polymarket-live-1h-crypto-data-collector`
- `polymarket-live-4h-crypto-data-collector`
- `polymarket-live-daily-crypto-data-collector`
- `polymarket-live-daily-stocks-data-collector`
- `questdb-archiver`
- `local-orderbook-service`

Recommended Portainer deployment strategy:

1. Make sure `polymarket-backend` exists on the target Docker host.
2. Provide the required QuestDB and Postgres environment variables in the stack.
3. Mount persistent `data` and `logs` paths if you need collector outputs and log retention.
4. Deploy the stack from the repository root so both Dockerfiles remain available to the builder.

## Database Migrations

Migration layout:

```text
migrations/
├── postgres/
│   ├── up/
│   └── down/
└── questdb/
    ├── up/
    └── down/
```

Run targeted migrations:

```bash
uv run python scripts/migrate.py --direction up --target postgres
uv run python scripts/migrate.py --direction up --target questdb
```

The current Postgres migration also seeds `table_registry` for the active QuestDB tables so the archiver can discover them.

## Development Commands

```bash
uv sync
uv run python scripts/migrate.py --direction up --target all
uv run python -m services.polymarket.15m.entrypoint
cargo build --workspace
cargo run -p app
cargo test --workspace
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

## Operations Notes

- Logs are written under `./logs` for both Python and Rust services.
- Python collectors ingest directly into QuestDB and do not require the Rust app to be running.
- The Rust app currently wires Binance and Bybit connectors in `crates/app/src/main.rs`.
- QuestDB retention is currently controlled at table level by migration SQL using `TTL`.
- Archival tracking depends on Postgres metadata being up to date with active QuestDB tables.

For more detail on archive metadata and seeding, see [docs/postgres-metadata-and-archiver.md](docs/postgres-metadata-and-archiver.md).
