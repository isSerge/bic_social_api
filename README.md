# bic_social_api

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A social likes API built with Rust, Axum, PostgreSQL, and Redis.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture](#architecture)
3. [Development Setup](#development-setup)
4. [Local DB Setup](#local-db-setup-for-sqlx-compile-time-checks)
5. [Running with Docker Compose](#running-with-docker-compose)
6. [Manual Testing](#manual-testing)
7. [Trade-offs](#trade-offs)
8. [Further Improvements](#further-improvements)
9. [Security: Diagnostic Endpoints](#security-diagnostic-endpoints)

---

## Quick Start

The fastest path is Docker Compose — no local Rust toolchain, database, or Redis needed:

```bash
docker compose up --build
```

The API is available at `http://localhost:8080` once all services are healthy (~30 s on first run while images build). See [Running with Docker Compose](#running-with-docker-compose) for details and seeded test credentials.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Clients                          │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP/1.1
┌────────────────────────▼────────────────────────────────┐
│                      social-api :8080                   │
│                                                         │
│  Axum Router                                            │
│  ├── protected (auth + rate-limit per user)             │
│  │     POST   /v1/likes                                 │
│  │     DELETE /v1/likes/{type}/{id}                     │
│  │     GET    /v1/likes/{type}/{id}/status              │
│  │     GET    /v1/likes/user                            │
│  │     POST   /v1/likes/batch/statuses                  │
│  ├── public (rate-limit per IP)                         │
│  │     GET    /v1/likes/{type}/{id}/count               │
│  │     POST   /v1/likes/batch/counts                    │
│  │     GET    /v1/likes/top                             │
│  │     GET    /v1/likes/stream  (SSE)                   │
│  └── diagnostic (no auth, internal only)                │
│        GET    /health/live  /health/ready  /metrics     │
│                                                         │
│  Middleware stack (outer → inner)                       │
│  TraceLayer → metrics → request_id → auth → rate_limit  │
└──────┬──────────────────┬──────────────────┬────────────┘
       │                  │                  │
┌──────▼──────┐  ┌────────▼───────┐  ┌──────▼──────────┐
│  PostgreSQL │  │     Redis      │  │  Profile API    │
│  :5432      │  │    :6379       │  │  :8084          │
│             │  │                │  │ (token → UUID)  │
│ like_events │  │ count cache    │  └─────────────────┘
│ like_counts │  │ like status    │  ┌─────────────────┐
│ (write + RO │  │ leaderboard    │  │  Content API    │
│  replica)   │  │ rate limits    │  │  :8081          │
└─────────────┘  │ auth token     │  │ (ID validation) │
                 └────────────────┘  └─────────────────┘
```

**Key design decisions:**

- **Redis read-aside cache** — like counts, leaderboards, and per-user like status are cached in Redis to shield the DB from read traffic. Writes invalidate/update the relevant keys.
- **Write-ahead DB + async SSE broadcast** — every `like`/`unlike` is committed to Postgres first, then broadcast over an in-process `tokio::sync::broadcast` channel to SSE subscribers, ensuring durability before notification.
- **Circuit breaker on outbound HTTP** — calls to the profile API and content API are wrapped in a circuit breaker (closed → open → half-open) to prevent cascading failures if a dependency degrades.
- **Rate limiting** — authenticated writes are bucketed per user ID; public reads per IP; public write-method endpoints (e.g. `batch/counts`) per IP in a separate write bucket so they cannot exhaust the read quota.
- **SQLX offline mode** — `.sqlx/` query metadata is checked in so the project compiles without a live database in CI and local non-migration builds.

### Why Cursor-Based Pagination?
The `/v1/likes/user` endpoint implements cursor-based pagination instead of traditional `LIMIT`/`OFFSET` for two critical reasons:
1. **Performance at Scale:** `OFFSET Y` requires the database to scan and discard `Y` rows before returning the result. For deep pages, this degrades from $O(1)$ to $O(N)$ and becomes a severe bottleneck. Cursor pagination uses a composite index `(created_at DESC, content_id DESC)` to seek directly to the last seen row in $O(1)$ time.
2. **Data Consistency:** In a highly active social app, users are constantly liking and unliking content. If a user is on Page 1 and a new like is inserted, traditional `OFFSET` shifts all items down, causing the user to see duplicate items on Page 2. Cursors provide a stable anchor, immune to insertion/deletion drift.

---

## Development Setup

### Prerequisites

- Rust (stable) — install via [rustup](https://rustup.rs)
- [cargo-sqlx](https://crates.io/crates/sqlx-cli) for migrations: `cargo install sqlx-cli --no-default-features --features rustls,postgres`
- Docker (for the local Postgres used by integration tests and offline metadata prep)

### Build & run tests

```bash
# Compile — SQLX_OFFLINE=true is set in .cargo/config.toml, no live DB required
cargo build

# Run the full test suite
cargo test

# Run with output visible (useful during development)
cargo test -- --nocapture
```

### Test coverage

Requires [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov):

```bash
cargo install cargo-llvm-cov
```

```bash
# Generate an HTML report and open it in the browser
RUST_TEST_THREADS=1 cargo +stable llvm-cov --html --open
```

> `RUST_TEST_THREADS=1` serialises test execution so that llvm-cov can merge coverage data correctly across all tests.

### Linting

```bash
cargo fmt --check                     # formatting
cargo clippy -- -D warnings           # lints (CI-equivalent)
```

---

## Local DB Setup (for SQLx compile-time checks)

`sqlx::query!` macros require a reachable database schema at compile time unless offline metadata is prepared. If `cargo build` fails without `DATABASE_URL`, use the setup below.

Project defaults are configured in `.cargo/config.toml`:
- `SQLX_OFFLINE=true` so compile-time SQLx checks use checked-in `.sqlx/` metadata
- `TEST_DATABASE_URL=postgres://social:social_password@localhost:55432/social_api` for repository tests

With this setup, you generally do not need to export env vars manually for `cargo check`, `cargo build`, or repository tests.

### 1. Start a temporary local Postgres

```bash
docker rm -f bic_social_pg_temp >/dev/null 2>&1 || true
docker run --name bic_social_pg_temp \
  -e POSTGRES_USER=social \
  -e POSTGRES_PASSWORD=social_password \
  -e POSTGRES_DB=postgres \
  -p 55432:5432 \
  -d postgres:16
```

### 2. Wait until Postgres is ready

```bash
until docker exec bic_social_pg_temp pg_isready -U social -d postgres >/dev/null 2>&1; do sleep 1; done
echo "postgres ready"
```

### 3. Create database and run migrations

```bash
DATABASE_URL=postgres://social:social_password@localhost:55432/social_api cargo sqlx database create
DATABASE_URL=postgres://social:social_password@localhost:55432/social_api cargo sqlx migrate run
```

### 4. Build with SQLx macros enabled

```bash
DATABASE_URL=postgres://social:social_password@localhost:55432/social_api cargo build
```

### 5. (Optional) Prepare offline SQLx metadata

```bash
DATABASE_URL=postgres://social:social_password@localhost:55432/social_api cargo sqlx prepare
```

If `.sqlx/` is committed, future builds can use offline metadata without requiring a live DB.

### 6. Stop and remove temporary DB

```bash
docker rm -f bic_social_pg_temp
```

---

## Running with Docker Compose <a name="running-with-docker-compose"></a>

`docker compose up --build` starts the entire system — no local Rust toolchain or running database required.

**Services:**

| Service | Image / Build | Port | Purpose |
|---|---|---|---|
| `postgres` | `postgres:16` | — | Primary + read-replica database |
| `redis` | `redis:7-alpine` | — | Cache and rate-limit store |
| `mock-profile-api` | `Dockerfile.mocks` | 8084 | Validates Bearer tokens, returns user UUIDs |
| `mock-content-api` | `Dockerfile.mocks` | 8081 | Serves all content types (`post`, `bonus_hunter`, `top_picks`) |
| `social-api` | `Dockerfile` | **8080** | The application |

`social-api` will not start until postgres and redis pass their health checks (and all mocks are healthy). Migrations run automatically on first startup.

```bash
# Start everything (first run builds images, ~5 min)
docker compose up --build

# Rebuild only the application after a source change
docker compose up --build social-api

# Tear down (keeps named volumes)
docker compose down

# Tear down and wipe data volumes
docker compose down -v
```

**Seeded test data:**

*Tokens (mock-profile-api):*
`tok_user_1` … `tok_user_5` — each maps to a stable UUID `usr_550e8400-…-44016554400{01-05}`

*Content IDs (mock-content-api):*
- `post`: `731b0395-4888-4822-b516-05b4b7bf2089`, `9601c044-6130-4ee5-a155-96570e05a02f`, and 8 more
- `bonus_hunter`: `c3d4e5f6-a7b8-9012-cdef-123456789012` and 4 more
- `top_picks`: `e8b15d9a-1153-4e8c-8c63-42eb43ccbce3` and 4 more

---

## Manual Testing

A ready-made smoke-test script covering all endpoints is provided:

```bash
bash test_curls.sh
```

The script uses the seeded tokens and content IDs above and reports `OK` / `FAIL` per check. Run it any time after `docker compose up --build` to verify the stack is healthy.

---

## Trade-offs

- **In-process SSE broadcast** — the `tokio::sync::broadcast` channel is fast and zero-dependency, but state is not shared across multiple API instances. A horizontally scaled deployment would need Redis Pub/Sub or a similar broker to fan out events across pods.
- **Leaderboard via Background Worker** — To avoid expensive aggregates on the read path, a background worker (`LeaderboardWorker`) pre-computes the top 50 items and stores them in a Redis Sorted Set (`ZREVRANGE`) every 60 seconds. The API only falls back to a full DB query if Redis is completely empty or down.
- **Single writer, RO replica** — the schema separates write and read pools, but both point to the same Postgres instance in the Docker Compose setup. True replica lag handling is not implemented.
- **Mock services for auth/content** — the profile and content APIs are stubbed with in-process mocks. Real implementations would add network latency; the circuit breaker is already wired but has not been load-tested against a real dependency.
- **No pagination on `top_liked`** — the leaderboard endpoint returns a fixed `limit` slice. Cursor-based pagination would be needed if clients need to page through a longer list.
- **`batch/counts` is a public POST** — POST is used to carry a large JSON body since GET has URL-length limits. It is rate-limited per IP in a dedicated write bucket, but unauthenticated bulk access is still possible; moving it behind auth would be more restrictive.
- **Denormalized `like_counts` table** — avoids expensive `COUNT(*)` scans on the `likes` table but requires an atomic UPSERT on every write; both rows are updated in the same transaction so consistency is strong, but it doubles write surface.
- **`idx_likes_created_at` is a global write-hot index** — every like/unlike appends to this index regardless of content type; useful for the global recent-activity query but may become a write bottleneck at scale.

---

## Further Improvements

- **Horizontal scaling** — replace the in-process SSE broadcaster with Redis Pub/Sub so multiple replicas can fan out live-update events
- **Leaderboard on write** — maintain a Redis sorted set (`ZADD`) on every like/unlike instead of querying Postgres on cache miss
- **Cursor pagination on `top_liked`** — add `cursor` / `limit` params consistent with the `GET /user` endpoint
- **OpenAPI spec** — generate `openapi.json` via `utoipa` or `aide` and serve it at `/docs`
- **Integration test suite** — add a Docker Compose–based test profile that spins up real Postgres + Redis and runs end-to-end scenario tests
- **Increase test coverage** — further expand unit and integration tests
- **Fuzz testing** — apply [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) / [`bolero`](https://github.com/camshaft/bolero) targets to request deserialisation, pagination parsing, and rate-limit key generation to catch unexpected panics on malformed input
- **More descriptive doc comments** — further extend `///` coverage to struct and methods, include param description.
- **Metrics dashboard** — ship a Grafana dashboard definition alongside the Prometheus `/metrics` endpoint
- **`cargo-chef` in Dockerfile** — replace the dummy `main.rs` layer trick with a proper recipe-based dependency cache
- **Distroless runtime image** — swap `debian:bookworm-slim` for `gcr.io/distroless/cc-debian12` to shrink attack surface
- **Partial index on `likes`** — replace `idx_likes_created_at` with a partial index filtered to a recent time window (e.g. `WHERE created_at > now() - interval '30 days'`) to bound index size as the table grows

---

## Security: Diagnostic Endpoints

`/health/*` and `/metrics` perform database/cache pings and generate large payloads. They are intentionally not behind the rate limiter — blocking them during a Redis outage would cause Kubernetes readiness probes to fail and trigger cascading pod restarts.

**Production posture:** these routes must not be exposed to the public internet. They are intended exclusively for internal cluster traffic (Kubelet probes and Prometheus scraping). An external API gateway / ingress controller must drop all external requests to `/health` or `/metrics`.

