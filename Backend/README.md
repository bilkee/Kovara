# Kovara Indexer

Event indexer for the Kovara Social contract on Stellar. Processes on-chain events and maintains a queryable database for the frontend.

> **Verified reference docs.** See [`docs/backend/`](../docs/backend/README.md) for
> the architecture, REST API contracts, and operational runbook derived from the
> code. When this README and those pages disagree, the code is the source of truth.

## Architecture

The indexer listens to Stellar contract events and processes them into a PostgreSQL database:

- **Event Handlers**: Process specific event types (PostCreated, TipEvent, LikeEvent, etc.)
- **Database**: PostgreSQL with migrations for schema management
- **Idempotency**: All handlers are idempotent using unique constraints and transaction hashes

### Runtime Flow,

The following describes the main runtime flow from startup through event handling:

1. **Configuration loading** (`src/config.ts`): Environment variables are validated and parsed before anything else runs. `DATABASE_URL` (which must be a `postgres://`/`postgresql://` connection string) and `CONTRACT_ID` (a 56-character Stellar `C…` contract strkey, checksum-verified) are required and have no fallback — startup fails immediately if either is missing or malformed. `STELLAR_RPC_URL` (default: Soroban testnet) and `START_LEDGER` (default: `0`, and rejected unless it is a finite non-negative integer) are optional but still validated when set. All configuration problems are reported together in a single error.

2. **Database initialization** (`src/index.ts:60-126`): A PostgreSQL connection pool is created. Three initialization steps run sequentially:
   - `ensureEventsTable()` — Creates the `events` table and supporting indexes if they do not exist (idempotent via `IF NOT EXISTS`).
   - `runMigrations()` — Applies any unapplied SQL migration files from the `migrations/` directory in numerical order.
   - `ensurePostSearchIndex()` — Adds and populates the `search_vector` column on the `posts` table for full-text search.

3. **API server startup** (`src/index.ts:205-207`): The Express app is created via `createApp(db)` and starts listening on the configured `HOST:PORT`. The API is ready to serve requests immediately, even before event streaming begins.

4. **Event streaming** (disabled in stub mode; `src/stream.ts`): When enabled, `streamEvents()` polls the Soroban RPC `getEvents` endpoint in a loop. Each batch of events is validated, normalized, deduplicated, and dispatched to the handler (`persistEvent`). The stream runs until an abort signal is received.

   - **Polling loop**: After each batch, the stream waits `POLL_INTERVAL_MS` (default 5s) before the next poll, unless a full page of events was returned (which implies more are available immediately).
   - **Deduplication**: An in-memory ring buffer of seen event IDs prevents redundant processing across overlapping RPC pages.
   - **Retry**: Transient network errors are retried up to 3 times with exponential backoff.

5. **Event replay** (`src/stream.ts`, BE-42): When `REPLAY_START_LEDGER` and `REPLAY_END_LEDGER` are set, the indexer starts in replay mode instead of live streaming. It iterates each ledger in the range and dispatches all matching events. This is useful for recovering from interruptions.

6. **Graceful shutdown**: On `SIGTERM` or `SIGINT`, the HTTP server stops accepting new connections and the process exits. In replay mode, the abort signal is passed so the replay loop can terminate early.

## Event Handlers

### Post Handlers (`src/handlers/post.ts`)

- **PostCreatedEvent**: Inserts new posts into the `posts` table
- **PostDeletedEvent**: Soft deletes posts by setting `deleted_at` timestamp

### Tip Handler (`src/handlers/tip.ts`)

- **TipEvent**: Records tips in `tips` table and increments `tip_total` on posts
- Idempotent via `tx_hash` unique constraint

### Like Handler (`src/handlers/like.ts`)

- **LikePostEvent**: Records likes in `likes` table and increments `like_count` on posts
- Idempotent via `(post_id, user_address)` unique constraint

## Database Migrations

Migrations live in the `migrations/` directory as numbered SQL files (e.g., `001_profiles.sql`).
On startup, the indexer automatically applies any unapplied migrations in order, tracking
them in a `schema_version` table.

```bash
# Manually trigger migrations (if running indexer with --skip-migrations):
npm run migrate
```

To add a new migration:

```bash
touch migrations/006_description.sql
# Write your DDL, then restart the indexer.
```

## Schema Versioning

The indexer uses a `schema_version` table to track which migrations have been applied:

```sql
CREATE TABLE schema_version (
    version    TEXT        PRIMARY KEY,
    name       TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## Error Handling

All API routes return structured JSON error responses with an `error` message and a
machine-readable `code` field:

```json
{ "error": "Profile not found", "code": "NOT_FOUND" }
```

## Event-Processing Reliability

A ledger event can legitimately be delivered more than once — `getEvents` pages
overlap on a cursor replay, a restart resumes from a persisted cursor, an
operator replays a range, or two replicas race during a handoff. These modules
make each of those a no-op rather than a second write.

| Module | Issue | Responsibility |
| --- | --- | --- |
| `src/idempotency.ts` | #648 | Derives, validates and parses the stable key for an event; `runOnce` executes a unit of work at most once per key. |
| `src/retry.ts` | #649 | `classifyFailure` splits errors into transient/permanent; `withRetry` applies jittered exponential backoff to the transient ones only. |
| `src/dead-letter.ts` | #650 | Counts attempts per event and dead-letters an unrecoverable failure with the full payload needed to debug and requeue it. |
| `src/aggregation/` | #651 | Daily price-index aggregation, leased so a day is computed exactly once across replicas. |

### Transient vs permanent

Retrying a permanent failure (a malformed payload, a unique-constraint
violation, a 4xx from the provider) burns the retry budget on work that can
never succeed and delays the dead-letter signal. `classifyFailure` therefore
checks, in order: an explicit `code` (SQLSTATE / syscall), then permanent
message markers, then transient markers, and **defaults to permanent** — an
error we do not understand cannot be fixed by repeating it, and the dead-letter
path exists so a human can look at it.

Backoff is exponential with **full jitter** (`random() * backoff`) rather than a
fixed delay: when a dependency restarts every replica fails at the same instant,
and a deterministic schedule would have them all retry in lockstep and reproduce
the overload.

### Idempotency keys

Keys are **derived, never caller-supplied**:

```
kovara:event:v1:<contractId>:<ledger>:<eventId>
```

Deriving them means a caller cannot reuse a key across two different events
(which would silently swallow the second). Uniqueness is enforced by the
database — `events.idempotency_key` has a unique index, and `persistEvent`
inserts with `ON CONFLICT DO NOTHING` — so two processes racing on the same
event cannot both win.

### Dead-letter queue

An event that fails permanently, or that has exhausted its retry budget, is
written to `event_dead_letters` with its topic, raw value, tx hash, ledger,
error and attempt count, so it can be reproduced and fixed. The queue keeps the
payload **unredacted** on purpose (log lines still redact payloads via
`src/logger.ts`): a redacted queue is useless for debugging. Records are
append-only; requeueing sets `requeued_at` rather than editing history.

`dead` events are excluded from startup recovery — they wait for an operator to
requeue them, and reclaiming them on every restart would defeat the queue.

### Daily aggregation

`AggregationScheduler` runs once an interval (default hourly) and aggregates the
most recent day that is still outstanding, so a window missed while the process
was down is caught up rather than skipped. Each run row in `aggregation_runs` is
the lease: a day is computed exactly once across replicas, a failed day stays
retryable, and the outcome is recorded either way.

Every aggregate carries both `run_date` (the day summarised) and `computed_at`
(when the job actually ran). They differ on a catch-up run, and conflating them
would make a late-computed historical day indistinguishable from a fresh one.

Prices are stored as `NUMERIC`, not `BIGINT`: the contract type is `i128`, and a
BIGINT would overflow on any token with 7+ decimals scaled into the smallest
unit, publishing a rounded — and wrong — cost-of-living index.

### Environment variables

All optional; the defaults are the ones documented in the modules above.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MAX_HANDLER_RETRIES` | `3` | Handler attempts before a failure is dead-lettered |
| `AGGREGATION_INTERVAL_MS` | `3600000` | Daily aggregation tick interval |
| `AGGREGATION_CATCH_UP_DAYS` | `7` | How many days back to catch up |

| HTTP Status | Code                | Description                     |
|-------------|---------------------|---------------------------------|
| 400         | `INVALID_QUERY`     | Invalid query parameters        |
| 400         | `LIMIT_EXCEEDED`    | Pagination limit too high       |
| 400         | `INVALID_ADDRESS`   | Missing or malformed address    |
| 400         | `INVALID_ID`        | Missing or malformed ID         |
| 404         | `NOT_FOUND`         | Resource not found              |
| 429         | `RATE_LIMIT_EXCEEDED` | Too many requests per IP      |
| 500         | `INTERNAL_ERROR`    | Unexpected server error         |

Unhandled errors are logged with request context (`[error] GET /api/profiles/GABC123: ...`)
and return a generic 500 response.

## Health Check

```bash
curl http://localhost:3000/health
```

Returns:

```json
{ "status": "ok", "uptime": 1234.56 }
```

## Version Endpoint

```bash
curl http://localhost:3000/version
```

Returns:

```json
{
  "version": "0.1.0",
  "git_commit": "abc1234",
  "build_time": "2024-01-15T10:30:00Z",
  "node_version": "v18.17.0"
}
```

The `version` field is read from `package.json`. The `git_commit` and
`build_time` fields can be injected via environment variables at build time
(`GIT_COMMIT`, `BUILD_TIME`) and default to `"unknown"` when not set.

## API Routes

## API Versioning and Deprecation

All public data API endpoints use a major-version prefix. The current stable
contract is **v1**, at `/api/v1`; for example,
`GET /api/v1/profiles/:address`. The health (`/health`) and build metadata
(`GET /version`) endpoints are operational endpoints and are intentionally not
versioned.

The existing unversioned `/api/*` paths remain temporarily available for v1
compatibility. Their responses include `Deprecation: true` and a
`Link: </api/v1/...>; rel="successor-version"` header. New integrations must
use `/api/v1`.

Breaking changes are introduced only in a new major API version (for example,
`/api/v2`). We will document the replacement before release, keep the prior
major version available for at least six months after announcing deprecation,
and publish the planned removal date in the release notes and this README.
Non-breaking additions may be made within an existing major version.

### Profiles

- `GET /api/v1/profiles/:address` — Get profile by Stellar address

### Version

- `GET /version` — Service version and build metadata (no auth required)

### Posts

- `GET /api/v1/posts?author=<address>&limit=<n>&offset=<n>` — List posts
- `GET /api/v1/posts/:id` — Get post by numeric ID
- `POST /api/v1/search/posts` — Full-text search (body: `{ "query": "...", "limit?", "offset?" }`)

### Follows

- `GET /api/v1/follows/:address/followers?limit=<n>&offset=<n>` — List followers
- `GET /api/v1/follows/:address/following?limit=<n>&offset=<n>` — List accounts the address follows

### Pools (Experimental)

- `GET /api/v1/pools/:id` — Get pool state by ID (enabled via `EXPERIMENTAL_FEATURES=true`)

### Debug Snapshot (BE-29)

- `GET /api/v1/debug/snapshot` — Export a JSON snapshot of posts, profiles, and pools for issue triage

Requires the `x-debug-token` header matching the `DEBUG_TOKEN` environment variable. If `DEBUG_TOKEN` is not set, the endpoint returns `503 Debug endpoint disabled`.

```bash
curl -H "x-debug-token: $DEBUG_TOKEN" http://localhost:3000/api/debug/snapshot
```

Response:

```json
{
  "posts": [...],
  "profiles": [...],
  "pools": [...],
  "generated_at": "2026-07-25T12:00:00.000Z",
  "post_count": 42,
  "profile_count": 10,
  "pool_count": 3
}
```

Each collection is capped at 1000 records. The `post_count`, `profile_count`, and `pool_count` fields reflect total counts in the database.

## Common Operational Tasks

### Starting the indexer

```bash
# Copy and configure environment
cp .env.example .env
# Edit .env with your values

# Start with Docker Compose (recommended)
docker compose up --build

# Or start manually
npm run dev        # development
npm run build && npm start   # production
```

### Running a replay after interruption

If the indexer was interrupted and missed some ledgers, set the replay range:

```bash
REPLAY_START_LEDGER=12345 REPLAY_END_LEDGER=13000 npm start
```

The indexer will process every ledger in the range [12345, 13000], then stop. After replay completes, remove the `REPLAY_*` variables and restart for live streaming.

### Checking indexer health

```bash
curl http://localhost:3000/health
# Expected: {"status":"ok","uptime":1234.56,"db":"ok"}
```

### Viewing version metadata

```bash
curl http://localhost:3000/version
```

### Exporting a debug snapshot

```bash
curl -H "x-debug-token: $DEBUG_TOKEN" http://localhost:3000/api/debug/snapshot
```

### Applying migrations manually

```bash
npm run migrate
```

### Adding a new migration

```bash
touch migrations/006_description.sql
# Write DDL, then restart the indexer.
```

### Adjusting timeouts and pool size for external dependencies

```bash
# Database pool tuning (all optional, with documented defaults)
DB_POOL_MAX=20
DB_POOL_CONNECTION_TIMEOUT_MS=5000
DB_POOL_IDLE_TIMEOUT_MS=30000
DB_STATEMENT_TIMEOUT_MS=30000  # legacy alias: QUERY_TIMEOUT_MS

# RPC fetch timeout (ms, default 15000)
RPC_FETCH_TIMEOUT_MS=30000

# API request timeout (ms, default 30000)
REQUEST_TIMEOUT_MS=60000
```

For different deployment sizes, adjust `DB_POOL_MAX` (small: `5`, medium: `10`, large: `20-30`) and timeouts without rebuilding — all are env-configurable with the defaults above.

### Monitoring the indexer

Key metrics to track:
- **Events per second**: Rate at which contract events are processed.
- **Database query latency**: Time spent in PostgreSQL queries.
- **Failed event count**: Events that could not be persisted.
- **Current indexed ledger**: The latest ledger that has been processed.

Logs include structured context for correlation:
```
[indexer] ledger=12345 type=PostCreatedEvent tx=abc...
[stream] Starting from ledger 100, contract=CDEF...
```

## Running Tests

```bash
# Run all tests
npm test

# Run route tests specifically
npm test -- routes
```

## Database Schema

### Posts Table

```sql
CREATE TABLE posts (
    id BIGINT PRIMARY KEY,
    author TEXT NOT NULL,
    content TEXT NOT NULL,
    tip_total BIGINT NOT NULL DEFAULT 0,
    like_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP DEFAULT NULL
);
```

### Tips Table

```sql
CREATE TABLE tips (
    id SERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id),
    tipper TEXT NOT NULL,
    amount BIGINT NOT NULL,
    fee BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    tx_hash TEXT NOT NULL UNIQUE
);
```

### Likes Table

```sql
CREATE TABLE likes (
    id SERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id),
    user_address TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    tx_hash TEXT NOT NULL UNIQUE,
    UNIQUE (post_id, user_address)
);
```

## Local Setup (Docker)

The fastest way to run the indexer and PostgreSQL together is Docker Compose.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Compose v2

### Steps

```bash
# 1. Copy and edit environment variables
cp .env.example .env
# Edit .env — set CONTRACT_ID, START_LEDGER, and STELLAR_RPC_URL at minimum

# 2. Start both services (migrations run automatically on first boot)
docker compose up --build
```

The indexer API will be available at `http://localhost:3000`.
PostgreSQL is exposed on port `5432`.

To stop and remove containers:

```bash
docker compose down
```

To also remove the database volume:

```bash
docker compose down -v
```

### Environment Variables

See [`.env.example`](.env.example) for all required variables.

| Variable               | Description                                                         |
| ---------------------- | ------------------------------------------------------------------- |
| `DATABASE_URL`         | PostgreSQL connection string                                        |
| `STELLAR_RPC_URL`      | Soroban RPC endpoint                                                |
| `CONTRACT_ID`          | Deployed Kovara contract address                                    |
| `START_LEDGER`         | Ledger sequence to start indexing from                              |
| `HOST`                 | Bind address for the API server (recommended: `0.0.0.0`)            |
| `PORT`                 | API port (default: `3000`)                                          |
| `TRUST_PROXY`          | Express trust-proxy setting; set to `1` only behind a trusted proxy |
| `RATE_LIMIT_WINDOW_MS` | Rate-limit window in milliseconds (default: `60000`)                |
| `RATE_LIMIT_MAX`       | Maximum requests per window per IP (default: `100`)                |
| `GIT_COMMIT`           | Git commit hash (populated in `/version` response)                 |
| `BUILD_TIME`           | ISO 8601 build timestamp (populated in `/version` response)        |
| `CORS_ORIGIN`          | Allowed CORS origin(s) (default: all)  |
| `DB_POOL_MAX`          | PostgreSQL pool max clients (default: `10`)                         |
| `DB_POOL_CONNECTION_TIMEOUT_MS` | PostgreSQL pool connection timeout in ms (default: `5000`, `0` = no timeout) |
| `DB_POOL_IDLE_TIMEOUT_MS` | PostgreSQL pool idle timeout in ms (default: `30000`)              |
| `DB_STATEMENT_TIMEOUT_MS` | PostgreSQL statement timeout in ms (default: `30000`; legacy alias `QUERY_TIMEOUT_MS` still honored) |
| `QUERY_TIMEOUT_MS`     | Legacy alias for `DB_STATEMENT_TIMEOUT_MS` (default: `30000`)      |
| `RPC_FETCH_TIMEOUT_MS` | Soroban RPC fetch timeout in milliseconds (default: `15000`)       |
| `REQUEST_TIMEOUT_MS`   | HTTP request timeout in milliseconds (default: `30000`)            |
| `ENABLE_AUTH_MIDDLEWARE` | Enable authentication middleware (default: `false`)                |
| `ENABLE_RATE_LIMITING`   | Enable rate limiting middleware (default: `true`)                  |
| `EXPERIMENTAL_FEATURES`  | Enable experimental routes (e.g., pools) (default: `false`)        |
| `REPLAY_START_LEDGER`  | Start ledger for event replay (omit for live streaming)            |
| `REPLAY_END_LEDGER`    | End ledger for event replay (inclusive, requires `REPLAY_START_LEDGER`) |


### Secure environment configuration

For production deployments, keep the API bound to a non-public interface unless you need external access, and only trust proxy headers from your reverse proxy:

```bash
HOST=0.0.0.0
PORT=3000
TRUST_PROXY=1
```

If the indexer is exposed directly or behind a network you do not control, leave `TRUST_PROXY=0` so forwarded client IPs are not trusted implicitly.

## Manual Setup

### Prerequisites

- Node.js 18+
- PostgreSQL 14+

### Installation

```bash
npm install
```

### Database Setup

```bash
# Apply migrations manually
psql "$DATABASE_URL" -f migrations/001_profiles.sql
psql "$DATABASE_URL" -f migrations/002_posts.sql
psql "$DATABASE_URL" -f migrations/003_follows.sql
psql "$DATABASE_URL" -f migrations/004_tips_likes.sql
psql "$DATABASE_URL" -f migrations/005_pools.sql
```

### Configuration

```bash
cp .env.example .env
# Edit .env with your values
```

## Running

```bash
# Development
npm run dev

# Production
npm run build
npm start
```

## Testing

```bash
# Run all tests
npm test

# Run with coverage
npm run test:coverage

# Run specific test file
npm test -- post.test.ts
```

## Idempotency

All event handlers are designed to be idempotent:

1. **PostCreatedEvent**: Uses `ON CONFLICT (id) DO NOTHING`
2. **PostDeletedEvent**: Only updates if `deleted_at IS NULL`
3. **TipEvent**: Uses `tx_hash` unique constraint
4. **LikeEvent**: Uses `(post_id, user_address)` unique constraint

This ensures the indexer can safely replay events without data corruption.

## CORS

The API uses the [`cors`](https://www.npmjs.com/package/cors) middleware and allows
all origins by default. To restrict access in production, set the `CORS_ORIGIN`
environment variable:

```bash
# Allow a single origin
CORS_ORIGIN=https://app.example.com

# Allow multiple origins (comma-separated)
CORS_ORIGIN=https://app.example.com,https://admin.example.com
```

When `CORS_ORIGIN` is not set, all origins are permitted (useful during development).
See `.env.example` for the full list of environment variables.

## Rate Limiting

All `/api/*` routes are protected by a rate limiter (express-rate-limit) by default.
Rate limiting can be disabled by setting `ENABLE_RATE_LIMITING=false`. The
default window is 60 seconds with 100 requests per IP. Configurable via:

| Variable               | Default | Description                          |
| ---------------------- | ------- | ------------------------------------ |
| `RATE_LIMIT_WINDOW_MS` | `60000` | Window duration in milliseconds      |
| `RATE_LIMIT_MAX`       | `100`   | Maximum requests per window per IP   |

When the limit is exceeded, the API returns `429 Too Many Requests` with a
`Retry-After` header and a JSON body containing `RATE_LIMIT_EXCEEDED`.

## Full-Text Search

Search across indexed post content:

```bash
curl -X POST http://localhost:3000/api/search/posts \
  -H "Content-Type: application/json" \
  -d '{"query": "stellar", "limit": 10, "offset": 0}'
```

The search uses PostgreSQL full-text search (`tsvector`/`tsquery`) for efficient
content matching. Results include `id`, `author`, `content`, `tip_total`,
`like_count`, and `created_ledger`.

## Token Metadata Enrichment

Pool responses include optional token metadata when available:

```json
{
  "pool_id": "...",
  "token": "GABCD...",
  "token_name": "Kovara Token",
  "token_symbol": "KOVA",
  "token_decimals": 7,
  ...
}
```

Metadata is fetched via the `getTokenMetadata` database method, which can be
populated from on-chain contract data or a supplementary table.

## Monitoring

### Health Check

```bash
curl http://localhost:3000/health
```

Returns `200 OK` with `{ "status": "ok", "uptime": <seconds> }`.

### Metrics

- Events processed per second
- Database query latency
- Failed event count
- Current indexed ledger

## Deployment

### Docker

```bash
docker build -t Kovara-indexer .
docker run -p 3000:3000 --env-file .env Kovara-indexer
```

### Kubernetes

```bash
kubectl apply -f k8s/deployment.yaml
```

## Troubleshooting

### Indexer falls behind

- Check Stellar RPC rate limits
- Increase database connection pool size
- Scale horizontally with multiple indexer instances

### Duplicate events

- Verify idempotency constraints are in place
- Check transaction hash uniqueness
- Review event replay logic

### Missing events

- Verify START_LEDGER is correct
- Check Stellar RPC connectivity
- Review event filter configuration

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

MIT

