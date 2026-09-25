# Kovara Indexer

Event indexer for the Kovara Social contract on Stellar. Processes on-chain events and maintains a queryable database for the frontend.

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

### Tiered Limits

A single budget per IP treats a `POST /tips` and a `GET /health` as equivalent,
even though one costs a transaction and the other costs nothing. Endpoints that
are genuinely more expensive get a tighter budget:

| Tier    | Applies to                                       | Default budget |
| ------- | ------------------------------------------------ | -------------- |
| search  | `GET` on `/search`, `/leaderboard`, `/index`, `/analytics`, `/history` | 30 / 60s |
| write   | `POST`, `PUT`, `PATCH`, `DELETE`                 | 20 / 60s       |

A tier rejection returns `429` with `RATE_LIMIT_TIER_EXCEEDED`, a `retry_after`
field, and an `X-RateLimit-Tier` header. Budgets are configurable in code via
`setTierConfig()`, and the whole mechanism is off with
`ENABLE_TIERED_RATE_LIMITS=false`.

### Abuse Detection

`ENABLE_ABUSE_DETECTION=false` disables the detector. A per-window counter
cannot see a client that stays just under the budget — 1 request/second is
60/minute against a 100/minute limit, so it is never throttled and can read the
whole database indefinitely. The detector watches the *shape* of traffic rather
than its volume:

| Signal       | Detected by                                                    |
| ------------ | -------------------------------------------------------------- |
| `enumeration`| Many distinct resources, most returning 404/400                |
| `scraping`   | Many distinct real resources with few repeats                  |
| `burst`      | Requests concentrated in a 1-second sub-window                 |
| `scanner`    | Traversal/SQLi in the path, or a known scanner user agent      |

Cooldowns are **temporary and escalating** (30s, doubling, capped at 15 min)
rather than permanent. A permanent ban triggered by a heuristic is
unrecoverable when the heuristic is wrong — a shared NAT or corporate proxy
would take out every user behind it with no way back but operator intervention.
Identities are hashed, so logs do not become a registry of who was throttled.

Operator endpoints:

```bash
curl http://localhost:3000/api/v1/abuse
# { trackedIdentities, detections, blockedRequests, blockedIdentities,
#   recentEvents: [...], config: {...} }

curl -X POST http://localhost:3000/api/v1/abuse/unblock \
  -H "Content-Type: application/json" -d '{"identity":"ip:1a2b3c4d5e6f7a8b"}'
```

`unblock` lifts a block early — the escape hatch for a false positive — and
returns `404` rather than a silent `200` when no identity matched, so a typo is
visible.

## Unified Search

`GET /api/v1/search` searches profiles, posts, and categories together and
returns them in one ranked, interleaved page:

```bash
curl "http://localhost:3000/api/v1/search?q=stellar&type=posts,profiles&limit=20&offset=0"
```

| Param    | Default | Notes                                          |
| -------- | ------- | ---------------------------------------------- |
| `q`      | —       | Required, 1–200 chars, whitespace-normalized    |
| `type`   | all     | Comma-separated subset of the entity names     |
| `limit`  | `20`    | Max 100                                        |
| `offset` | `0`     | Max 10000, to bound deep-scan cost             |

Ranking combines text relevance (0.7), log-scaled engagement (0.2), and recency
(0.1), with `id` as a final tiebreak so the order is **total and reproducible** —
a relevance order that varies between identical requests makes a client paging
through results see rows repeat and skip.

Two supporting endpoints: `GET /api/v1/search/facets?q=…` for per-entity counts,
and `GET /api/v1/search/cache` for live cache counters.

### Performance

The existing `POST /search/posts` matched with

```sql
search_vector @@ plainto_tsquery('simple', $1) OR content ILIKE '%' || $1 || '%'
```

A leading-wildcard `ILIKE` cannot use a btree index, so the planner cannot prove
the `OR` is selective and falls back to a **sequential scan** — meaning the GIN
index on `search_vector` was never actually used.

Migration `013_search_performance.sql` fixes this by enabling `pg_trgm` and
adding a GIN trigram index on `posts.content`, so *both* branches of the `OR` are
index-backed and the planner can prefer an index scan. It also recreates
`idx_posts_search_vector` as a partial index (`WHERE deleted_at IS NULL`), since
deleted posts can never be returned by any search path.

`pg_trgm` is created inside a `DO` block that tolerates `insufficient_privilege`
and `undefined_file`. Where it is unavailable the migration still succeeds and
substring search degrades to the same sequential scan as before — it is a
performance migration, never a hard startup failure.

### Optional columns

`posts.search_vector` is created at runtime by `ensurePostSearchIndex()` and
`posts.category` is not created by any migration in this repository. SQL naming a
non-existent column is a *parse* error, so a `COALESCE(search_vector, …)` fallback
cannot help. The store therefore probes `information_schema.columns` once, caches
the result for 60s, and collapses concurrent probes into one query. Missing
`search_vector` falls back to an inline `to_tsvector`; missing `category` yields
an empty result set, because "no categories exist" is the correct answer for a
schema that has none.

## Response Caching

Cached endpoints return a pre-serialized JSON body with an `X-Cache` header of
`HIT-MISS` or `STALE`, so a client can tell a fresh result from a stale-but-served
one rather than silently acting on stale data.

Invalidation uses the shared `cache_epoch` table rather than TTL alone. These
services run multiple replicas against one Postgres, and a write on replica A is
invisible to replica B's memory for the whole TTL — a read-your-own-write
violation that looks like a cache bug and is actually a design bug. Every writer
bumps the epoch; a reader compares it against the epoch its entry was stored
with. The epoch is polled at most once per 500ms, because reading it per request
would add a database round trip to each one and give back what caching saves.
Consistency bound: a cross-replica write becomes visible within 500ms, and never
later than the entry TTL.

Misses are protected by **single-flight**: concurrent misses on one key share a
single upstream call. Without it, a hot key expiring sends every concurrent
request to the database at once, multiplying load at exactly the moment the
database can least absorb it. Past the fresh window the last known value is
served while a refresh runs in the background; if that refresh fails the previous
value is served **flagged stale** rather than deleted, so a blip does not empty
the cache and turn every key into a cold miss.

## Response Serialization

`api/serialize.ts` replaces per-route conversion and does not rely on the global
`BigInt.prototype.toJSON` override:

```ts
import { defineSerializer } from "./api/serialize";

const serializePost = defineSerializer<PostRow>({
  id: {},
  author: {},
  tip_total: {},
  // creator_token is an internal column: never named, therefore never published.
});
```

Conversions are explicit: `bigint` → decimal **string** (never `Number`, which
would silently round past 2^53-1 — tip totals in the smallest unit exceed that),
`Date` → ISO 8601, invalid `Date` → `null`, `NaN`/`Infinity` → `null`,
`Buffer` → base64, `undefined`/functions/symbols → omitted. Circular references
throw with the path rather than emitting `"[Circular]"`, because a cycle is a
programming error and shipping a marker turns a bug into a silently wrong
contract.

**Tenant-safe** is the property that rules out a plain replacer. A response
assembled from a database row carries whatever columns happened to be there, so
the safe path is a *declared schema*: the service states which fields it intends
to publish and the serializer projects exactly those. A `SELECT *` that gains
`creator_token` cannot start leaking it, because it was never named.
Untrusted `toJSON` is ignored by default, and `__proto__` / `constructor` /
`prototype` keys are dropped.

## Full-Text Search

Legacy single-entity search, retained for existing clients:

```bash
curl -X POST http://localhost:3000/api/search/posts \
  -H "Content-Type: application/json" \
  -d '{"query": "stellar", "limit": 10, "offset": 0}'
```

The search uses PostgreSQL full-text search (`tsvector`/`tsquery`) for efficient
content matching. Results include `id`, `author`, `content`, `tip_total`,
`like_count`, and `created_ledger`. See
[Performance](#performance-1) for the index situation.

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
