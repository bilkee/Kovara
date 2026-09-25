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

## Rewards, audit, and feeds (#656-#659)

### Reward calculation (#656)

`src/rewards/rules.ts` is pure. It reads the recorded submission and
verification facts and returns integers — no clock, no randomness, no globals —
so the same stored data always produces the same rewards. That is what makes a
disputed payout arguable rather than mysterious.

| Rule | Behaviour |
| --- | --- |
| Rejected submission | Pays nothing — not to the submitter, and not to the verifier who rejected it. Rejecting bad data is a cost the protocol bears, not a paid service. |
| Pending submission | Pays nothing yet; verifier rewards are also withheld, because a pending submission can still be rejected. |
| Verified submission | `base`, scaled by the corroboration multiplier, plus `verificationReward` to each deduplicated approving verifier. |
| Flagged for review | The submitter's reward is **held** at `pendingReviewBps` and marked `pending`, not `claimable`. Held rather than clawed back, so a reversal never has to recover money already spent. |

Corroboration is weighted `1/n^0.5` per submitter and capped, so agreement is
rewarded but a colluding group cannot scale a payout without limit.

**Duplicate votes are prevented twice.** In application code by
`dedupeVerifications`, and in the database by `PRIMARY KEY (submission_id,
verifier)` on `verifications` — so the rule holds even if a caller forgets to
check. The *first* vote is the one that counts: a verifier who approves and then
rejects is recorded as a duplicate rather than converting an approval into a
rejection, because letting someone change their vote after seeing the reward
would make the reward a function of when they looked.

**Self-verification pays nothing.** An address cannot both create data and
certify it.

All amounts are `bigint`; multipliers are basis points applied with integer
arithmetic, so the rules and the ledger cannot drift apart by a rounding step.

### Claims (#657)

`processing` is a real state, not a formality.

- **No double-claim.** Accruals are claimed under `FOR UPDATE`, so two
  concurrent claims of the same rows serialise and the second finds nothing
  claimable. `idempotency_key` is `UNIQUE`, and that constraint — not the
  application check — is what makes a concurrent retry resolve to one payout.
- **A failed payout releases the accruals** back to `claimable`. A crash
  mid-payout leaves the money `processing` and visible as such, rather than lost
  or double-paid.
- **A completed claim is never reversed.** The transfer already happened;
  pretending otherwise would be a lie in the ledger.

Balances are always derived by summing `reward_accruals`; no running total is
stored, so a total and its underlying lines cannot disagree. An in-flight claim
is reported separately from `claimed` so a payout in progress reads as in
progress.

```
GET  /api/v1/rewards/:address
GET  /api/v1/rewards/:address/claims?state=&limit=&offset=
GET  /api/v1/rewards/:address/accruals?state=&limit=&offset=
POST /api/v1/rewards/:address/claim      { "idempotency_key": "..." }
```

`idempotency_key` is **required** on claim. A client that times out mid-request
cannot tell whether the payout landed, so a retry has to be safe by default —
which means the key comes from the caller, not from the server. Reusing a key
returns the original claim and moves no money.

A claim with nothing claimable returns `200` with a zero-amount `processing`
claim: that is a statement about the caller's balance, not a server fault.

### Audit log (#658)

An audit log that an operator with write access can quietly edit is not an audit
log. Each entry hashes its own canonical content **and** the previous entry's
hash:

```
hash(n) = SHA256(canonical(record n) || hash(n-1))
```

Altering or removing any record breaks verification for every record after it,
and `GET /api/v1/audit/:stream/verify` reports the first break. The canonical
form is fixed-order and length-prefixed, not `JSON.stringify` of an arbitrary
object: field order is not guaranteed across versions, and without a length
prefix `{a: "bc"}` and `{ab: "c"}` could hash identically.

The chain is **per stream** (one per contract), so concurrent writers for
different contracts do not serialise. Appends lock the stream's head row — the
alternative, read-head-then-insert, lets two concurrent appends read the same
head and silently fork the chain.

> **Operational note.** The chain only means something if the application role
> cannot `UPDATE` or `DELETE` `audit_log` in production. Revoke those grants
> after setup:
>
> ```sql
> REVOKE UPDATE, DELETE ON audit_log FROM <app_role>;
> ```

Audit writes never fail an event. A full audit table must not stall the indexer,
so a write error is logged loudly and the event proceeds; a gap is detectable
afterwards because the chain records where a record should have continued from.

```
GET /api/v1/audit?stream=&action=&outcome=&subject=&actor=&ledger=&from=&to=&limit=&offset=
GET /api/v1/audit/:stream/verify
```

### Submission feed (#659)

```
GET /api/v1/submissions?status=&submitter=&country=&category=&from=&to=&limit=&offset=&summary=true
GET /api/v1/submissions/:id
```

Filters combine with AND. An unrecognised `status` is a `400` rather than being
ignored — silently returning everything because a client typo'd a filter is the
failure most likely to go unnoticed, because the response is still a
plausible-looking list. An empty feed is `200` with `total: 0`, not `404`.

Ordering is `submitted_at DESC, id DESC`. The `id` tiebreak is what makes offset
pagination consistent: without it, two rows sharing a timestamp can swap places
between pages and a client sees a record twice or misses one. `has_more` is
derived from the returned row count, so the final page reports `false` even when
`total` is an exact multiple of the page size.

`summary=true` returns a per-status breakdown. It deliberately ignores the
`status` filter — a summary that could only ever report the one status that was
filtered to would be useless.

> Offset paging is the right choice for a stable snapshot, and the wrong one for
> a continuously-written feed, where inserts during a walk shift the window. If
> this feed needs to be tailed, add a keyset variant (`?after=<id>`) rather than
> raising the offset cap.

### Migration note

`012_rewards_audit_submissions.sql` follows `010` (PR #773) and `011` (PR #774).
The runner keys applied migrations by **filename prefix alone**, so each number
is claimed by exactly one file. Note that `upstream/main` already has three
`007_*.sql` and three `008_*.sql` files, so by that same rule only the first of
each is ever applied — worth confirming those columns and indexes exist in your
environment.

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
