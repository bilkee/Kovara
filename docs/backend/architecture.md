# Kovara backend — architecture

This page describes the **Kovara indexer** (`Backend/`), the Node/TypeScript service
that indexes Kovara Social contract events into PostgreSQL and serves a read-only
REST API over the result.

> **Source of truth.** Everything here was read from the code on `main`
> (verified at commit `2c42735c443e`). Where this page and a comment disagree, the
> code wins. The "Known gaps" section lists behaviour that is visible in the code
> but easy to assume otherwise.

> **Note on the root `README.md`.** The repository root README still describes an
> older `packages/sentinel` / `packages/api` / `packages/atlas` layout that does
> not exist in this tree. It is not a description of this backend. Use
> `Backend/README.md` and this directory for the backend.

## 1. Where the service sits

| Path | What it is |
| --- | --- |
| `Backend/` | The indexer + REST API (this document). Node 18+, TypeScript, Express 4, `pg`. |
| `Contract/`, `packages/contracts/` | Soroban smart contracts (Rust). The indexer consumes their events. |
| `apps/web/` | Next.js web client. |
| `apps/mobile/` | Expo/React Native client. |
| `packages/sdk/` | TypeScript SDK (`packages/sdk/src/client.ts`). |

The backend is a **single process** with two responsibilities:

1. stream Soroban contract events into PostgreSQL, and
2. serve a read-only REST API over the indexed data.

Runtime dependencies are deliberately small — `express`, `cors`,
`express-rate-limit`, `express-async-errors`, `pg` (`Backend/package.json`). There
is no ORM; SQL lives in `Backend/src/db.ts` and the `handlers/` modules.

## 2. Startup sequence

Entry point: `Backend/src/index.ts` (`main()`).

1. **Configuration** — `loadStartupConfig()` calls `loadConfig()`
   (`Backend/src/config.ts`) at module scope, before the pool is created.
   `DATABASE_URL` (must be `postgres://`/`postgresql://`) and `CONTRACT_ID` (a
   56-character `C…` StrKey, CRC16-checked) are required. `STELLAR_RPC_URL` and
   `START_LEDGER` are optional but validated when set. A `ConfigError` logs
   `config_invalid` and exits `1` with every problem listed together.
2. `configureAlerting()` wires the alert sinks (inert unless a sink is configured).
3. **Mode selection** — if both `REPLAY_START_LEDGER` and `REPLAY_END_LEDGER` are
   set, the process runs a replay of that inclusive ledger range and exits;
   otherwise it runs live streaming.
4. **Database init** — `runMigrations(pgPool)` →
   `ensureEventsTable()` → `ensurePostSearchIndex()`.
5. **HTTP server** — `createApp(db, { authMiddleware })` and `app.listen(HOST, PORT)`.
   The API is available immediately, even in replay mode.
6. **Streaming** — live mode calls `streamEvents(...)`; each event is dispatched
   through `processEvent(event, handleEvent)`.
7. **Shutdown** — `SIGTERM`/`SIGINT` abort the stream, close the HTTP server, and
   end the pool (`shuttingDown` guard prevents double cleanup).

## 3. Persistence

PostgreSQL accessed through a `pg.Pool` sized by `DB_POOL_*` (`Backend/src/config.ts`).
Migrations live in `Backend/migrations/*.sql` and are applied by
`runMigrations()` (`Backend/src/migrate.ts`):

- an `schema_version` table records applied versions;
- `pg_advisory_lock(7357192468)` serialises concurrent migrators;
- each file runs in its own transaction; a failing file is rolled back, logged as
  possible schema drift, and **skipped** rather than aborting startup.

Core tables (created by migrations and `ensureEventsTable`): `profiles`, `posts`,
`follows`, `tips`, `likes`, `pools`, `events`, `stream_state`, `schema_version`.

### Event processing state machine

`events` carries an explicit processing status so a persisted event is never
"present but unaccounted for":

| Status | Meaning |
| --- | --- |
| `new` | Persisted (`INSERT … ON CONFLICT (event_id) DO NOTHING`), side effects not applied. |
| `processing` | Claimed atomically by `claimEvent()` before dispatch, so concurrent/restarted workers cannot double-apply side effects. |
| `processed` | Handler succeeded (`markEventProcessed`). |
| `failed` | Handler raised (`markEventFailed`); error text and timestamps are retained for retry. |

Dead-letter bookkeeping: `attempts`, `error`, `processed_at`, `failed_at`,
`dead_lettered_at`. `EventStore` (`Backend/src/event-store.ts`) exposes
`saveCursor`/`loadCursor` (the `stream_state` key/value table),
`markProcessed`, `markFailed`, `deadLetter`, `listFailedEvents`, `requeueEvent`
and `retryFailedEvents`.

`recoverPendingEvents()` (in `index.ts`) replays every row whose status is not
`processed`, which is what makes restart-based recovery safe.

## 4. Event pipeline

```
Soroban RPC getEvents          (stream.ts)
  → validateEventsResult()     shape-check the provider response
  → normalizeRawEvent()        trim/validate fields (normalize.ts)
  → in-memory dedup ring       event.id, bounded cache (dedupCacheSize)
  → contract-ownership check   event.contractId === CONTRACT_ID
  → eventTypeFilter            optional FILTER_EVENTS allowlist
  → processEvent()             persist → claim → handler → mark processed/failed
  → handleEvent()              typed dispatch
```

The dispatch table in `handleEvent()` (`Backend/src/index.ts`) is keyed on
`event.topic[0]`:

| Topic key | Handler |
| --- | --- |
| `profile_set` | `handlers/profile.ts` → `handleProfileSet` |
| `post_created` | `handlers/post.ts` → `handlePostCreated` (receives `{ pgPool }`) |
| `post_deleted` | `handlers/post.ts` → `handlePostDeleted` |
| `like` | `handlers/like.ts` → `handleLike` |
| `follow` / `unfollow` | `handlers/follow.ts` → `handleFollow` / `handleUnfollow` |
| `tip` | `handlers/tip.ts` → `handleTip` |
| `pool_created` / `pool_deposit` / `pool_withdraw` | `handlers/pool.ts` |
| anything else | `logger.warn("unknown_event_type", …)` |

Handlers are written to be idempotent (upserts and unique constraints), so replay
does not duplicate data.

The on-chain event schema the indexer consumes is documented in
[`packages/contracts/contracts/linkora-contracts/EVENTS.md`](../../packages/contracts/contracts/linkora-contracts/EVENTS.md).

## 5. API layer

`createApp(db, options)` (`Backend/src/api/index.ts`) builds the Express app.
Middleware order:

1. `cors()` — currently permissive, no configured origin (see Known gaps).
2. `express.json()`.
3. Request timeout (`REQUEST_TIMEOUT_MS`, default 30s) → `503 REQUEST_TIMEOUT`.
4. Correlation ID middleware — reads `X-Correlation-Id`, generates one when
   missing, stores it on `req.correlationId`.
5. `GET /health` and `GET /version` (unversioned, no auth).
6. Rate limiter on the `/api` prefix when `ENABLE_RATE_LIMITING !== "false"`.
7. Routers mounted on an API router:
   - `/profiles`, `/posts`, `/follows`,
   - `POST /search/posts`, `GET /debug/snapshot`,
   - `/pools` only when `EXPERIMENTAL_FEATURES === "true"`.
8. API router mounted at `/api/v1` (canonical) and `/api` (legacy). Legacy
   responses get `Deprecation: true` and a `Link: </api/v1/…>; rel="successor-version"` header.
9. 404 catch-all, then the global error handler.

Request/response shapes and error codes are in
[`api-contracts.md`](./api-contracts.md). Shared response helpers live in
`Backend/src/api/response.ts`; shared TypeScript response types live in
`Backend/src/api/contracts.ts`.

## 6. Observability

- **Logging** (`Backend/src/logger.ts`) — `StructuredLogger` writes one JSON line
  per event (`ts`, `level`, `logger`, `msg`, plus bound fields). It deduplicates
  identical `level:message` pairs within a 60s window, redacts Stellar addresses
  and long opaque payloads, and records error/warn counts accessible via
  `getErrorMetrics()`.
- **Alerting** (`Backend/src/alerting.ts`) — `AlertManager` delivers to a
  `SentrySink` (built from `SENTRY_DSN`) and/or a `WebhookSink`
  (`ALERT_WEBHOOK_URL`). It applies a severity threshold, an ignore list, a
  per-fingerprint dedup window, a per-window delivery budget, and a sampling
  rate. `installLoggerAlerting()` routes `logger.error` into it. Alerting is
  completely inert until a sink is configured.
- **Endpoints** — `GET /health` returns `{ status, uptime, db }` where `db` is
  `ok` or `unavailable`; `GET /version` returns
  `{ version, git_commit, build_time, node_version }`.

## 7. Known gaps (verified)

These are real, observable in the code, and worth knowing before operating the service:

- **`Backend/src/stream.ts` does not parse.** The file is missing a closing brace
  (a stray `while (!signal.aborted) {` at the top of the stream loop), so
  `tsc`/`ts-jest` cannot compile it. The live streaming and replay paths cannot be
  built as committed.
- **Migration version collisions.** `runMigrations` keys a migration by the text
  before the first `_`, so files sharing a numeric prefix collapse. In the current
  tree `007_*` has five files and `008_*` has three; only the first per prefix (in
  sort order) is applied.
- **Auth middleware is not applied.** `index.ts` builds a Bearer/`API_SECRET`
  guard when `ENABLE_AUTH_MIDDLEWARE=true` and passes it via `createApp`'s
  `options`, but `createApp` resolves it into a variable and never calls
  `app.use(...)`. Requests are not authenticated.
- **Rate-limit env vars are not wired.** The limiter reads the module-level
  `rateLimitWindowMs`/`rateLimitMax` (defaults 60000/100, changeable only via the
  exported `setRateLimit()`, which tests call). `RATE_LIMIT_WINDOW_MS` /
  `RATE_LIMIT_MAX` are parsed but unused. `addressRateLimiter` is imported but not
  mounted.
- **`CORS_ORIGIN` is not read.** `app.use(cors())` is called with no options, so
  all origins are allowed regardless of the environment.
- **Unmounted routers.** `routes/regions.ts`, `routes/categories.ts` and
  `routes/verification-votes.ts` exist but are not mounted in `createApp`.
- **Experimental flag mismatch.** `index.ts` parses `ENABLE_EXPERIMENTAL_ROUTES`
  but never uses it; the pools router is actually gated on `EXPERIMENTAL_FEATURES`
  (which is what `Backend/README.md` and `.env.example` document).
- **Event topic convention.** `handleEvent` switches on `topic[0]`, while
  `EVENTS.md` defines topics as `(ContractName, EventName, Version)` with
  `topic[0] == "Kovara"`. Confirm which contract/ABI is deployed before relying on
  the dispatch keys.
- **Response envelope is inconsistent.** `api/response.ts` defines a
  `{ success, data, timestamp }` envelope, but most routes return bare resource
  objects and route-level errors return `{ error, code }` without `success`/
  `timestamp`. Only the 404 catch-all and the global error handler use the shared
  helpers.
