/**
 * Audit persistence and forensic queries.
 *
 * Issue #658. The chain itself lives in `chain.ts`; this module is the durable
 * side of it.
 *
 * Appending is serialised per stream with a row lock on the chain head. The
 * alternative — read the head, compute the hash, insert — has a race: two
 * concurrent appends for the same stream read the same head and produce two
 * records claiming the same `previousHash`, which silently forks the chain and
 * makes `verifyChain` report a break that nobody caused. Locking the head row
 * makes the read-hash-insert sequence atomic.
 */

import { Pool, PoolClient } from "pg";
import {
  AuditAction,
  AuditActor,
  AuditEntry,
  AuditEntryInput,
  AuditOutcome,
  ChainVerification,
  GENESIS_HASH,
  actorKeyOf,
  chainEntry,
  verifyChain,
} from "./chain";

/** Filters accepted by {@link AuditStore.listEntries}. */
export interface AuditQuery {
  stream?: string;
  action?: AuditAction;
  outcome?: AuditOutcome;
  /** Match entries whose subject is this contract, entity, or address. */
  subject?: string;
  /** Match entries performed by this address. */
  actor?: string;
  ledger?: number;
  from?: Date;
  to?: Date;
  limit: number;
  offset: number;
}

/** A filtered, paginated page of audit entries. */
export interface AuditPage {
  entries: AuditEntry[];
  total: number;
  limit: number;
  offset: number;
  hasMore: boolean;
}

export class AuditStore {
  constructor(private readonly pool: Pool) {}

  /**
   * Append one entry, chained to the current head of its stream.
   *
   * `FOR UPDATE` on the head row serialises appends within a stream, so the
   * chain cannot fork under concurrency. Streams are independent, so a busy
   * stream never blocks another.
   */
  async append(input: AuditEntryInput, id: string): Promise<AuditEntry> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const previousHash = await this.lockHead(client, input.stream);
      const entry = chainEntry(input, previousHash, id);
      await this.insert(client, entry);
      await client.query("COMMIT");
      return entry;
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Append many entries in one transaction, chained in order.
   *
   * A batch is all-or-nothing on purpose: a partially written audit trail for a
   * single logical action is worse than none, because it implies a record was
   * lost and leaves the chain describing a sequence that never happened.
   */
  async appendBatch(inputs: AuditEntryInput[], idFor: (index: number) => string): Promise<AuditEntry[]> {
    if (inputs.length === 0) return [];
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      // Lock every affected stream in a deterministic order (sorted) so two
      // batches touching the same streams in different orders cannot deadlock.
      const streams = [...new Set(inputs.map((i) => i.stream))].sort();
      const heads = new Map<string, string>();
      for (const stream of streams) {
        heads.set(stream, await this.lockHead(client, stream));
      }

      const written: AuditEntry[] = [];
      for (const [index, input] of inputs.entries()) {
        const previous = heads.get(input.stream) ?? GENESIS_HASH;
        const entry = chainEntry(input, previous, idFor(index));
        await this.insert(client, entry);
        heads.set(input.stream, entry.hash);
        written.push(entry);
      }

      await client.query("COMMIT");
      return written;
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Read the current chain head for a stream, taking a row lock on it.
   *
   * The row is inserted on demand so the lock has something to attach to. A
   * concurrent first-append for the same stream blocks here and then sees the
   * row the other transaction committed.
   */
  private async lockHead(client: PoolClient, stream: string): Promise<string> {
    // Insert the head row on demand so the lock has something to attach to.
    // DO NOTHING on conflict: this statement does not lock the existing row,
    // and is only establishing that the row exists.
    await client.query(
      `
      INSERT INTO audit_chain_heads (stream, hash, updated_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (stream) DO NOTHING
      `,
      [stream, GENESIS_HASH]
    );
    // The row lock is taken here. Two concurrent appends for the same stream
    // serialise on this read, so the second sees the hash the first committed
    // and chains onto it instead of forking.
    const locked = await client.query<{ hash: string }>(
      "SELECT hash FROM audit_chain_heads WHERE stream = $1 FOR UPDATE",
      [stream]
    );
    return locked.rows[0]?.hash ?? GENESIS_HASH;
  }

  private async insert(client: PoolClient, entry: AuditEntry): Promise<void> {
    await client.query(
      `
      INSERT INTO audit_log
        (id, stream, action, actor, outcome, subject, ledger, transaction_hash,
         metadata, occurred_at, hash, previous_hash)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12)
      ON CONFLICT (id) DO NOTHING
      `,
      [
        entry.id,
        entry.stream,
        entry.action,
        actorKeyOf(entry.actor),
        entry.outcome,
        entry.subject,
        entry.ledger ?? null,
        entry.transactionHash ?? null,
        JSON.stringify(entry.metadata ?? {}),
        entry.occurredAt,
        entry.hash,
        entry.previousHash,
      ]
    );
    // Keep the head in step with the insert. The row is already locked by
    // lockHead, so this update cannot race another append in the same stream.
    await client.query(
      "UPDATE audit_chain_heads SET hash = $2, updated_at = NOW() WHERE stream = $1",
      [entry.stream, entry.hash]
    );
  }

  /**
   * Filtered, paginated audit query.
   *
   * Filters are bound as parameters and assembled as an AND list, so no caller
   * value can reach the SQL text. Ordering is total — `occurred_at DESC` then
   * `id DESC` — so pagination cannot skip or repeat a record when two entries
   * share a timestamp, which in a burst of contract events is the normal case
   * rather than the exception.
   */
  async listEntries(query: AuditQuery): Promise<AuditPage> {
    const conditions: string[] = [];
    const params: unknown[] = [];

    const add = (clause: (index: number) => string, value: unknown): void => {
      params.push(value);
      conditions.push(clause(params.length));
    };

    if (query.stream) add((i) => `stream = $${i}`, query.stream);
    if (query.action) add((i) => `action = $${i}`, query.action);
    if (query.outcome) add((i) => `outcome = $${i}`, query.outcome);
    if (query.subject) add((i) => `subject = $${i}`, query.subject);
    if (query.actor) add((i) => `actor = $${i}`, actorKeyOf({ kind: "address", address: query.actor }));
    if (query.ledger !== undefined) add((i) => `ledger = $${i}`, query.ledger);
    if (query.from) add((i) => `occurred_at >= $${i}`, query.from);
    if (query.to) add((i) => `occurred_at <= $${i}`, query.to);

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const countResult = await this.pool.query(
      `SELECT COUNT(*)::int AS total FROM audit_log ${where}`,
      params
    );
    const result = await this.pool.query<Record<string, unknown>>(
      `
      SELECT * FROM audit_log ${where}
      ORDER BY occurred_at DESC, id DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
      `,
      [...params, query.limit, query.offset]
    );

    const total = Number(countResult.rows[0]?.total ?? 0);
    return {
      entries: result.rows.map((row) => this.mapEntry(row)),
      total,
      limit: query.limit,
      offset: query.offset,
      hasMore: query.offset + result.rows.length < total,
    };
  }

  /** Read a whole stream in chain order, for verification. */
  async getStream(stream: string): Promise<AuditEntry[]> {
    const result = await this.pool.query<Record<string, unknown>>(
      `
      SELECT * FROM audit_log
      WHERE stream = $1
      ORDER BY occurred_at ASC, id ASC
      `,
      [stream]
    );
    return result.rows.map((row) => this.mapEntry(row));
  }

  /**
   * Recompute a stream's chain and report whether it is intact.
   *
   * A full read, because a chain cannot be verified from a prefix: proving the
   * history is intact means checking all of it. Callers should run this on a
   * schedule or on demand, not per request.
   */
  async verify(stream: string): Promise<ChainVerification> {
    return verifyChain(await this.getStream(stream));
  }

  private mapEntry(row: Record<string, unknown>): AuditEntry {
    const actor = String(row.actor);
    const transactionHash = row.transaction_hash ? String(row.transaction_hash) : undefined;
    return {
      id: String(row.id),
      stream: String(row.stream),
      action: String(row.action) as AuditAction,
      actor: parseActorKey(actor),
      outcome: String(row.outcome) as AuditOutcome,
      subject: String(row.subject),
      ledger:
        row.ledger === null || row.ledger === undefined ? undefined : Number(row.ledger),
      ...(transactionHash ? { transactionHash } : {}),
      metadata: (row.metadata ?? {}) as Record<string, unknown>,
      occurredAt: new Date(row.occurred_at as string),
      hash: String(row.hash),
      previousHash: String(row.previous_hash),
    };
  }
}

/** Parse the stored actor key back into a structured actor. */
function parseActorKey(key: string): AuditActor {
  if (key.startsWith("address:")) return { kind: "address", address: key.slice("address:".length) };
  if (key.startsWith("system:")) return { kind: "system", component: key.slice("system:".length) };
  return { kind: "unknown" };
}
