/**
 * Ledger event ingestion (issue #646).
 *
 * Turns ledger events from a source into internal domain events, exactly once
 * per event id. The source is an interface so this pipeline can be exercised
 * without a network, and normalization is delegated to `stellar-normalize`
 * (#647) so field mapping has one home.
 *
 * "Reliably" here means three specific things, each of which was previously
 * implicit:
 *
 *  1. **Bounded resume.** A source is asked for a ledger range, and the next
 *     range is derived from what it actually returned — so a source that
 *     returns nothing does not advance the cursor past data it never saw.
 *  2. **Idempotent ingest.** Re-running the same range produces no duplicate
 *     domain events, and the second run reports the duplicates rather than
 *     silently swallowing them.
 *  3. **Failed transactions excluded.** A `status: "failed"` event is dropped
 *     before it can reach a handler, because its effects were rolled back.
 */

import { logger } from "../logger";
import { normalizeStellarEvent } from "./stellar-normalize";
import type { NormalizedStellarEvent, TxStatus } from "./stellar-normalize";

/** An inclusive ledger range. */
export interface LedgerRange {
  startLedger: number;
  endLedger: number;
}

/** A source of raw ledger payloads. */
export interface LedgerEventSource {
  /** Stable name, used in logs and metrics. */
  readonly name: string;
  /**
   * Fetch every raw payload in `range`. Implementations should return an empty
   * array rather than throw when a range has no events.
   */
  fetch(range: LedgerRange): Promise<unknown[]>;
}

/** An event after mapping, ready for the domain handlers. */
export interface DomainEvent {
  /**
   * Idempotency key. Stable for a given on-chain event, so a replay of the same
   * ledger is recognised as a duplicate rather than processed twice.
   */
  eventId: string;
  /** Internal event name, e.g. `post_created`. */
  type: string;
  ledger: number;
  occurredAt: string;
  contractId: string;
  txHash: string;
  status: TxStatus;
  /** Acting account, when the payload identified one. */
  account: string | null;
  /** Remaining decoded topics, base64-decoded. */
  topics: string[];
  /** Decoded payload value. */
  value: string;
  /** Amount in stroops, or null when the payload was not a bare integer. */
  amountStroops: bigint | null;
}

export interface IngestResult {
  /** Events accepted and emitted downstream. */
  accepted: DomainEvent[];
  /** Events skipped because the id was already ingested. */
  duplicates: DomainEvent[];
  /** Events dropped because their transaction failed. */
  rejected: DomainEvent[];
  /** Events the normalizer could not process at all. */
  malformed: number;
  /** Ledger the caller should resume from next. */
  nextLedger: number;
}

/** A source backed by an injected fetch function, for tests and custom RPCs. */
export class InjectedLedgerSource implements LedgerEventSource {
  constructor(
    readonly name: string,
    private readonly fetcher: (range: LedgerRange) => Promise<unknown[]>
  ) {}

  fetch(range: LedgerRange): Promise<unknown[]> {
    return this.fetcher(range);
  }
}

/**
 * Remembers which event ids have been ingested.
 *
 * Bounded because this runs for the life of the process: an unbounded set is a
 * slow memory leak, and an indexer that restarts hourly would rather re-check a
 * few ids than hold every event it has ever seen. Eviction is oldest-first
 * insertion order, so what gets dropped is the least recent history.
 */
export class SeenEventIds {
  private readonly ids = new Set<string>();

  constructor(private readonly capacity = 10_000) {}

  has(id: string): boolean {
    return this.ids.has(id);
  }

  /** Record `id` as seen. Returns false if it was already present. */
  add(id: string): boolean {
    if (this.ids.has(id)) return false;
    this.ids.add(id);
    if (this.ids.size > this.capacity) {
      // Set iteration is insertion order, so the first key is the oldest.
      const oldest = this.ids.values().next();
      if (!oldest.done) this.ids.delete(oldest.value);
    }
    return true;
  }

  get size(): number {
    return this.ids.size;
  }

  clear(): void {
    this.ids.clear();
  }
}

/** Maps a normalized event onto the internal domain shape. */
export function toDomainEvent(event: NormalizedStellarEvent): DomainEvent {
  return {
    eventId: event.eventId,
    type: event.type,
    ledger: event.ledger,
    occurredAt: event.ledgerClosedAt,
    contractId: event.contractId,
    txHash: event.txHash,
    status: event.status,
    account: event.account,
    topics: event.topics,
    value: event.decodedValue,
    amountStroops: event.amountStroops,
  };
}

/**
 * The ingestion pipeline.
 *
 * Construct one per process and reuse it — the seen-id registry is the state
 * that makes ingestion idempotent, and a fresh instance per call would defeat
 * the whole mechanism.
 */
export class LedgerIngestor {
  private readonly seen: SeenEventIds;

  constructor(private readonly source: LedgerEventSource, seenCapacity = 10_000) {
    this.seen = new SeenEventIds(seenCapacity);
  }

  /** Ids already ingested. Exposed for assertions and operator tooling. */
  get seenCount(): number {
    return this.seen.size;
  }

  /**
   * Ingest one ledger range.
   *
   * The cursor advances to `max(lastSeenLedger) + 1`, not to `endLedger + 1`.
   * If a source under-delivers, advancing to the requested end would skip the
   * gap permanently on the next pass; advancing to what was actually observed
   * re-requests the missing range instead of losing it.
   */
  async ingestRange(range: LedgerRange): Promise<IngestResult> {
    const accepted: DomainEvent[] = [];
    const duplicates: DomainEvent[] = [];
    const rejected: DomainEvent[] = [];
    let malformed = 0;
    let highestLedger = range.startLedger - 1;

    let raw: unknown[];
    try {
      raw = await this.source.fetch(range);
    } catch (err) {
      // A source failure must not advance the cursor. Rethrow so the caller
      // (or the retry layer in #649) can decide, with `range` intact.
      logger.error("ingest_source_fetch_failed", { source: this.source.name, range, err });
      throw err;
    }

    for (const payload of raw) {
      let normalized: NormalizedStellarEvent;
      try {
        normalized = normalizeStellarEvent(payload);
      } catch {
        malformed += 1;
        continue;
      }

      if (normalized.ledger > highestLedger) {
        highestLedger = normalized.ledger;
      }

      const domain = toDomainEvent(normalized);

      if (domain.status === "failed") {
        // The effects were rolled back on-chain. Recording them would create
        // state that never existed, so they are dropped before dedup — a
        // replayed failed event should keep being reported as rejected, not
        // start being reported as a duplicate.
        rejected.push(domain);
        continue;
      }

      if (!this.seen.add(domain.eventId)) {
        duplicates.push(domain);
        continue;
      }

      accepted.push(domain);
    }

    const nextLedger = Math.max(highestLedger + 1, range.startLedger);

    logger.info("ingest_range_complete", {
      source: this.source.name,
      range,
      accepted: accepted.length,
      duplicates: duplicates.length,
      rejected: rejected.length,
      malformed,
      nextLedger,
    });

    return { accepted, duplicates, rejected, malformed, nextLedger };
  }
}

/**
 * Split a starting ledger and a target ledger into fixed-size windows.
 *
 * A single request for the whole history is how an indexer runs out of memory
 * on its first real sync, so ranges are walked in bounded chunks.
 */
export function planLedgerWindows(
  startLedger: number,
  endLedger: number,
  windowSize: number
): LedgerRange[] {
  if (!Number.isInteger(windowSize) || windowSize < 1) {
    throw new Error("planLedgerWindows: windowSize must be a positive integer");
  }
  if (!Number.isInteger(startLedger) || !Number.isInteger(endLedger)) {
    throw new Error("planLedgerWindows: ledgers must be integers");
  }
  if (endLedger < startLedger) return [];

  const windows: LedgerRange[] = [];
  for (let start = startLedger; start <= endLedger; start += windowSize) {
    windows.push({ startLedger: start, endLedger: Math.min(start + windowSize - 1, endLedger) });
  }
  return windows;
}
