/**
 * Dead-letter queue for unrecoverable event failures.
 *
 * Issue #650. An event that fails for a reason no retry will fix — a malformed
 * payload, a constraint the data permanently violates — must not be dropped on
 * the floor, and must not be retried forever either. The stream needs a place
 * to put it, enough context to debug it, and a durable record that outlives the
 * process.
 *
 * The distinction this module makes is *unrecoverable* versus *not yet
 * recovered*. A failure is not dead because it happened to fail once; it is
 * dead because the retry policy in `retry.ts` gave up. The queue therefore
 * counts attempts against a configured ceiling and only then dead-letters, so
 * a transient blip that survived three attempts lands in the queue only because
 * the system genuinely could not make progress.
 *
 * A dead letter keeps everything needed to reproduce and fix the failure:
 * the raw payload, the topic, the transaction and ledger it came from, the
 * error, and the attempt count. It deliberately keeps the raw `value` even
 * though the logger redacts payload bodies — a redacted queue is useless for
 * debugging, and the queue is an operator-facing table, not a log stream.
 *
 * The queue is intentionally append-only. Requeueing moves an event back into
 * the live stream rather than editing history, so an operator can always
 * reconstruct what failed and what was done about it.
 */

/** Why an event ended up in the dead-letter queue. */
export type DeadLetterReason =
  /** Retries were exhausted and the failure was still transient. */
  | "retries_exhausted"
  /** The failure was classified permanent on the first attempt. */
  | "permanent_failure"
  /** The event failed an intrinsic validation and must never be dispatched. */
  | "invalid_payload"
  /** The event belonged to a contract this indexer is not indexing. */
  | "wrong_contract";

/** A single dead-lettered event, as stored and as returned to operators. */
export interface DeadLetterRecord {
  /** Stable identity of the event, from `buildIdempotencyKey`. */
  idempotencyKey: string;
  /** The provider's event id. */
  eventId: string;
  contractId: string;
  txHash: string;
  ledger: number;
  /** Event type, i.e. `topic[0]`. Empty when the payload had no usable topic. */
  eventType: string;
  topic: string[];
  /** The raw, unredacted event value. */
  value: string;
  error: string;
  reason: DeadLetterReason;
  attempts: number;
  deadLetteredAt: Date;
  /** Set when an operator requeued the event; null while it is still dead. */
  requeuedAt: Date | null;
}

/** A `DeadLetterRecord` as it arrives from the database (camelCase, dates parsed). */
export interface DeadLetterRow {
  idempotencyKey: string;
  eventId: string;
  contractId: string;
  txHash: string;
  ledger: number;
  eventType: string;
  topic: string[];
  value: string;
  error: string;
  reason: DeadLetterReason;
  attempts: number;
  deadLetteredAt: Date;
  requeuedAt: Date | null;
}

/** Default number of attempts after which a failure is dead-lettered. */
export const DEFAULT_MAX_ATTEMPTS = 3;

/** The persistence surface the queue needs. */
export interface DeadLetterStore {
  /** Insert (or update) the dead-letter record for an event. */
  record(entry: Omit<DeadLetterRecord, "deadLetteredAt" | "requeuedAt">): Promise<void>;
  /** List dead letters, newest first, for operator inspection. */
  list(limit: number, offset: number): Promise<{ entries: DeadLetterRecord[]; total: number }>;
  /** Move a dead letter back into the live stream. Returns false if unknown. */
  requeue(idempotencyKey: string): Promise<boolean>;
  /** Count of dead letters that have not been requeued. */
  countPending(): Promise<number>;
}

/** Input for {@link DeadLetterQueue.fail}. */
export interface FailureContext {
  eventId: string;
  contractId: string;
  txHash: string;
  ledger: number;
  topic: string[];
  value: string;
  error: string;
}

/**
 * Tracks attempts per event and dead-letters once the ceiling is reached.
 *
 * The attempt counter is per-key and in-process, which is the right scope: a
 * restart should give a previously failing event a fresh budget, because the
 * thing that made it fail (a bad deploy, a full disk) may have been fixed
 * meanwhile. The durable record of previous failures lives in the store.
 */
export class DeadLetterQueue {
  private readonly attemptsByKey = new Map<string, number>();

  constructor(
    private readonly store: DeadLetterStore,
    private readonly maxAttempts: number = DEFAULT_MAX_ATTEMPTS
  ) {}

  /**
   * How many times this key has already failed, including the current one.
   */
  attemptsFor(idempotencyKey: string): number {
    return this.attemptsByKey.get(idempotencyKey) ?? 0;
  }

  /** Forget the attempt counter for a key (called once processing succeeds). */
  reset(idempotencyKey: string): void {
    this.attemptsByKey.delete(idempotencyKey);
  }

  /**
   * Whether `attempts` has reached the dead-letter ceiling.
   *
   * Exposed separately from {@link fail} so a caller can decide to stop retrying
   * before performing another attempt, rather than only after it fails.
   */
  shouldDeadLetter(attempts: number): boolean {
    return attempts >= this.maxAttempts;
  }

  /**
   * Record a failure, dead-lettering it if the ceiling has been reached.
   *
   * Returns the reason when the event was dead-lettered, or null when it is
   * still within its retry budget. A `permanent_failure` reason is
   * dead-lettered immediately: retrying it cannot help.
   */
  async fail(
    idempotencyKey: string,
    context: FailureContext,
    reason: DeadLetterReason = "retries_exhausted"
  ): Promise<DeadLetterReason | null> {
    const attempts = this.attemptsByKey.get(idempotencyKey) ?? 0;
    const nextAttempts = attempts + 1;
    this.attemptsByKey.set(idempotencyKey, nextAttempts);

    // A permanent failure is dead on arrival; only repeated transient failures
    // need to exhaust the budget.
    const isPermanent = reason === "permanent_failure" || reason === "invalid_payload" ||
      reason === "wrong_contract";
    if (!isPermanent && nextAttempts < this.maxAttempts) {
      return null;
    }

    await this.store.record({
      idempotencyKey,
      eventId: context.eventId,
      contractId: context.contractId,
      txHash: context.txHash,
      ledger: context.ledger,
      eventType: context.topic[0] ?? "",
      topic: context.topic,
      value: context.value,
      error: context.error,
      reason,
      attempts: nextAttempts,
    });

    return reason;
  }

  /** List dead letters for operator inspection. */
  list(limit = 100, offset = 0): Promise<{ entries: DeadLetterRecord[]; total: number }> {
    return this.store.list(limit, offset);
  }

  /** Requeue a dead letter so it is processed again. */
  requeue(idempotencyKey: string): Promise<boolean> {
    // Clear the in-process budget: the operator is explicitly asking for a
    // fresh set of attempts, not a continuation of the exhausted one.
    this.attemptsByKey.delete(idempotencyKey);
    return this.store.requeue(idempotencyKey);
  }

  /** Count of dead letters awaiting operator attention. */
  countPending(): Promise<number> {
    return this.store.countPending();
  }
}
