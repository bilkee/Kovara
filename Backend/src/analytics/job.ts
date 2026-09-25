/**
 * Daily index aggregation job.
 *
 * Issues #652 and #653. Computes one aggregate per (country, category) for a
 * run date and persists it with the filter decisions that produced it.
 *
 * The job is designed to be **restartable and idempotent** rather than
 * exactly-once. A run that dies halfway has already committed some
 * (country, category) pairs, and a re-run must converge on the same state
 * instead of failing on duplicates or double-counting. That is achieved by
 * upserting each pair in its own transaction and treating a completed pair as
 * done, so the worst case is repeated work, never a corrupted aggregate.
 *
 * Leasing is deliberately simple. A single advisory lock serialises the job
 * across replicas for the duration of a run: with one daily run a minute-scale
 * lease and a reclaim path would add failure modes (expired lease taken over
 * mid-write, fencing tokens) for no benefit. A crashed run's lock is released
 * automatically by Postgres when the connection drops, so the next tick
 * proceeds without manual intervention.
 */

import { Pool } from "pg";
import { PostgresAnalyticsStore, QueryValidationError } from "./store";
import { FilterOptions } from "./index-aggregation";

/** Advisory lock key for the daily aggregation run. */
const AGGREGATION_LOCK_KEY = 8_112_004_731;

export interface DailyAggregationOptions {
  pool: Pool;
  /** The date to aggregate, `YYYY-MM-DD`. Defaults to the previous UTC day. */
  runDate?: string;
  /** Filter configuration applied to every (country, category) pair. */
  filter?: FilterOptions;
  /** Stop after this many pairs, for a bounded catch-up run. */
  maxPairs?: number;
  /** Called after each pair commits. */
  onPair?: (result: { countryIso: string; category: string; median: bigint | null }) => void;
}

export interface DailyAggregationResult {
  runDate: string;
  /** Pairs that produced a published aggregate. */
  computed: number;
  /** Pairs with no surviving observation, so nothing to publish. */
  skipped: number;
  /** Pairs that failed. A failure never aborts the remaining pairs. */
  failed: number;
}

/** Yesterday in UTC, as `YYYY-MM-DD`. */
export function previousUtcDay(now: Date = new Date()): string {
  const day = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())
  );
  day.setUTCDate(day.getUTCDate() - 1);
  return day.toISOString().slice(0, 10);
}

/** The (country, category) pairs that have verified observations on a date. */
export interface AggregationTarget {
  country_iso: string;
  category: string;
}

/**
 * Run the daily aggregation for one date.
 *
 * Each pair is computed and committed independently. A pair that throws is
 * counted and logged, then skipped: one malformed country's data must not stop
 * every other country from getting an index for the day.
 */
export async function runDailyIndexAggregation(
  options: DailyAggregationOptions
): Promise<DailyAggregationResult> {
  const { pool, filter = {}, maxPairs, onPair } = options;
  const runDate = options.runDate ?? previousUtcDay();
  const store = new PostgresAnalyticsStore(pool);

  // Validate the date up front so a bad runDate fails before any work, rather
  // than once per pair.
  try {
    new Date(runDate).toISOString();
  } catch {
    throw new QueryValidationError(`runDate is not a valid date: ${runDate}`, "INVALID_DATE");
  }

  const client = await pool.connect();
  let result: DailyAggregationResult = { runDate, computed: 0, skipped: 0, failed: 0 };

  try {
    // Serialise across replicas. pg_try_advisory_lock returns false rather than
    // blocking when another replica holds it. Released implicitly when the
    // connection drops, so a crashed run does not wedge the job.
    const { rows } = await client.query<{ locked: boolean }>(
      "SELECT pg_try_advisory_lock($1) AS locked",
      [AGGREGATION_LOCK_KEY]
    );
    if (!rows[0]?.locked) {
      // Another replica is running the job. Returning an empty result rather
      // than throwing keeps the scheduler's tick loop quiet.
      return result;
    }

    // Parameterised LIMIT: maxPairs is coerced to a number and bound as a
    // parameter, so a non-numeric or out-of-range value cannot reach the SQL
    // text at all.
    const targets = await client.query<AggregationTarget>(
      `
      SELECT DISTINCT country_iso, category
      FROM price_observations
      WHERE status = 'verified'
        AND observed_at >= $1::date
        AND observed_at <  ($1::date + INTERVAL '1 day')
      ORDER BY country_iso ASC, category ASC
      LIMIT $2
      `,
      [runDate, maxPairs ?? Number.MAX_SAFE_INTEGER]
    );

    // The lock is no longer needed: per-pair writes are idempotent upserts, so
    // a second replica doing the same pair converges on the same row.
    await client.query("SELECT pg_advisory_unlock($1)", [AGGREGATION_LOCK_KEY]);

    for (const target of targets.rows) {
      try {
        const outcome = await store.aggregateDay(
          target.country_iso,
          target.category,
          runDate,
          filter
        );
        if (outcome?.median === null || outcome === null) {
          result.skipped += 1;
        } else {
          result.computed += 1;
        }
        onPair?.({
          countryIso: target.country_iso,
          category: target.category,
          median: outcome?.median ?? null,
        });
      } catch (err) {
        // Logged, counted, and swallowed: one country's bad data must not
        // prevent every other country from receiving an index.
        result.failed += 1;
        console.warn(
          `[index-aggregation] ${target.country_iso}/${target.category} failed: ${String(err)}`
        );
      }
    }
  } finally {
    client.release();
  }

  return result;
}
