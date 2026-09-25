import { Router, Request, Response } from "express";
import { Database, PoolRecord } from "../../db";
import { ApiErrorResponse, PoolListResponse, PoolResponse } from "../contracts";
import { serializeBigInt } from "../index";

const MAX_LIMIT = 100;
const DEFAULT_LIMIT = 20;
const DEFAULT_OFFSET = 0;

function isThresholdValid(pool: PoolRecord): boolean {
  return pool.threshold > 0 && pool.threshold <= pool.admins.length;
}

function serializePool(
  pool: PoolRecord,
  meta?: { token_name?: string; token_symbol?: string; token_decimals?: number }
): Record<string, unknown> {
  return serializeBigInt({
    pool_id: pool.pool_id,
    token: pool.token,
    balance: pool.balance,
    admins: pool.admins,
    threshold: pool.threshold,
    created_ledger: pool.created_ledger,
    updated_ledger: pool.updated_ledger,
    ...meta,
  }) as Record<string, unknown>;
}

export function createPoolsRouter(db: Database): Router {
  const router = Router();

  /**
   * GET /pools?limit=<n>&offset=<n>
   * Lists pools with limit/offset pagination and has_more.
   */
  router.get(
    "/",
    async (req: Request, res: Response<PoolListResponse | ApiErrorResponse>): Promise<void> => {
      const rawLimit = req.query.limit !== undefined ? Number(req.query.limit) : DEFAULT_LIMIT;
      const rawOffset = req.query.offset !== undefined ? Number(req.query.offset) : DEFAULT_OFFSET;

      if (!Number.isInteger(rawLimit) || rawLimit < 1) {
        res.status(400).json({ error: "limit must be a positive integer", code: "INVALID_QUERY" });
        return;
      }
      if (rawLimit > MAX_LIMIT) {
        res.status(400).json({ error: `limit cannot exceed ${MAX_LIMIT}`, code: "LIMIT_EXCEEDED" });
        return;
      }
      if (!Number.isInteger(rawOffset) || rawOffset < 0) {
        res.status(400).json({ error: "offset must be a non-negative integer", code: "INVALID_QUERY" });
        return;
      }

      const { pools, total } = await db.listPools({ limit: rawLimit, offset: rawOffset });

      const enriched = await Promise.all(
        pools.map(async (pool) => {
          let token_name: string | undefined;
          let token_symbol: string | undefined;
          let token_decimals: number | undefined;
          try {
            const meta = await db.getTokenMetadata(pool.token);
            if (meta) {
              token_name = meta.name;
              token_symbol = meta.symbol;
              token_decimals = meta.decimals;
            }
          } catch {
            token_name = "unknown";
            token_symbol = "UNK";
            token_decimals = 7;
          }
          return serializePool(pool, { token_name, token_symbol, token_decimals });
        })
      );

      res.json({
        pools: enriched,
        total,
        limit: rawLimit,
        offset: rawOffset,
        has_more: rawOffset + pools.length < total,
      } as unknown as PoolListResponse);
    }
  );

  /**
   * GET /pools/:id
   * Returns the current state of a pool by its ID.
   */
  router.get(
    "/:id",
    async (req: Request, res: Response<PoolResponse | ApiErrorResponse>): Promise<void> => {
      const { id } = req.params;

      if (!id || typeof id !== "string" || id.trim() === "") {
        res.status(400).json({ error: "Invalid pool ID: must be a non-empty string", code: "INVALID_ID" });
        return;
      }

      const pool = await db.getPool(id);
      if (!pool) {
        res.status(404).json({ error: "Pool not found", code: "NOT_FOUND" });
        return;
      }

      if (!isThresholdValid(pool)) {
        res.status(422).json({ error: "Pool threshold is invalid", code: "INVALID_THRESHOLD" });
        return;
      }

      let token_name: string | undefined;
      let token_symbol: string | undefined;
      let token_decimals: number | undefined;
      try {
        const meta = await db.getTokenMetadata(pool.token);
        if (meta) {
          token_name = meta.name;
          token_symbol = meta.symbol;
          token_decimals = meta.decimals;
        }
      } catch {
        token_name = "unknown";
        token_symbol = "UNK";
        token_decimals = 7;
      }

      res.json(
        serializeBigInt({
          ...pool,
          token_name,
          token_symbol,
          token_decimals,
        })
      );
    }
  );

  return router;
}
