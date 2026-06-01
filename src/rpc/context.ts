import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';

/** The authenticated caller, derived from the verified token (stubbed until Phase 4). */
export interface ConsumerIdentity {
  id: string;
  scopes: string[];
  claims: Record<string, unknown>;
}

/**
 * Handed to every handler. `db` is the full local replica — the handler body is the
 * access-control boundary: it reads everything but returns only what `consumer` may see.
 */
export interface RpcContext {
  /** Authenticated caller. NEVER trust client-supplied identity fields; use this. */
  consumer: ConsumerIdentity;
  /** The local replica adapter (same surface gossip uses). */
  db: IStorageAdapter;
  /** Current HLC watermark — returned on writes for read-your-writes (Phase 5). */
  hlc(): HLCTimestamp;
  /**
   * Run `fn` at most once per idempotency key. Phase 2 runs it directly; Phase 5 adds the
   * dedupe store so failover replays don't double-apply writes.
   */
  idempotent<T>(key: string | undefined, fn: () => T | Promise<T>): Promise<T>;
  /** Fires on deadline or client cancel; handlers SHOULD abort long work when it aborts. */
  signal: AbortSignal;
  /** The originating request envelope (id, method, idempotencyKey). */
  request: { id: string; method: string; idempotencyKey?: string };
  /** Per-request logger (no-op until Phase 8 metrics). */
  log(message: string, meta?: Record<string, unknown>): void;
}
