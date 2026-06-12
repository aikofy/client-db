/**
 * Run-once cache backing `ctx.idempotent(key, fn)`. Dedupes both in-flight and recently-completed
 * calls for the same key, so a Consumer that retries a write (e.g. after a dropped channel) does
 * not apply it twice. A failed call is NOT cached — it can be retried.
 *
 * Scope: in-memory, per Normal Client. This dedupes retries against the SAME node. Cross-node
 * dedupe under failover (a replicated dedupe store) is a follow-up (see docs/rpc-protocol.md §6);
 * the cache is injectable so it can be swapped later.
 */
const DEFAULT_TTL_MS = 10 * 60 * 1000; // 10 minutes
const PRUNE_INTERVAL_MS = 60 * 1000; // sweep expired entries at most once a minute

interface Entry {
  promise: Promise<unknown>;
  /** Time after which the entry is discarded. Refreshed when the call settles. */
  expiresAt: number;
}

export interface IdempotencyCacheOptions {
  ttlMs?: number;
  /** Injectable clock for tests. */
  now?: () => number;
}

export class IdempotencyCache {
  private readonly map = new Map<string, Entry>();
  private readonly ttlMs: number;
  private readonly now: () => number;
  private lastPruneAt = 0;

  constructor(opts: IdempotencyCacheOptions = {}) {
    this.ttlMs = opts.ttlMs ?? DEFAULT_TTL_MS;
    this.now = opts.now ?? (() => Date.now());
  }

  run<T>(key: string, fn: () => T | Promise<T>): Promise<T> {
    // Throttled sweep: pruning is O(entries) and purely GC — the lookup below
    // already ignores expired entries, so correctness never depends on it. An
    // every-call sweep would make a busy cache quadratic.
    const t = this.now();
    if (t - this.lastPruneAt >= PRUNE_INTERVAL_MS) {
      this.lastPruneAt = t;
      this._prune();
    }

    const existing = this.map.get(key);
    if (existing && existing.expiresAt > this.now()) {
      return existing.promise as Promise<T>;
    }

    const promise = (async () => fn())();
    const entry: Entry = { promise, expiresAt: this.now() + this.ttlMs };
    this.map.set(key, entry);

    promise.then(
      () => {
        // Keep the result for ttlMs AFTER completion.
        entry.expiresAt = this.now() + this.ttlMs;
      },
      () => {
        // Don't cache failures — allow a retry.
        if (this.map.get(key) === entry) this.map.delete(key);
      },
    );

    return promise as Promise<T>;
  }

  private _prune(): void {
    const t = this.now();
    for (const [k, e] of this.map) {
      if (e.expiresAt <= t) this.map.delete(k);
    }
  }
}
