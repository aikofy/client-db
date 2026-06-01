/**
 * Cross-cutting RPC middleware: per-consumer rate limiting, payload sizing, and the
 * observability record. Kept small and dependency-free.
 */
import type { MethodKind, RpcStatus } from './protocol.js';

/**
 * Token-bucket rate limiter. Capacity = one minute's worth of calls (the burst), refilled
 * continuously at `perMinute / 60s`. `now` is injectable for tests.
 */
export class TokenBucket {
  private tokens: number;
  private last: number;
  private readonly capacity: number;
  private readonly refillPerMs: number;

  constructor(perMinute: number, now: number) {
    this.capacity = Math.max(1, perMinute);
    this.refillPerMs = this.capacity / 60_000;
    this.tokens = this.capacity;
    this.last = now;
  }

  /** Consume one token. Returns false if the bucket is empty (rate exceeded). */
  tryRemove(now: number): boolean {
    this.tokens = Math.min(this.capacity, this.tokens + (now - this.last) * this.refillPerMs);
    this.last = now;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }
    return false;
  }
}

/** Approximate serialized byte size of a value (for payload caps). 0 for undefined. */
export function byteSize(value: unknown): number {
  if (value === undefined) return 0;
  return new TextEncoder().encode(JSON.stringify(value)).length;
}

/** Emitted once per call (success or failure) to the `onCall` hook — for metrics and audit.
 *  Audit = filter `kind === 'write'`; persist to a (replicated) collection if you want a durable trail. */
export interface CallRecord {
  /** The transport-level consumer id (self-asserted). */
  consumerId: string;
  /** The authenticated identity id (from the token's `sub`). */
  identityId: string;
  method: string;
  kind: MethodKind | 'unknown';
  status: RpcStatus;
  durationMs: number;
}
