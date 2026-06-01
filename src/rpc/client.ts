import { RpcError } from './errors.js';
import type { HLCTimestamp } from '../core/types.js';
import {
  PROTOCOL_VERSION,
  type AuthOkFrame,
  type ClientFrame,
  type ReqFrame,
  type ServerFrame,
} from './protocol.js';

const DEFAULT_DEADLINE_MS = 30_000;
/** Client waits a little past the request deadline before giving up locally, so the server's
 *  own DEADLINE_EXCEEDED normally wins the race; the client timer only fires if nothing comes. */
const CLIENT_DEADLINE_GRACE_MS = 1_000;

export interface RpcClientConfig {
  /** Send a client frame over the open channel. */
  send: (frame: ClientFrame) => void;
  /** Returns the current access token (e.g. from the IdP). May be async. */
  getToken: () => string | Promise<string>;
  protocolVersion?: number;
  defaultDeadlineMs?: number;
  /** Override correlation-id generation (tests). Default: crypto.randomUUID. */
  generateId?: () => string;
}

export interface InvokeOptions {
  idempotencyKey?: string;
  deadlineMs?: number;
  signal?: AbortSignal;
  /**
   * Whether the call may be replayed on failover. Defaults: reads `true`, writes `false`
   * (replaying a write across nodes can duplicate until the dedupe store is replicated). Set
   * `true` to opt a write into failover-replay (relies on its idempotency key).
   */
  idempotent?: boolean;
  /** Read-your-writes watermark — the server waits until its replica reaches this HLC. Auto-set
   *  to the last write watermark for reads; pass explicitly to override. */
  readAfter?: HLCTimestamp;
}

interface Pending {
  resolve: (value: unknown) => void;
  reject: (err: unknown) => void;
  timer: ReturnType<typeof setTimeout> | null;
  signal?: AbortSignal;
  onAbort?: () => void;
}

/**
 * Transport-agnostic RPC caller. Feed it incoming frames via `handleMessage` and give it a
 * `send`; it manages the auth handshake and unary `invoke` (correlation ids, deadlines, cancel).
 * No WebRTC/storage dependencies — the ConsumerClient owns the channel.
 */
export class RpcClient {
  private readonly cfg: RpcClientConfig;
  private readonly pending = new Map<string, Pending>();
  private readonly streams = new Map<string, StreamCall<unknown>>();
  private authReady: Promise<AuthOkFrame> | null = null;
  private authResolve: ((ok: AuthOkFrame) => void) | null = null;
  private authReject: ((e: unknown) => void) | null = null;
  /** The server's auth-ok (method catalog, limits) once authenticated. */
  serverInfo: AuthOkFrame | null = null;
  /** Latest HLC watermark seen on a write response (for read-your-writes; used in Phase 7). */
  lastHlc: HLCTimestamp | null = null;

  constructor(cfg: RpcClientConfig) {
    this.cfg = cfg;
  }

  /** Start (or return the in-flight) auth handshake. Call once the channel is open. */
  authenticate(): Promise<AuthOkFrame> {
    if (this.authReady) return this.authReady;
    this.authReady = new Promise<AuthOkFrame>((resolve, reject) => {
      this.authResolve = resolve;
      this.authReject = reject;
    });
    void Promise.resolve(this.cfg.getToken())
      .then((token) => {
        this.cfg.send({
          type: 'auth',
          token,
          protocolVersion: this.cfg.protocolVersion ?? PROTOCOL_VERSION,
        });
      })
      .catch((e) => this.authReject?.(e));
    return this.authReady;
  }

  async invoke<R = unknown>(method: string, params?: unknown, opts?: InvokeOptions): Promise<R> {
    await this.authenticate();
    this._assertMethodSupported(method);
    // Auto-attach a STABLE idempotency key for write methods so a retry (e.g. after the channel
    // dropped, or a failover in Phase 7) is deduped server-side rather than applied twice. The
    // key is fixed once here, so it survives the re-auth retry below.
    let idempotencyKey = opts?.idempotencyKey;
    if (idempotencyKey === undefined && this._isWriteMethod(method)) {
      idempotencyKey = (this.cfg.generateId ?? genId)();
    }
    // Read-your-writes: attach the last write watermark to reads so a (failed-over) replica waits
    // until it has caught up. Harmless on the same node (already at/above its own watermark).
    let readAfter = opts?.readAfter;
    if (readAfter === undefined && !this._isWriteMethod(method) && this.lastHlc) {
      readAfter = this.lastHlc;
    }
    const callOpts: InvokeOptions = { ...opts, idempotencyKey, readAfter };

    let attempt = 0;
    for (;;) {
      await this.authenticate();
      try {
        return await this._invokeOnce<R>(method, params, callOpts);
      } catch (e) {
        // The token expired mid-session: drop the stale session, re-fetch a fresh token, retry once.
        if (e instanceof RpcError && e.status === 'UNAUTHENTICATED' && attempt === 0) {
          attempt += 1;
          this._resetAuth();
          continue;
        }
        throw e;
      }
    }
  }

  private _isWriteMethod(method: string): boolean {
    return this.serverInfo?.methods.some((m) => m.name === method && m.kind === 'write') ?? false;
  }

  /** Capability negotiation: fail fast (no round-trip) if the server didn't advertise `method`. */
  private _assertMethodSupported(method: string): void {
    if (this.serverInfo && !this.serverInfo.methods.some((m) => m.name === method)) {
      throw new RpcError('NOT_FOUND', `server does not support method "${method}"`);
    }
  }

  /** Call a server-streaming method. Yields chunks in order; throws on error; cancels on
   *  early break or `opts.signal`. */
  stream<T = unknown>(method: string, params?: unknown, opts?: InvokeOptions): AsyncGenerator<T> {
    return this._stream<T>(method, params, opts);
  }

  private async *_stream<T>(method: string, params?: unknown, opts?: InvokeOptions): AsyncGenerator<T> {
    await this.authenticate();
    this._assertMethodSupported(method);
    const id = (this.cfg.generateId ?? genId)();
    const deadlineMs = opts?.deadlineMs ?? this.cfg.defaultDeadlineMs ?? DEFAULT_DEADLINE_MS;
    const call = new StreamCall<T>();
    this.streams.set(id, call as StreamCall<unknown>);

    let aborted = false;
    const onAbort = (): void => {
      aborted = true;
      this.cfg.send({ type: 'cancel', id });
      call.fail(new RpcError('CANCELLED', 'cancelled by caller'));
    };
    if (opts?.signal) {
      if (opts.signal.aborted) {
        this.streams.delete(id);
        throw new RpcError('CANCELLED', 'aborted before send');
      }
      opts.signal.addEventListener('abort', onAbort, { once: true });
    }

    this.cfg.send({ type: 'req', id, method, params, deadlineMs });

    try {
      for await (const chunk of call.iterate()) yield chunk;
    } finally {
      this.streams.delete(id);
      if (opts?.signal) opts.signal.removeEventListener('abort', onAbort);
      // Consumer stopped early (break) before a terminal frame → ask the server to stop.
      if (!call.terminated && !aborted) this.cfg.send({ type: 'cancel', id });
    }
  }

  private _invokeOnce<R>(method: string, params?: unknown, opts?: InvokeOptions): Promise<R> {
    const id = (this.cfg.generateId ?? genId)();
    const deadlineMs = opts?.deadlineMs ?? this.cfg.defaultDeadlineMs ?? DEFAULT_DEADLINE_MS;

    if (opts?.signal?.aborted) return Promise.reject(new RpcError('CANCELLED', 'aborted before send'));

    const req: ReqFrame = {
      type: 'req',
      id,
      method,
      params,
      idempotencyKey: opts?.idempotencyKey,
      deadlineMs,
      readAfter: opts?.readAfter,
    };

    return new Promise<R>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._take(id);
        reject(new RpcError('DEADLINE_EXCEEDED', `no response for "${method}"`));
      }, deadlineMs + CLIENT_DEADLINE_GRACE_MS);

      const pending: Pending = { resolve: resolve as (v: unknown) => void, reject, timer };

      if (opts?.signal) {
        const onAbort = (): void => {
          this._take(id);
          this.cfg.send({ type: 'cancel', id });
          reject(new RpcError('CANCELLED', 'cancelled by caller'));
        };
        pending.signal = opts.signal;
        pending.onAbort = onAbort;
        opts.signal.addEventListener('abort', onAbort, { once: true });
      }

      this.pending.set(id, pending);
      this.cfg.send(req);
    });
  }

  handleMessage(frame: ServerFrame): void {
    switch (frame.type) {
      case 'auth-ok':
        this.serverInfo = frame;
        this.authResolve?.(frame);
        break;
      case 'auth-err':
        this.authReject?.(new RpcError(frame.status, frame.message));
        break;
      case 'res': {
        if (frame.hlc !== undefined) this.lastHlc = frame.hlc;
        const p = this._take(frame.id);
        p?.resolve(frame.body);
        break;
      }
      case 'err': {
        const p = this._take(frame.id);
        if (p) {
          p.reject(new RpcError(frame.status, frame.message, frame.retryable));
        } else {
          const s = this.streams.get(frame.id);
          if (s) {
            this.streams.delete(frame.id);
            s.fail(new RpcError(frame.status, frame.message, frame.retryable));
          }
        }
        break;
      }
      case 'stream-chunk': {
        this.streams.get(frame.id)?.push(frame.chunk);
        break;
      }
      case 'stream-end': {
        const s = this.streams.get(frame.id);
        if (s) {
          this.streams.delete(frame.id);
          s.end();
        }
        break;
      }
      // 'stream-start' and 'pong' need no client action yet.
      default:
        break;
    }
  }

  /** Reject all pending calls and the auth handshake (e.g. on channel close). */
  reset(reason: RpcError): void {
    for (const id of [...this.pending.keys()]) {
      const p = this._take(id);
      p?.reject(reason);
    }
    for (const id of [...this.streams.keys()]) {
      const s = this.streams.get(id);
      this.streams.delete(id);
      s?.fail(reason);
    }
    if (this.authReject && !this.serverInfo) this.authReject(reason);
    this.authReady = null;
    this.authResolve = null;
    this.authReject = null;
    this.serverInfo = null;
  }

  /** Drop the current session so the next call re-runs the auth handshake (fresh token). */
  private _resetAuth(): void {
    this.authReady = null;
    this.authResolve = null;
    this.authReject = null;
    this.serverInfo = null;
  }

  private _take(id: string): Pending | undefined {
    const p = this.pending.get(id);
    if (!p) return undefined;
    this.pending.delete(id);
    if (p.timer) clearTimeout(p.timer);
    if (p.signal && p.onAbort) p.signal.removeEventListener('abort', p.onAbort);
    return p;
  }
}

function genId(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `id-${Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`;
}

/** Backing buffer for a server-streaming call: a queue fed by incoming frames, drained by an
 *  async iterator. `terminated` distinguishes "stream ended/errored" from "caller broke early". */
class StreamCall<T> {
  private queue: T[] = [];
  private ended = false;
  private err: unknown = null;
  private resolveNext: (() => void) | null = null;
  terminated = false;

  push(chunk: T): void {
    this.queue.push(chunk);
    this._wake();
  }

  end(): void {
    this.ended = true;
    this.terminated = true;
    this._wake();
  }

  fail(e: unknown): void {
    this.err = e;
    this.ended = true;
    this.terminated = true;
    this._wake();
  }

  private _wake(): void {
    const r = this.resolveNext;
    this.resolveNext = null;
    r?.();
  }

  async *iterate(): AsyncGenerator<T> {
    for (;;) {
      while (this.queue.length > 0) yield this.queue.shift()!;
      if (this.err) throw this.err;
      if (this.ended) return;
      await new Promise<void>((resolve) => {
        this.resolveNext = resolve;
      });
    }
  }
}
