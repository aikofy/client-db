import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';
import type { RpcContext, ConsumerIdentity } from './context.js';
import { RpcError, defaultRetryable } from './errors.js';
import { RpcRouter, type HandlerDef, type RpcHandler, type StreamHandler } from './router.js';
import { IdempotencyCache } from './idempotency.js';
import { TokenBucket, byteSize, type CallRecord } from './middleware.js';
import type { MethodKind } from './protocol.js';
import {
  PROTOCOL_VERSION,
  DEFAULT_LIMITS,
  type AuthFrame,
  type CancelFrame,
  type ClientFrame,
  type ErrFrame,
  type PingFrame,
  type ReqFrame,
  type ResFrame,
  type RpcLimits,
  type RpcStatus,
  type ServerFrame,
} from './protocol.js';

/**
 * Verifies a consumer token → identity, or null/throw to reject. Phase 2 has no verifier
 * (every connection is trusted as `{ id: consumerId }`); Phase 4 injects a real WebCrypto one.
 */
export type TokenVerifier = (
  token: string,
  consumerId: string,
) => ConsumerIdentity | null | Promise<ConsumerIdentity | null>;

export interface RpcServerConfig {
  router: RpcRouter;
  /** Local replica handed to handlers as `ctx.db`. */
  adapter: IStorageAdapter;
  /** Current HLC watermark provider (e.g. () => adapter.getHLC().now()). */
  hlc: () => HLCTimestamp;
  /** Sends a frame to a connected consumer (e.g. transport.sendToConsumer). */
  send: (consumerId: string, frame: ServerFrame) => void;
  /** Backpressure-aware send for streaming chunks (e.g. transport.sendToConsumerAsync).
   *  Falls back to a Promise-wrapped `send` if omitted. */
  sendAsync?: (consumerId: string, frame: ServerFrame) => Promise<void>;
  nodeId: string;
  verifyToken?: TokenVerifier;
  limits?: Partial<RpcLimits>;
  /** TTL for the idempotency dedupe cache (ms). Default 10 min. */
  idempotencyTtlMs?: number;
  /** Max time (ms) to wait for the local replica to catch up to a request's `readAfter`
   *  watermark before replying UNAVAILABLE. Default 2000. */
  readAfterTimeoutMs?: number;
  /** Observability hook — invoked once per call (success or failure) for metrics/audit. */
  onCall?: (record: CallRecord) => void;
}

const DEFAULT_READ_AFTER_TIMEOUT_MS = 2000;

interface InflightCall {
  ac: AbortController;
  /** Settle the call once (send a frame, or null to settle silently). Idempotent. */
  finish: (frame: ServerFrame | null) => void;
}

interface Session {
  authenticated: boolean;
  identity: ConsumerIdentity | null;
  /** Token expiry (ms epoch) from claims.exp, if present; reqs after this force re-auth. */
  expiresAt?: number;
  inflight: Map<string, InflightCall>;
  bucket: TokenBucket;
}

/**
 * Dispatches RPC frames from consumers to router handlers. One instance per Normal Client;
 * tracks a session per connected consumer. Wire it to the transport in db.ts:
 *   transport.onConsumerConnected    = (id) => server.onConnect(id)
 *   transport.onConsumerDisconnected = (id) => server.onDisconnect(id)
 *   transport.onConsumerMessage      = (id, data) => server.handleMessage(id, data)
 */
export class RpcServer {
  private readonly config: RpcServerConfig;
  private readonly limits: RpcLimits;
  private readonly sessions = new Map<string, Session>();
  private readonly idempotency: IdempotencyCache;
  private readonly readAfterTimeoutMs: number;

  constructor(config: RpcServerConfig) {
    this.config = config;
    this.limits = { ...DEFAULT_LIMITS, ...config.limits };
    this.idempotency = new IdempotencyCache({ ttlMs: config.idempotencyTtlMs });
    this.readAfterTimeoutMs = config.readAfterTimeoutMs ?? DEFAULT_READ_AFTER_TIMEOUT_MS;
  }

  /** Wait (bounded) for the local replica's HLC to reach `readAfter` — read-your-writes across
   *  failover. Returns false on timeout/abort so the caller can reply UNAVAILABLE. */
  private async _awaitReadAfter(readAfter: HLCTimestamp, signal: AbortSignal): Promise<boolean> {
    if (this.config.hlc() >= readAfter) return true;
    const deadline = Date.now() + this.readAfterTimeoutMs;
    while (Date.now() < deadline) {
      if (signal.aborted) return false;
      await new Promise((r) => setTimeout(r, 50));
      if (this.config.hlc() >= readAfter) return true;
    }
    return false;
  }

  onConnect(consumerId: string): void {
    this.sessions.set(consumerId, {
      authenticated: false,
      identity: null,
      inflight: new Map(),
      bucket: new TokenBucket(this.limits.rateLimit.perMin, Date.now()),
    });
  }

  private _record(
    consumerId: string,
    identityId: string,
    method: string,
    kind: MethodKind | 'unknown',
    status: RpcStatus,
    start: number,
  ): void {
    this.config.onCall?.({ consumerId, identityId, method, kind, status, durationMs: Date.now() - start });
  }

  onDisconnect(consumerId: string): void {
    const session = this.sessions.get(consumerId);
    if (session) {
      // Abort + settle anything in flight so handlers can stop and no late frame is sent.
      for (const call of session.inflight.values()) {
        call.ac.abort();
        call.finish(null);
      }
    }
    this.sessions.delete(consumerId);
  }

  async handleMessage(consumerId: string, data: unknown): Promise<void> {
    const session = this.sessions.get(consumerId);
    if (!session) return;
    if (!isFrame(data)) return;
    const frame = data as ClientFrame;
    switch (frame.type) {
      case 'auth':
        await this._handleAuth(consumerId, session, frame as AuthFrame);
        break;
      case 'req':
        this._handleReq(consumerId, session, frame as ReqFrame);
        break;
      case 'cancel': {
        const call = session.inflight.get((frame as CancelFrame).id);
        if (call) {
          call.ac.abort();
          call.finish(null); // client gave up — settle silently per spec
        }
        break;
      }
      case 'ping': {
        const p = frame as PingFrame;
        this.config.send(consumerId, { type: 'pong', id: p.id, ts: p.ts });
        break;
      }
      default:
        break;
    }
  }

  private async _handleAuth(
    consumerId: string,
    session: Session,
    frame: AuthFrame,
  ): Promise<void> {
    // Capability/version negotiation: refuse a client newer than this server understands.
    if (typeof frame.protocolVersion === 'number' && frame.protocolVersion > PROTOCOL_VERSION) {
      this.config.send(consumerId, {
        type: 'auth-err',
        status: 'FAILED_PRECONDITION',
        message: `unsupported protocol version ${frame.protocolVersion} (server: ${PROTOCOL_VERSION})`,
      });
      return;
    }

    let identity: ConsumerIdentity | null;
    try {
      identity = this.config.verifyToken
        ? await this.config.verifyToken(frame.token, consumerId)
        : { id: consumerId, scopes: [], claims: {} };
    } catch {
      identity = null;
    }

    if (!identity) {
      this.config.send(consumerId, {
        type: 'auth-err',
        status: 'UNAUTHENTICATED',
        message: 'invalid token',
      });
      return;
    }

    session.authenticated = true;
    session.identity = identity;
    const exp = identity.claims['exp'];
    session.expiresAt = typeof exp === 'number' ? exp * 1000 : undefined;
    this.config.send(consumerId, {
      type: 'auth-ok',
      server: this.config.nodeId,
      protocolVersion: PROTOCOL_VERSION,
      methods: this.config.router.catalog(),
      serverHlc: this.config.hlc(),
      limits: this.limits,
    });
  }

  private _handleReq(consumerId: string, session: Session, frame: ReqFrame): void {
    const start = Date.now();
    const idId = session.identity?.id ?? consumerId;
    // Reject + record an outcome in one place (used for all pre-dispatch failures).
    const reject = (status: RpcStatus, message: string, kind: MethodKind | 'unknown', retryable?: boolean): void => {
      this.config.send(consumerId, errFrame(frame.id, status, message, retryable));
      this._record(consumerId, idId, frame.method, kind, status, start);
    };

    if (!session.authenticated || !session.identity) {
      reject('UNAUTHENTICATED', 'authenticate first', 'unknown');
      return;
    }
    // Token expiry (spec §8): once a session's token has expired, force a fresh auth.
    if (session.expiresAt !== undefined && Date.now() > session.expiresAt) {
      session.authenticated = false;
      session.identity = null;
      session.expiresAt = undefined;
      reject('UNAUTHENTICATED', 'token expired; re-authenticate', 'unknown');
      return;
    }
    // Per-consumer rate limit.
    if (!session.bucket.tryRemove(start)) {
      reject('RESOURCE_EXHAUSTED', 'rate limit exceeded', 'unknown', true);
      return;
    }
    // Payload cap.
    if (byteSize(frame.params) > this.limits.maxPayloadBytes) {
      reject('INVALID_ARGUMENT', 'request payload too large', 'unknown');
      return;
    }
    const def = this.config.router.get(frame.method);
    if (!def) {
      reject('NOT_FOUND', `unknown method "${frame.method}"`, 'unknown');
      return;
    }
    if (def.kind === 'stream') {
      this._handleStream(consumerId, session, def, frame, start);
      return;
    }

    const ac = new AbortController();
    let settled = false;
    let timer: ReturnType<typeof setTimeout>;
    const finish = (out: ServerFrame | null): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      session.inflight.delete(frame.id);
      if (out) {
        this.config.send(consumerId, out);
        const status: RpcStatus = out.type === 'res' ? 'OK' : out.type === 'err' ? out.status : 'INTERNAL';
        this._record(consumerId, idId, frame.method, def.kind, status, start);
      }
    };
    const deadlineMs = frame.deadlineMs ?? this.limits.defaultDeadlineMs;
    timer = setTimeout(() => {
      ac.abort();
      finish(errFrame(frame.id, 'DEADLINE_EXCEEDED', 'deadline exceeded', false));
    }, deadlineMs);
    session.inflight.set(frame.id, { ac, finish });

    void this._runHandler(consumerId, session.identity, def, frame, ac.signal)
      .then((out) => finish(out))
      .catch((e) => finish(errFrame(frame.id, 'INTERNAL', errMsg(e))));
  }

  private async _runHandler(
    consumerId: string,
    identity: ConsumerIdentity,
    def: HandlerDef,
    frame: ReqFrame,
    signal: AbortSignal,
  ): Promise<ServerFrame> {
    // Authorize: the caller's token must carry every scope the handler declares.
    if (def.scopes.length > 0) {
      const have = new Set(identity.scopes);
      const missing = def.scopes.filter((s) => !have.has(s));
      if (missing.length > 0) {
        return errFrame(frame.id, 'PERMISSION_DENIED', `missing scope(s): ${missing.join(', ')}`);
      }
    }

    if (frame.readAfter && !(await this._awaitReadAfter(frame.readAfter, signal))) {
      return errFrame(frame.id, 'UNAVAILABLE', 'replica has not caught up to readAfter', true);
    }

    let params = frame.params;
    if (def.input) {
      try {
        params = def.input.parse(params);
      } catch (e) {
        return errFrame(frame.id, 'INVALID_ARGUMENT', errMsg(e));
      }
    }

    const ctx = this._makeContext(consumerId, identity, frame, signal);
    try {
      const result = await (def.handler as RpcHandler)(ctx, params);
      const res: ResFrame = { type: 'res', id: frame.id, status: 'OK', body: result };
      // Writes return the post-write HLC watermark for read-your-writes (Phase 7 uses readAfter).
      if (def.kind === 'write') res.hlc = this.config.hlc();
      return res;
    } catch (e) {
      if (e instanceof RpcError) return errFrame(frame.id, e.status, e.message, e.retryable);
      throw e; // → INTERNAL in the caller's .catch
    }
  }

  private _handleStream(
    consumerId: string,
    session: Session,
    def: HandlerDef,
    frame: ReqFrame,
    start: number,
  ): void {
    const idId = session.identity?.id ?? consumerId;
    const ac = new AbortController();
    let settled = false;
    let timer: ReturnType<typeof setTimeout>;
    const finish = (out: ServerFrame | null): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      ac.abort(); // stops the producer (the async generator) via ctx.signal
      session.inflight.delete(frame.id);
      if (out) {
        this.config.send(consumerId, out);
        const status: RpcStatus = out.type === 'res' ? 'OK' : out.type === 'err' ? out.status : 'INTERNAL';
        this._record(consumerId, idId, frame.method, def.kind, status, start);
      }
    };
    const deadlineMs = frame.deadlineMs ?? this.limits.defaultDeadlineMs;
    timer = setTimeout(() => finish(errFrame(frame.id, 'DEADLINE_EXCEEDED', 'deadline exceeded', false)), deadlineMs);
    session.inflight.set(frame.id, { ac, finish });

    void this._runStream(consumerId, session.identity!, def, frame, ac.signal)
      .then(() => {
        // Completed normally — close the stream (unless cancelled/timed-out/errored already).
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        session.inflight.delete(frame.id);
        this.config.send(consumerId, { type: 'stream-end', id: frame.id, status: 'OK' });
        this._record(consumerId, idId, frame.method, def.kind, 'OK', start);
      })
      .catch((e) => {
        finish(
          e instanceof RpcError
            ? errFrame(frame.id, e.status, e.message, e.retryable)
            : errFrame(frame.id, 'INTERNAL', errMsg(e)),
        );
      });
  }

  private async _runStream(
    consumerId: string,
    identity: ConsumerIdentity,
    def: HandlerDef,
    frame: ReqFrame,
    signal: AbortSignal,
  ): Promise<void> {
    if (def.scopes.length > 0) {
      const have = new Set(identity.scopes);
      const missing = def.scopes.filter((s) => !have.has(s));
      if (missing.length > 0) throw new RpcError('PERMISSION_DENIED', `missing scope(s): ${missing.join(', ')}`);
    }

    let params = frame.params;
    if (def.input) {
      try {
        params = def.input.parse(params);
      } catch (e) {
        throw new RpcError('INVALID_ARGUMENT', errMsg(e));
      }
    }

    if (frame.readAfter && !(await this._awaitReadAfter(frame.readAfter, signal))) {
      throw new RpcError('UNAVAILABLE', 'replica has not caught up to readAfter', true);
    }

    const ctx = this._makeContext(consumerId, identity, frame, signal);
    const sendChunk = this.config.sendAsync ?? ((id, f) => (this.config.send(id, f), Promise.resolve()));

    this.config.send(consumerId, { type: 'stream-start', id: frame.id });

    let seq = 0;
    // Breaking the for-await (on abort) invokes the generator's return(), running its finally.
    for await (const chunk of (def.handler as StreamHandler)(ctx, params)) {
      if (signal.aborted) break;
      await sendChunk(consumerId, { type: 'stream-chunk', id: frame.id, seq: seq++, chunk });
      if (signal.aborted) break;
    }
  }

  private _makeContext(
    consumerId: string,
    identity: ConsumerIdentity,
    frame: ReqFrame,
    signal: AbortSignal,
  ): RpcContext {
    void consumerId;
    return {
      consumer: identity,
      db: this.config.adapter,
      hlc: () => this.config.hlc(),
      idempotent: <T>(key: string | undefined, fn: () => T | Promise<T>): Promise<T> =>
        key === undefined
          ? (Promise.resolve(fn()) as Promise<T>)
          : this.idempotency.run<T>(`${identity.id}:${frame.method}:${key}`, fn),
      signal,
      request: { id: frame.id, method: frame.method, idempotencyKey: frame.idempotencyKey },
      log: () => undefined,
    };
  }
}

function errFrame(id: string, status: RpcStatus, message: string, retryable?: boolean): ErrFrame {
  return { type: 'err', id, status, message, retryable: retryable ?? defaultRetryable(status) };
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function isFrame(data: unknown): data is { type: string } {
  return typeof data === 'object' && data !== null && typeof (data as { type?: unknown }).type === 'string';
}
