/**
 * Consumer Client — the thin client entry point (`@aikofy/client-db/consumer`).
 *
 * Holds NO database, never gossips. It connects to a Normal Client over a WebRTC `'rpc'`
 * data channel and calls that client's registered handlers via `invoke()`/`stream()`.
 * Independent of the storage/gossip/snapshot code so this bundle stays tiny.
 *
 * Selection is round-robin over the signaling server's healthy `server-list`. On a dropped
 * connection (or a retryable server error), an idempotent call transparently fails over to the
 * next candidate, re-authenticates, and replays. Reads carry the last write watermark so a
 * failed-over replica serves read-your-writes once it has caught up. Writes are NOT replayed
 * across nodes by default (cross-node dedupe is best-effort until the dedupe store is replicated);
 * pass `{ idempotent: true }` to opt in.
 */
import { RpcClient, type InvokeOptions } from './rpc/client.js';
import { RpcError } from './rpc/errors.js';
import type { ClientFrame, MethodInfo, ServerFrame } from './rpc/protocol.js';

export interface ConsumerClientConfig {
  signalingServerUrl: string;
  /** Room = the Normal Clients' DB name. */
  room: string;
  /** Stable id for this consumer. Defaults to a random id. */
  nodeId?: string;
  iceServers?: RTCIceServer[];
  auth: { getToken: () => string | Promise<string> };
  /** Default per-call deadline (ms). */
  deadlineMs?: number;
}

export class ConsumerClient {
  private readonly cfg: ConsumerClientConfig;
  private readonly rpc: RpcClient;
  private readonly nodeId: string;

  private ws: WebSocket | null = null;
  private pc: RTCPeerConnection | null = null;
  private channel: RTCDataChannel | null = null;
  private pendingCandidates: RTCIceCandidateInit[] = [];

  private servers: string[] = [];
  private serverIndex = 0;
  private connected = false;
  private everConnected = false;
  private connecting: Promise<void> | null = null;
  private closed = false;

  // Signaling readiness (resolves when the first server-list arrives).
  private signalingReady: Promise<void> | null = null;
  private signalingResolve: (() => void) | null = null;
  private signalingReject: ((e: unknown) => void) | null = null;

  // Current channel establishment.
  private establishResolve: (() => void) | null = null;
  private establishReject: ((e: unknown) => void) | null = null;

  constructor(cfg: ConsumerClientConfig) {
    this.cfg = cfg;
    this.nodeId = cfg.nodeId ?? genId();
    this.rpc = new RpcClient({
      send: (frame) => this._sendOnChannel(frame),
      getToken: cfg.auth.getToken,
      defaultDeadlineMs: cfg.deadlineMs,
    });
  }

  /** Connect signaling → pick a server → establish the 'rpc' channel → authenticate. */
  connect(): Promise<void> {
    if (this.connected) return Promise.resolve();
    if (this.connecting) return this.connecting;
    const p = this._establish();
    this.connecting = p;
    const clear = (): void => {
      if (this.connecting === p) this.connecting = null;
    };
    p.then(clear, clear);
    return p;
  }

  /** Call a handler on the connected Normal Client. Connects first; fails over on retryable
   *  errors for idempotent calls (reads by default; writes via `{ idempotent: true }`). */
  async invoke<R = unknown>(method: string, params?: unknown, opts?: InvokeOptions): Promise<R> {
    await this.connect();
    const replayable = opts?.idempotent ?? !this._isWrite(method);
    let attempts = 0;
    for (;;) {
      await this.connect();
      try {
        return await this.rpc.invoke<R>(method, params, opts);
      } catch (e) {
        attempts += 1;
        const maxAttempts = Math.max(1, this.servers.length);
        if (attempts < maxAttempts && replayable && this._isRetryable(e)) {
          // A server-sent retryable error leaves the channel up — force a switch to the next
          // candidate. A dropped channel already tore the connection down via _onChannelDown.
          if (this.connected) this._failover();
          continue;
        }
        throw e;
      }
    }
  }

  /** Call a server-streaming handler. Connects first. (Streams do not auto-fail-over; a dropped
   *  channel ends the stream with an error and the caller restarts.) */
  async *stream<T = unknown>(method: string, params?: unknown, opts?: InvokeOptions): AsyncGenerator<T> {
    await this.connect();
    yield* this.rpc.stream<T>(method, params, opts);
  }

  /** The server's advertised method catalog (after connect). */
  get methods(): MethodInfo[] {
    return this.rpc.serverInfo?.methods ?? [];
  }

  close(): void {
    this.closed = true;
    this.connected = false;
    this.rpc.reset(new RpcError('CANCELLED', 'consumer closed'));
    this._teardownChannel();
    try { this.ws?.close(); } catch { /* ignore */ }
  }

  // ─── Connection lifecycle ──────────────────────────────────────────────────────

  private async _establish(): Promise<void> {
    await this._ensureSignaling();
    if (this.servers.length === 0) throw new RpcError('UNAVAILABLE', 'no Normal Clients available');
    // On a reconnect, advance to the next candidate (round-robin failover).
    if (this.everConnected) this.serverIndex += 1;
    const target = this.servers[this.serverIndex % this.servers.length];
    await this._establishChannel(target);
    this.connected = true;
    this.everConnected = true;
  }

  /** Drop the current channel and reject in-flight calls so invoke() retries on the next node. */
  private _failover(): void {
    this.connected = false;
    this._teardownChannel();
    this.rpc.reset(new RpcError('UNAVAILABLE', 'failing over'));
  }

  private _onChannelDown(): void {
    if (this.closed || !this.channel) return;
    this.connected = false;
    this._teardownChannel();
    this.rpc.reset(new RpcError('UNAVAILABLE', 'rpc channel closed'));
  }

  private _establishChannel(target: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.establishResolve = resolve;
      this.establishReject = reject;

      const pc = new RTCPeerConnection({ iceServers: this.cfg.iceServers ?? [] });
      this.pc = pc;
      const channel = pc.createDataChannel('rpc', { ordered: true });
      this.channel = channel;

      channel.onopen = () => {
        void this.rpc
          .authenticate()
          .then(() => this.establishResolve?.())
          .catch((e) => this.establishReject?.(e));
      };
      channel.onclose = () => this._onChannelDown();
      channel.onmessage = (evt) => {
        let frame: ServerFrame;
        try {
          frame = JSON.parse(evt.data as string) as ServerFrame;
        } catch {
          return;
        }
        this.rpc.handleMessage(frame);
      };

      pc.onicecandidate = (evt) => {
        if (evt.candidate) {
          this._sendSignal({ type: 'ice-candidate', to: target, from: this.nodeId, candidate: evt.candidate });
        }
      };
      pc.onconnectionstatechange = () => {
        const s = pc.connectionState;
        if (s === 'failed' || s === 'closed' || s === 'disconnected') {
          this.establishReject?.(new RpcError('UNAVAILABLE', 'connection failed'));
          this._onChannelDown();
        }
      };

      pc.createOffer()
        .then((offer) => pc.setLocalDescription(offer))
        .then(() => this._sendSignal({ type: 'offer', to: target, from: this.nodeId, sdp: pc.localDescription }))
        .catch((e) => this.establishReject?.(e));
    });
  }

  private _teardownChannel(): void {
    const ch = this.channel;
    const pc = this.pc;
    this.channel = null;
    this.pc = null;
    this.pendingCandidates = [];
    if (ch) ch.onclose = null; // prevent re-entrant _onChannelDown
    if (pc) pc.onconnectionstatechange = null;
    try { ch?.close(); } catch { /* ignore */ }
    try { pc?.close(); } catch { /* ignore */ }
  }

  // ─── Signaling ──────────────────────────────────────────────────────────────────

  private _ensureSignaling(): Promise<void> {
    if (this.signalingReady) return this.signalingReady;
    this.signalingReady = new Promise<void>((resolve, reject) => {
      this.signalingResolve = resolve;
      this.signalingReject = reject;
    });
    this._openWS();
    return this.signalingReady;
  }

  private _openWS(): void {
    const url = appendParams(this.cfg.signalingServerUrl, { room: this.cfg.room, nodeId: this.nodeId });
    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      void Promise.resolve(this.cfg.auth.getToken())
        .then((token) => {
          this._sendSignal({ type: 'register', role: 'consumer', nodeId: this.nodeId, token, protocolVersion: 1 });
        })
        .catch((e) => this.signalingReject?.(e));
    };

    this.ws.onmessage = (evt) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(evt.data as string) as Record<string, unknown>;
      } catch {
        return;
      }
      this._handleSignal(msg);
    };

    this.ws.onclose = () => {
      if (this.closed) return;
      const err = new RpcError('UNAVAILABLE', 'signaling connection closed');
      this.signalingReject?.(err);
      this.signalingReject = null;
      this.establishReject?.(err);
      this.connected = false;
    };
    this.ws.onerror = () => {
      this.ws?.close();
    };
  }

  private _handleSignal(msg: Record<string, unknown>): void {
    switch (msg['type']) {
      case 'server-list':
        this.servers = (msg['servers'] as string[]) ?? [];
        this.signalingResolve?.();
        this.signalingResolve = null;
        this.signalingReject = null;
        break;
      case 'answer':
        if (this.pc) {
          void this.pc
            .setRemoteDescription(msg['sdp'] as RTCSessionDescriptionInit)
            .then(() => this._drainCandidates())
            .catch((e) => this.establishReject?.(e));
        }
        break;
      case 'ice-candidate':
        this._addCandidate(msg['candidate'] as RTCIceCandidateInit);
        break;
      case 'auth-err': {
        const err = new RpcError('UNAUTHENTICATED', (msg['message'] as string) ?? 'token rejected');
        this.signalingReject?.(err);
        this.signalingReject = null;
        this.establishReject?.(err);
        break;
      }
    }
  }

  private _addCandidate(candidate: RTCIceCandidateInit): void {
    if (this.pc?.remoteDescription) {
      void this.pc.addIceCandidate(candidate).catch(() => undefined);
    } else {
      this.pendingCandidates.push(candidate);
    }
  }

  private _drainCandidates(): void {
    const pc = this.pc;
    if (!pc) return;
    const pending = this.pendingCandidates;
    this.pendingCandidates = [];
    for (const c of pending) void pc.addIceCandidate(c).catch(() => undefined);
  }

  // ─── helpers ──────────────────────────────────────────────────────────────────

  private _isWrite(method: string): boolean {
    return this.rpc.serverInfo?.methods.some((m) => m.name === method && m.kind === 'write') ?? false;
  }

  private _isRetryable(e: unknown): boolean {
    if (this.closed) return false;
    if (!(e instanceof RpcError)) return false;
    return e.status === 'UNAVAILABLE' || e.status === 'RESOURCE_EXHAUSTED';
  }

  private _sendOnChannel(frame: ClientFrame): void {
    const ch = this.channel;
    if (ch && ch.readyState === 'open') ch.send(JSON.stringify(frame));
  }

  private _sendSignal(msg: Record<string, unknown>): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg));
  }
}

export type { InvokeOptions } from './rpc/client.js';
export { RpcError } from './rpc/errors.js';
export type { RpcStatus, MethodInfo, AuthOkFrame } from './rpc/protocol.js';

function appendParams(url: string, params: Record<string, string>): string {
  let out = url;
  for (const [key, value] of Object.entries(params)) {
    const sep = out.includes('?') ? '&' : '?';
    out = `${out}${sep}${key}=${encodeURIComponent(value)}`;
  }
  return out;
}

function genId(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `consumer-${Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`;
}
