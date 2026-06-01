/**
 * In-memory fakes for WebRTC + signaling, for testing WebRTCTransport without a browser.
 *
 * Two FakeRTCPeerConnections auto-link via a shared registry: the fake SDP carries the
 * offerer's id, so when the answer's remote description is set on both ends, a data-channel
 * pair is created and opened, and `ondatachannel` fires on the answerer. FakeSignalingHub
 * models the signaling server (relays offer/answer/ice by `to`, stamps `fromRole`, and
 * answers `register` with `peer-list` for normals or `server-list` for consumers).
 *
 * Not shipped: only `src/index.ts` is built (see tsup.config.ts) and only `dist` is published.
 */

type Listener = (ev: { data?: string; candidate?: unknown; channel?: FakeRTCDataChannel } | Record<string, unknown>) => void;

// ─── Data channel ───────────────────────────────────────────────────────────────

export class FakeRTCDataChannel {
  readyState: 'connecting' | 'open' | 'closing' | 'closed' = 'connecting';
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  onopen: Listener | null = null;
  onmessage: Listener | null = null;
  onclose: Listener | null = null;
  onerror: Listener | null = null;
  _peer: FakeRTCDataChannel | null = null;
  private listeners = new Map<string, Set<Listener>>();

  constructor(public label: string) {}

  addEventListener(type: string, cb: Listener): void {
    let set = this.listeners.get(type);
    if (!set) this.listeners.set(type, (set = new Set()));
    set.add(cb);
  }

  removeEventListener(type: string, cb: Listener): void {
    this.listeners.get(type)?.delete(cb);
  }

  private _emit(type: string, ev: Record<string, unknown>): void {
    const on = (this as unknown as Record<string, Listener | null>)['on' + type];
    if (on) on(ev);
    const set = this.listeners.get(type);
    if (set) for (const cb of [...set]) cb(ev);
  }

  /** @internal — open the channel and notify listeners. */
  _open(): void {
    if (this.readyState === 'open') return;
    this.readyState = 'open';
    this._emit('open', {});
  }

  send(data: string): void {
    if (this.readyState !== 'open') throw new Error('FakeRTCDataChannel: not open');
    const peer = this._peer;
    if (!peer) return;
    setTimeout(() => {
      if (peer.readyState === 'open') peer._emit('message', { data });
    }, 0);
  }

  close(): void {
    if (this.readyState === 'closed') return;
    this.readyState = 'closed';
    // DOM events dispatch asynchronously; emitting synchronously here would re-enter
    // teardown handlers before their map deletes run (double-fire).
    setTimeout(() => this._emit('close', {}), 0);
    const peer = this._peer;
    if (peer && peer.readyState !== 'closed') setTimeout(() => peer.close(), 0);
  }
}

// ─── Peer connection ──────────────────────────────────────────────────────────────

let _pcCounter = 0;
const _pcRegistry = new Map<string, FakeRTCPeerConnection>();

type ConnState = 'new' | 'connecting' | 'connected' | 'closed' | 'failed' | 'disconnected';

export class FakeRTCPeerConnection {
  id: string;
  localDescription: { type: string; sdp: string } | null = null;
  remoteDescription: { type: string; sdp: string } | null = null;
  connectionState: ConnState = 'new';
  onicecandidate: Listener | null = null;
  onconnectionstatechange: Listener | null = null;
  ondatachannel: Listener | null = null;
  _localChannel: FakeRTCDataChannel | null = null;
  _peer: FakeRTCPeerConnection | null = null;
  private _established = false;
  private _channels: FakeRTCDataChannel[] = [];

  constructor(_config?: unknown) {
    this.id = `pc${++_pcCounter}`;
    _pcRegistry.set(this.id, this);
  }

  createDataChannel(label: string, _opts?: unknown): FakeRTCDataChannel {
    const ch = new FakeRTCDataChannel(label);
    this._localChannel = ch;
    this._channels.push(ch);
    return ch;
  }

  createOffer(): Promise<{ type: string; sdp: string }> {
    return Promise.resolve({ type: 'offer', sdp: `fake:${this.id}` });
  }

  createAnswer(): Promise<{ type: string; sdp: string }> {
    return Promise.resolve({ type: 'answer', sdp: `fake:${this.id}` });
  }

  setLocalDescription(desc: { type: string; sdp: string }): Promise<void> {
    this.localDescription = desc;
    return Promise.resolve();
  }

  setRemoteDescription(desc: { type: string; sdp: string }): Promise<void> {
    this.remoteDescription = desc;
    const m = /^fake:(pc\d+)$/.exec(desc?.sdp ?? '');
    if (m) {
      const peer = _pcRegistry.get(m[1]);
      if (peer) {
        this._peer = peer;
        peer._peer = this;
      }
    }
    this._maybeEstablish();
    return Promise.resolve();
  }

  addIceCandidate(_candidate: unknown): Promise<void> {
    return Promise.resolve();
  }

  /** Once both ends have a remote description, link channels, fire ondatachannel, open. */
  private _maybeEstablish(): void {
    const a = this;
    const b = this._peer;
    if (!b || a._established) return;
    if (!a.remoteDescription || !b.remoteDescription) return;

    const offerer = a.localDescription?.type === 'offer' ? a : b;
    const answerer = offerer === a ? b : a;
    const offChan = offerer._localChannel;
    if (!offChan) return;

    a._established = b._established = true;
    const ansChan = new FakeRTCDataChannel(offChan.label);
    offChan._peer = ansChan;
    ansChan._peer = offChan;
    answerer._channels.push(ansChan);

    setTimeout(() => {
      // Answerer learns the channel (and wires its handlers) BEFORE either end opens.
      if (answerer.ondatachannel) answerer.ondatachannel({ channel: ansChan });
      // If the answerer tore the connection down inside its ondatachannel handler
      // (e.g. refused a non-'rpc' label), do NOT bring the channel up.
      if (answerer.connectionState === 'closed' || offerer.connectionState === 'closed') {
        offChan.close();
        ansChan.close();
        return;
      }
      offerer._setState('connected');
      answerer._setState('connected');
      offChan._open();
      ansChan._open();
    }, 0);
  }

  private _setState(s: ConnState): void {
    this.connectionState = s; // property updates synchronously…
    setTimeout(() => {
      // …but the event dispatches async, matching DOM semantics and avoiding teardown reentrancy.
      if (this.onconnectionstatechange) this.onconnectionstatechange({});
    }, 0);
  }

  close(): void {
    if (this.connectionState === 'closed') return;
    this._setState('closed');
    for (const ch of this._channels) if (ch.readyState !== 'closed') ch.close();
  }
}

// ─── Signaling ──────────────────────────────────────────────────────────────────

let _hub: FakeSignalingHub | null = null;

interface SignalMsg {
  type: string;
  nodeId?: string;
  role?: 'normal' | 'consumer';
  serveConsumers?: boolean;
  to?: string;
  from?: string;
  [k: string]: unknown;
}

export class FakeSignalingHub {
  private sockets = new Map<string, FakeWebSocket>();
  private roles = new Map<string, 'normal' | 'consumer'>();
  private serveConsumers = new Map<string, boolean>();

  handleMessage(socket: FakeWebSocket, msg: SignalMsg): void {
    switch (msg.type) {
      case 'register': {
        const nodeId = msg.nodeId as string;
        const role = msg.role ?? 'normal';
        this.sockets.set(nodeId, socket);
        this.roles.set(nodeId, role);
        if (role === 'normal') {
          this.serveConsumers.set(nodeId, msg.serveConsumers ?? true);
          const peers = [...this.roles.entries()]
            .filter(([id, r]) => r === 'normal' && id !== nodeId)
            .map(([id]) => id);
          this._deliver(nodeId, { type: 'peer-list', peers });
        } else {
          const servers = [...this.roles.entries()]
            .filter(([id, r]) => r === 'normal' && (this.serveConsumers.get(id) ?? true))
            .map(([id]) => id);
          this._deliver(nodeId, { type: 'server-list', servers });
        }
        break;
      }
      case 'offer': {
        const fromRole = this.roles.get(msg.from as string) ?? 'normal';
        const toRole = this.roles.get(msg.to as string);
        // Never broker consumer↔consumer.
        if (fromRole === 'consumer' && toRole === 'consumer') return;
        this._deliver(msg.to as string, { ...msg, fromRole });
        break;
      }
      case 'answer':
      case 'ice-candidate': {
        this._deliver(msg.to as string, msg);
        break;
      }
    }
  }

  private _deliver(nodeId: string, msg: Record<string, unknown>): void {
    const sock = this.sockets.get(nodeId);
    if (sock) setTimeout(() => sock._receive(msg), 0);
  }
}

export class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = FakeWebSocket.CONNECTING;
  onopen: Listener | null = null;
  onmessage: Listener | null = null;
  onclose: Listener | null = null;
  onerror: Listener | null = null;

  constructor(public url: string) {
    setTimeout(() => {
      this.readyState = FakeWebSocket.OPEN;
      if (this.onopen) this.onopen({});
    }, 0);
  }

  send(data: string): void {
    if (!_hub) return;
    const msg = JSON.parse(data) as SignalMsg;
    setTimeout(() => _hub?.handleMessage(this, msg), 0);
  }

  /** @internal — deliver a server→client message. */
  _receive(msg: Record<string, unknown>): void {
    if (this.onmessage) this.onmessage({ data: JSON.stringify(msg) });
  }

  close(): void {
    if (this.readyState === FakeWebSocket.CLOSED) return;
    this.readyState = FakeWebSocket.CLOSED;
    if (this.onclose) this.onclose({});
  }
}

// ─── Consumer driver (stand-in for the real Phase 3 ConsumerClient) ───────────────

/** Minimal consumer-side connector used only to exercise the Normal Client transport. */
export class FakeConsumer {
  private ws: FakeWebSocket;
  private pc: FakeRTCPeerConnection | null = null;
  private channel: FakeRTCDataChannel | null = null;
  connected = false;
  received: unknown[] = [];
  serverList: string[] = [];

  constructor(private nodeId: string, room: string, _hubRef: FakeSignalingHub) {
    this.ws = new FakeWebSocket(`ws://fake/?room=${room}&nodeId=${nodeId}`);
    this.ws.onopen = () => {
      this.ws.send(JSON.stringify({ type: 'register', role: 'consumer', nodeId: this.nodeId }));
    };
    this.ws.onmessage = (evt) => {
      const msg = JSON.parse((evt as { data: string }).data) as SignalMsg;
      if (msg.type === 'server-list') {
        this.serverList = msg['servers'] as string[];
      } else if (msg.type === 'answer') {
        void this.pc?.setRemoteDescription(msg['sdp'] as { type: string; sdp: string });
      } else if (msg.type === 'ice-candidate') {
        void this.pc?.addIceCandidate(msg['candidate']);
      }
    };
  }

  /** Open a data channel of `label` to `target`. Use 'rpc' for a real consumer; 'sync'
   *  to assert the transport refuses a consumer trying to grab a gossip channel. */
  async connectTo(target: string, label = 'rpc'): Promise<void> {
    const pc = new FakeRTCPeerConnection({});
    this.pc = pc;
    const ch = pc.createDataChannel(label, { ordered: true });
    this.channel = ch;
    ch.onopen = () => {
      this.connected = true;
    };
    ch.onmessage = (evt) => {
      this.received.push(JSON.parse((evt as { data: string }).data));
    };
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    this.ws.send(
      JSON.stringify({ type: 'offer', to: target, from: this.nodeId, sdp: pc.localDescription }),
    );
  }

  send(data: unknown): void {
    this.channel?.send(JSON.stringify(data));
  }
}

// ─── Install / helpers ────────────────────────────────────────────────────────────

/** Install fakes onto globalThis and return a fresh signaling hub. Call in beforeEach. */
export function installFakeWebRTC(): FakeSignalingHub {
  const hub = new FakeSignalingHub();
  _hub = hub;
  _pcRegistry.clear();
  _pcCounter = 0;
  (globalThis as Record<string, unknown>)['WebSocket'] = FakeWebSocket;
  (globalThis as Record<string, unknown>)['RTCPeerConnection'] = FakeRTCPeerConnection;
  return hub;
}

/** Remove fakes from globalThis. Call in afterEach. */
export function uninstallFakeWebRTC(): void {
  _hub = null;
  _pcRegistry.clear();
  delete (globalThis as Record<string, unknown>)['WebSocket'];
  delete (globalThis as Record<string, unknown>)['RTCPeerConnection'];
}

/** Poll `fn` until truthy or timeout. */
export async function waitFor(fn: () => boolean, timeout = 1000, interval = 5): Promise<void> {
  const start = Date.now();
  while (!fn()) {
    if (Date.now() - start > timeout) throw new Error('waitFor: timed out');
    await new Promise((r) => setTimeout(r, interval));
  }
}

/** Let pending setTimeout(0) callbacks drain. */
export function tick(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
