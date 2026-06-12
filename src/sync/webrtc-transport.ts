import type { SyncMessage } from '../core/types.js';
import { drainIfNeeded, BACKPRESSURE_LOW_WATER } from './backpressure.js';

export interface WebRTCTransportConfig {
  signalingServerUrl: string;
  iceServers: RTCIceServer[];
  nodeId: string;
  room: string;
  /** Whether this Normal Client accepts Consumer ('rpc') connections. Default true. */
  serveConsumers?: boolean;
}

type PeerEventHandler = (peerId: string) => void;
type MessageEventHandler = (peerId: string, message: SyncMessage) => void;
/** Consumer ('rpc' channel) frames are opaque to the transport; the RPC layer parses them. */
type ConsumerMessageHandler = (consumerId: string, data: unknown) => void;

interface PeerState {
  conn: RTCPeerConnection;
  channel: RTCDataChannel | null;
  channelOpen: boolean;
  /** ICE candidates received before setRemoteDescription; applied on drain. */
  pendingCandidates: RTCIceCandidateInit[];
}

/**
 * State for an inbound Consumer connection. Kept in a SEPARATE map from `peerStates`
 * so consumers can never leak into gossip: `peers()`, `broadcast()` and
 * `onPeerConnected` only ever read `peerStates`. (Phase 4 will add session/auth here.)
 */
interface ConsumerState {
  conn: RTCPeerConnection;
  channel: RTCDataChannel | null;
  channelOpen: boolean;
  pendingCandidates: RTCIceCandidateInit[];
}

/** Minimal shape shared by PeerState/ConsumerState for ICE candidate buffering. */
interface CandidateBuffer {
  conn: RTCPeerConnection;
  pendingCandidates: RTCIceCandidateInit[];
}

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const SIGNALING_TYPES = ['peer-list', 'offer', 'answer', 'ice-candidate'];
// Incoming frame caps (UTF-16 units ≈ bytes): drop absurd frames BEFORE JSON.parse,
// so a hostile/buggy sender can't force huge allocations. Gossip legitimately carries
// 500-doc sync/snapshot batches → generous cap; RPC requests are server-capped at
// ~1 MB params → a much tighter cap suffices.
export const MAX_SYNC_MESSAGE_BYTES = 64 * 1024 * 1024; // 64 MB
export const MAX_RPC_MESSAGE_BYTES = 4 * 1024 * 1024; //    4 MB
// Bound the per-peer queue of messages awaiting channel-open. Dropping the oldest is
// safe for gossip: anything dropped is re-pulled by a later sync round (watermarks
// only advance on success).
const MAX_PENDING_MESSAGES_PER_PEER = 256;

export class WebRTCTransport {
  private config: WebRTCTransportConfig;
  private ws: WebSocket | null = null;
  private peerStates = new Map<string, PeerState>();
  /** Inbound Consumer connections, kept apart from gossip peers (see ConsumerState). */
  private consumerStates = new Map<string, ConsumerState>();
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;
  private pendingMessages = new Map<string, SyncMessage[]>();

  onPeerConnected: PeerEventHandler = () => undefined;
  onPeerDisconnected: PeerEventHandler = () => undefined;
  onMessage: MessageEventHandler = () => undefined;

  // ─── Consumer ('rpc') connection events — consumed by the RPC server (Phase 2) ──
  onConsumerConnected: PeerEventHandler = () => undefined;
  onConsumerDisconnected: PeerEventHandler = () => undefined;
  onConsumerMessage: ConsumerMessageHandler = () => undefined;

  constructor(config: WebRTCTransportConfig) {
    this.config = config;
  }

  connect(): void {
    this.stopped = false;
    this._openWS();
  }

  private _openWS(): void {
    if (this.stopped) return;

    try {
      const url = _appendParams(this.config.signalingServerUrl, {
        room: this.config.room,
        nodeId: this.config.nodeId,
      });
      this.ws = new WebSocket(url);
    } catch {
      this._scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      // Do NOT reset the backoff here — a server that accepts then immediately
      // closes would loop at the base delay forever. Reset only once we receive
      // a real signaling message (proof the server is actually healthy).
      this.ws!.send(JSON.stringify({
        type: 'register',
        nodeId: this.config.nodeId,
        role: 'normal',
        serveConsumers: this.config.serveConsumers ?? true,
      }));
    };

    this.ws.onmessage = (evt) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(evt.data as string) as Record<string, unknown>;
      } catch {
        return;
      }
      this._handleSignalingMessage(msg);
    };

    this.ws.onclose = () => {
      if (!this.stopped) this._scheduleReconnect();
    };

    this.ws.onerror = () => {
      this.ws?.close();
    };
  }

  private _scheduleReconnect(): void {
    if (this.stopped) return;
    const jitter = Math.random() * 0.3;
    const delay = Math.min(
      RECONNECT_BASE_MS * 2 ** this.reconnectAttempt * (1 + jitter),
      RECONNECT_MAX_MS,
    );
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this._openWS();
    }, delay);
  }

  private _handleSignalingMessage(msg: Record<string, unknown>): void {
    // A recognized signaling message proves the server is healthy → reset backoff.
    if (SIGNALING_TYPES.includes(msg['type'] as string)) this.reconnectAttempt = 0;

    switch (msg['type']) {
      case 'peer-list': {
        const peers = msg['peers'] as string[];
        for (const peerId of peers) {
          if (peerId !== this.config.nodeId && !this.peerStates.has(peerId)) {
            this._initiateConnection(peerId);
          }
        }
        break;
      }
      case 'offer': {
        const from = msg['from'] as string;
        const offer = msg['sdp'] as RTCSessionDescriptionInit;
        // `fromRole` is stamped by the signaling server. A consumer offer is routed to
        // the isolated consumer path; the data-channel label is the authoritative switch.
        if (msg['fromRole'] === 'consumer') {
          void this._handleConsumerOffer(from, offer).catch(() => this._removeConsumer(from));
        } else {
          void this._handleOffer(from, offer).catch(() => this._removePeer(from));
        }
        break;
      }
      case 'answer': {
        const from = msg['from'] as string;
        const answer = msg['sdp'] as RTCSessionDescriptionInit;
        const state = this.peerStates.get(from);
        if (state) {
          void state.conn
            .setRemoteDescription(answer)
            .then(() => this._drainCandidates(state))
            .catch(() => this._removePeer(from));
        }
        break;
      }
      case 'ice-candidate': {
        const from = msg['from'] as string;
        const candidate = msg['candidate'] as RTCIceCandidateInit;
        const state = this.peerStates.get(from) ?? this.consumerStates.get(from);
        if (state) this._addIceCandidate(state, candidate);
        break;
      }
    }
  }

  private _initiateConnection(peerId: string): void {
    const conn = new RTCPeerConnection({ iceServers: this.config.iceServers });
    const channel = conn.createDataChannel('sync', { ordered: true });
    const state: PeerState = { conn, channel, channelOpen: false, pendingCandidates: [] };
    this.peerStates.set(peerId, state);

    this._wireChannel(peerId, channel, state);
    this._wireICE(peerId, conn, (id) => this._removePeer(id));

    void conn
      .createOffer()
      .then((offer) => conn.setLocalDescription(offer))
      .then(() => {
        this._sendSignal({
          type: 'offer',
          to: peerId,
          from: this.config.nodeId,
          sdp: conn.localDescription!,
        });
      })
      .catch(() => this._removePeer(peerId));
  }

  private async _handleOffer(
    peerId: string,
    offer: RTCSessionDescriptionInit,
  ): Promise<void> {
    // Glare: both peers offered simultaneously. Break the tie deterministically
    // so exactly one side proceeds — the "impolite" peer keeps its own offer and
    // ignores this one; the "polite" peer drops its in-flight conn and answers.
    const existing = this.peerStates.get(peerId);
    if (existing) {
      const impolite = this.config.nodeId < peerId;
      if (impolite) return;
      existing.conn.onconnectionstatechange = null;
      existing.conn.onicecandidate = null;
      if (existing.channel) existing.channel.onclose = null;
      existing.conn.close();
      this.peerStates.delete(peerId);
    }

    const conn = new RTCPeerConnection({ iceServers: this.config.iceServers });
    const state: PeerState = { conn, channel: null, channelOpen: false, pendingCandidates: [] };
    this.peerStates.set(peerId, state);

    conn.ondatachannel = (evt) => {
      state.channel = evt.channel;
      this._wireChannel(peerId, evt.channel, state);
    };

    this._wireICE(peerId, conn, (id) => this._removePeer(id));

    await conn.setRemoteDescription(offer);
    this._drainCandidates(state);
    const answer = await conn.createAnswer();
    await conn.setLocalDescription(answer);
    this._sendSignal({
      type: 'answer',
      to: peerId,
      from: this.config.nodeId,
      sdp: conn.localDescription!,
    });
  }

  /** Apply an ICE candidate, or buffer it until the remote description is set. */
  private _addIceCandidate(state: CandidateBuffer, candidate: RTCIceCandidateInit): void {
    if (state.conn.remoteDescription === null) {
      state.pendingCandidates.push(candidate);
    } else {
      void state.conn.addIceCandidate(candidate).catch(() => undefined);
    }
  }

  private _drainCandidates(state: CandidateBuffer): void {
    const pending = state.pendingCandidates;
    state.pendingCandidates = [];
    for (const c of pending) void state.conn.addIceCandidate(c).catch(() => undefined);
  }

  /** Wire ICE relay + teardown. `onClose` lets gossip peers and consumers share this path. */
  private _wireICE(peerId: string, conn: RTCPeerConnection, onClose: (id: string) => void): void {
    conn.onicecandidate = (evt) => {
      if (evt.candidate) {
        this._sendSignal({
          type: 'ice-candidate',
          to: peerId,
          from: this.config.nodeId,
          candidate: evt.candidate,
        });
      }
    };

    conn.onconnectionstatechange = () => {
      if (
        conn.connectionState === 'failed' ||
        conn.connectionState === 'closed' ||
        conn.connectionState === 'disconnected'
      ) {
        onClose(peerId);
      }
    };
  }

  private _wireChannel(
    peerId: string,
    channel: RTCDataChannel,
    state: PeerState,
  ): void {
    channel.onopen = () => {
      state.channelOpen = true;
      channel.bufferedAmountLowThreshold = BACKPRESSURE_LOW_WATER;
      this.onPeerConnected(peerId);

      const queued = this.pendingMessages.get(peerId);
      if (queued) {
        for (const msg of queued) this._sendOnChannel(channel, msg);
        this.pendingMessages.delete(peerId);
      }
    };

    channel.onclose = () => this._removePeer(peerId);

    channel.onmessage = (evt) => {
      const raw = evt.data as unknown;
      if (typeof raw !== 'string' || raw.length > MAX_SYNC_MESSAGE_BYTES) return;
      let msg: SyncMessage;
      try {
        msg = JSON.parse(raw) as SyncMessage;
      } catch {
        return;
      }
      this.onMessage(peerId, msg);
    };
  }

  private _removePeer(peerId: string): void {
    const state = this.peerStates.get(peerId);
    if (!state) return;
    state.conn.close();
    this.peerStates.delete(peerId);
    // Drop any messages queued for a peer that will never open its channel.
    this.pendingMessages.delete(peerId);
    this.onPeerDisconnected(peerId);
  }

  // ─── Consumer ('rpc') connections ──────────────────────────────────────────────
  // A consumer is always the offerer and we only ever answer it. Consumer state lives
  // in `consumerStates`, never `peerStates`, so it can never enter the gossip mesh.

  private async _handleConsumerOffer(
    consumerId: string,
    offer: RTCSessionDescriptionInit,
  ): Promise<void> {
    // No glare to resolve (the consumer never receives an offer from us). Replace any
    // stale connection for this id.
    const existing = this.consumerStates.get(consumerId);
    if (existing) {
      existing.conn.onconnectionstatechange = null;
      existing.conn.onicecandidate = null;
      if (existing.channel) existing.channel.onclose = null;
      existing.conn.close();
      this.consumerStates.delete(consumerId);
    }

    const conn = new RTCPeerConnection({ iceServers: this.config.iceServers });
    const state: ConsumerState = { conn, channel: null, channelOpen: false, pendingCandidates: [] };
    this.consumerStates.set(consumerId, state);

    conn.ondatachannel = (evt) => {
      // Authoritative isolation switch: a consumer may ONLY open an 'rpc' channel.
      // Refuse anything else so a consumer can never acquire a gossip ('sync') channel.
      if (evt.channel.label !== 'rpc') {
        this._removeConsumer(consumerId);
        return;
      }
      state.channel = evt.channel;
      this._wireConsumerChannel(consumerId, evt.channel, state);
    };

    this._wireICE(consumerId, conn, (id) => this._removeConsumer(id));

    await conn.setRemoteDescription(offer);
    this._drainCandidates(state);
    const answer = await conn.createAnswer();
    await conn.setLocalDescription(answer);
    this._sendSignal({
      type: 'answer',
      to: consumerId,
      from: this.config.nodeId,
      sdp: conn.localDescription!,
    });
  }

  private _wireConsumerChannel(
    consumerId: string,
    channel: RTCDataChannel,
    state: ConsumerState,
  ): void {
    channel.onopen = () => {
      state.channelOpen = true;
      this.onConsumerConnected(consumerId);
    };

    channel.onclose = () => this._removeConsumer(consumerId);

    channel.onmessage = (evt) => {
      const raw = evt.data as unknown;
      if (typeof raw !== 'string' || raw.length > MAX_RPC_MESSAGE_BYTES) return;
      let data: unknown;
      try {
        data = JSON.parse(raw);
      } catch {
        return;
      }
      this.onConsumerMessage(consumerId, data);
    };
  }

  private _removeConsumer(consumerId: string): void {
    const state = this.consumerStates.get(consumerId);
    if (!state) return;
    state.conn.close();
    this.consumerStates.delete(consumerId);
    this.onConsumerDisconnected(consumerId);
  }

  private _sendSignal(msg: Record<string, unknown>): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  private _sendOnChannel(channel: RTCDataChannel, message: SyncMessage): void {
    channel.send(JSON.stringify(message));
  }

  async sendAsync(peerId: string, message: SyncMessage): Promise<void> {
    const state = this.peerStates.get(peerId);
    if (!state?.channelOpen || !state.channel || state.channel.readyState !== 'open') {
      this.send(peerId, message);
      return;
    }
    const channel = state.channel;
    await drainIfNeeded(channel);
    // Only send if the channel is still usable; a dropped channel re-pulls next round.
    if (channel.readyState === 'open') {
      this._sendOnChannel(channel, message);
    }
  }

  /** Backpressure-aware send to a consumer (used for RPC streaming). No-op if not open. */
  async sendToConsumerAsync(consumerId: string, data: unknown): Promise<void> {
    const state = this.consumerStates.get(consumerId);
    if (!state?.channelOpen || !state.channel || state.channel.readyState !== 'open') return;
    const channel = state.channel;
    await drainIfNeeded(channel);
    if (channel.readyState === 'open') channel.send(JSON.stringify(data));
  }

  send(peerId: string, message: SyncMessage): void {
    const state = this.peerStates.get(peerId);
    if (state?.channelOpen && state.channel) {
      this._sendOnChannel(state.channel, message);
    } else {
      const queue = this.pendingMessages.get(peerId) ?? [];
      queue.push(message);
      // A channel that never opens must not buffer unboundedly (a 500-doc page
      // can be megabytes). Drop the oldest; gossip re-pulls it next round.
      if (queue.length > MAX_PENDING_MESSAGES_PER_PEER) queue.shift();
      this.pendingMessages.set(peerId, queue);
    }
  }

  broadcast(message: SyncMessage): void {
    // Serialize the payload at most once, regardless of peer count, instead of
    // re-stringifying the identical message per peer inside send().
    let data: string | null = null;
    for (const [peerId, state] of this.peerStates) {
      if (state.channelOpen && state.channel) {
        if (data === null) data = JSON.stringify(message);
        state.channel.send(data);
      } else {
        // Not yet open — keep the existing queueing path (object queued, drained
        // and stringified on channel open).
        this.send(peerId, message);
      }
    }
  }

  peers(): string[] {
    const out: string[] = [];
    for (const [id, state] of this.peerStates) {
      if (state.channelOpen) out.push(id);
    }
    return out;
  }

  /** Connected consumer ids (open 'rpc' channel). Disjoint from peers() by construction. */
  consumers(): string[] {
    const out: string[] = [];
    for (const [id, state] of this.consumerStates) {
      if (state.channelOpen) out.push(id);
    }
    return out;
  }

  /** Send a raw RPC frame to a connected consumer. Dropped if its channel isn't open. */
  sendToConsumer(consumerId: string, data: unknown): void {
    const state = this.consumerStates.get(consumerId);
    if (state?.channelOpen && state.channel && state.channel.readyState === 'open') {
      state.channel.send(JSON.stringify(data));
    }
  }

  disconnect(): void {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.reconnectAttempt = 0;
    this.ws?.close();
    for (const [peerId] of this.peerStates) {
      this._removePeer(peerId);
    }
    for (const [consumerId] of this.consumerStates) {
      this._removeConsumer(consumerId);
    }
  }
}

function _appendParams(url: string, params: Record<string, string>): string {
  let out = url;
  for (const [key, value] of Object.entries(params)) {
    const sep = out.includes('?') ? '&' : '?';
    out = `${out}${sep}${key}=${encodeURIComponent(value)}`;
  }
  return out;
}
