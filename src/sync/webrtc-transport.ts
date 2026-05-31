import type { SyncMessage } from '../core/types.js';

export interface WebRTCTransportConfig {
  signalingServerUrl: string;
  iceServers: RTCIceServer[];
  nodeId: string;
  room: string;
}

type PeerEventHandler = (peerId: string) => void;
type MessageEventHandler = (peerId: string, message: SyncMessage) => void;

interface PeerState {
  conn: RTCPeerConnection;
  channel: RTCDataChannel | null;
  channelOpen: boolean;
  /** ICE candidates received before setRemoteDescription; applied on drain. */
  pendingCandidates: RTCIceCandidateInit[];
}

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const BACKPRESSURE_HIGH_WATER = 16 * 1024 * 1024; // 16 MB
const BACKPRESSURE_LOW_WATER = 1 * 1024 * 1024;   //  1 MB
const BACKPRESSURE_TIMEOUT_MS = 30000;            // backstop so a stalled channel can't hang a send forever
const SIGNALING_TYPES = ['peer-list', 'offer', 'answer', 'ice-candidate'];

export class WebRTCTransport {
  private config: WebRTCTransportConfig;
  private ws: WebSocket | null = null;
  private peerStates = new Map<string, PeerState>();
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;
  private pendingMessages = new Map<string, SyncMessage[]>();

  onPeerConnected: PeerEventHandler = () => undefined;
  onPeerDisconnected: PeerEventHandler = () => undefined;
  onMessage: MessageEventHandler = () => undefined;

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
      this.ws!.send(JSON.stringify({ type: 'register', nodeId: this.config.nodeId }));
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
        void this._handleOffer(from, offer).catch(() => this._removePeer(from));
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
        const state = this.peerStates.get(from);
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
    this._wireICE(peerId, conn);

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

    this._wireICE(peerId, conn);

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
  private _addIceCandidate(state: PeerState, candidate: RTCIceCandidateInit): void {
    if (state.conn.remoteDescription === null) {
      state.pendingCandidates.push(candidate);
    } else {
      void state.conn.addIceCandidate(candidate).catch(() => undefined);
    }
  }

  private _drainCandidates(state: PeerState): void {
    const pending = state.pendingCandidates;
    state.pendingCandidates = [];
    for (const c of pending) void state.conn.addIceCandidate(c).catch(() => undefined);
  }

  private _wireICE(peerId: string, conn: RTCPeerConnection): void {
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
        this._removePeer(peerId);
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
      let msg: SyncMessage;
      try {
        msg = JSON.parse(evt.data as string) as SyncMessage;
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
    if (channel.bufferedAmount > BACKPRESSURE_HIGH_WATER) {
      channel.bufferedAmountLowThreshold = BACKPRESSURE_LOW_WATER;
      await new Promise<void>((resolve) => {
        let settled = false;
        const finish = () => {
          if (settled) return;
          settled = true;
          channel.removeEventListener('bufferedamountlow', onSettle);
          channel.removeEventListener('close', onSettle);
          channel.removeEventListener('error', onSettle);
          clearTimeout(timer);
          resolve();
        };
        const onSettle = () => finish();
        const timer = setTimeout(finish, BACKPRESSURE_TIMEOUT_MS);
        channel.addEventListener('bufferedamountlow', onSettle);
        // If the channel closes/errors while we wait, resolve too — otherwise this
        // await (and the caller's sync loop) would hang for the whole session.
        channel.addEventListener('close', onSettle);
        channel.addEventListener('error', onSettle);
        // Guard the lost-event race: the drain/close may have happened between the
        // bufferedAmount check and attaching the listeners.
        if (channel.readyState !== 'open' || channel.bufferedAmount <= BACKPRESSURE_LOW_WATER) {
          finish();
        }
      });
    }
    // Only send if the channel is still usable; a dropped channel re-pulls next round.
    if (channel.readyState === 'open') {
      this._sendOnChannel(channel, message);
    }
  }

  send(peerId: string, message: SyncMessage): void {
    const state = this.peerStates.get(peerId);
    if (state?.channelOpen && state.channel) {
      this._sendOnChannel(state.channel, message);
    } else {
      const queue = this.pendingMessages.get(peerId) ?? [];
      queue.push(message);
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
