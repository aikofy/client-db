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
}

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;

export class WebRTCTransport {
  private config: WebRTCTransportConfig;
  private ws: WebSocket | null = null;
  private peerStates = new Map<string, PeerState>();
  private reconnectAttempt = 0;
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
      const url = _appendRoomParam(this.config.signalingServerUrl, this.config.room);
      this.ws = new WebSocket(url);
    } catch {
      this._scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      this.reconnectAttempt = 0;
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
    setTimeout(() => this._openWS(), delay);
  }

  private _handleSignalingMessage(msg: Record<string, unknown>): void {
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
        void this._handleOffer(from, offer);
        break;
      }
      case 'answer': {
        const from = msg['from'] as string;
        const answer = msg['sdp'] as RTCSessionDescriptionInit;
        const state = this.peerStates.get(from);
        if (state) void state.conn.setRemoteDescription(answer);
        break;
      }
      case 'ice-candidate': {
        const from = msg['from'] as string;
        const candidate = msg['candidate'] as RTCIceCandidateInit;
        const state = this.peerStates.get(from);
        if (state) void state.conn.addIceCandidate(candidate);
        break;
      }
    }
  }

  private _initiateConnection(peerId: string): void {
    const conn = new RTCPeerConnection({ iceServers: this.config.iceServers });
    const channel = conn.createDataChannel('sync', { ordered: true });
    const state: PeerState = { conn, channel, channelOpen: false };
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
      });
  }

  private async _handleOffer(
    peerId: string,
    offer: RTCSessionDescriptionInit,
  ): Promise<void> {
    const conn = new RTCPeerConnection({ iceServers: this.config.iceServers });
    const state: PeerState = { conn, channel: null, channelOpen: false };
    this.peerStates.set(peerId, state);

    conn.ondatachannel = (evt) => {
      state.channel = evt.channel;
      this._wireChannel(peerId, evt.channel, state);
    };

    this._wireICE(peerId, conn);

    await conn.setRemoteDescription(offer);
    const answer = await conn.createAnswer();
    await conn.setLocalDescription(answer);
    this._sendSignal({
      type: 'answer',
      to: peerId,
      from: this.config.nodeId,
      sdp: conn.localDescription!,
    });
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
    for (const peerId of this.peerStates.keys()) {
      this.send(peerId, message);
    }
  }

  peers(): string[] {
    return Array.from(this.peerStates.keys()).filter(
      (id) => this.peerStates.get(id)?.channelOpen,
    );
  }

  disconnect(): void {
    this.stopped = true;
    this.ws?.close();
    for (const [peerId] of this.peerStates) {
      this._removePeer(peerId);
    }
  }
}

function _appendRoomParam(url: string, room: string): string {
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}room=${encodeURIComponent(room)}`;
}
