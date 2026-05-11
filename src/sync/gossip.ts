import { v4 as uuidv4 } from 'uuid';
import { resolveConflict, logConflict } from '../core/conflict.js';
import { requestSnapshot, importSnapshot, sendSnapshotTo } from './snapshot.js';
import type {
  IStorageAdapter,
  HLCTimestamp,
  Doc,
  SyncRequestMessage,
  SyncResponseMessage,
  PeerHelloMessage,
  SyncMessage,
  CollectionSchema,
} from '../core/types.js';
import type { WebRTCTransport } from './webrtc-transport.js';
import type { IndexedDBAdapter } from '../storage/indexeddb.js';

const GOSSIP_INTERVAL_MS = 30_000;
const FANOUT = 3;
const META_PEER_SYNC_PREFIX = 'lastSync:';

export class GossipSync {
  private transport: WebRTCTransport;
  private adapter: IndexedDBAdapter;
  private collections: Record<string, CollectionSchema>;
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private pendingRequests = new Map<string, (msg: SyncResponseMessage) => void>();
  private onlineListener: (() => void) | null = null;

  // Bootstrap state
  private peerHLCs = new Map<string, HLCTimestamp>();
  private bootstrapped = false;
  private bootstrapping = false;

  constructor(
    transport: WebRTCTransport,
    adapter: IndexedDBAdapter,
    collections: Record<string, CollectionSchema>,
  ) {
    this.transport = transport;
    this.adapter = adapter;
    this.collections = collections;
  }

  start(): void {
    this.transport.onMessage = this._handleMessage.bind(this);

    // Chain onto whatever db.ts wired (e.g. syncStatus update)
    const upstreamConnected = this.transport.onPeerConnected;
    this.transport.onPeerConnected = (peerId) => {
      upstreamConnected(peerId);
      this._onPeerConnected(peerId);
    };

    this.intervalId = setInterval(() => void this._gossipRound(), GOSSIP_INTERVAL_MS);

    if (typeof window !== 'undefined') {
      this.onlineListener = () => void this._onOnline();
      window.addEventListener('online', this.onlineListener);
    }

    // Initial gossip after connections settle
    setTimeout(() => void this._gossipRound(), 1000);
  }

  stop(): void {
    if (this.intervalId) clearInterval(this.intervalId);
    if (this.onlineListener && typeof window !== 'undefined') {
      window.removeEventListener('online', this.onlineListener);
    }
  }

  /** Push a single doc to all connected peers immediately (no round-trip). */
  broadcastDoc(collection: string, doc: Doc): void {
    const peers = this.transport.peers();
    if (peers.length === 0) return;

    const message: SyncResponseMessage = {
      type: 'sync-response',
      changes: [{
        id: doc._id,
        collection,
        _rev: doc._rev,
        _updatedAt: doc._updatedAt,
        operation: doc._deleted ? 'delete' : 'put',
        origin: 'local' as const,
      }],
      docs: [{ ...doc, _collection: collection } as Doc & { _collection: string }],
      fromNodeId: this.adapter.nodeId,
      requestId: uuidv4(),
    };

    this.transport.broadcast(message);
  }

  // ─── Peer connection & bootstrap ─────────────────────────────────────────────

  private _onPeerConnected(peerId: string): void {
    const hello: PeerHelloMessage = {
      type: 'peer-hello',
      nodeId: this.adapter.nodeId,
      currentHLC: this.adapter.getHLC().now(),
    };
    this.transport.send(peerId, hello);
  }

  private async _handlePeerHello(peerId: string, msg: PeerHelloMessage): Promise<void> {
    this.peerHLCs.set(peerId, msg.currentHLC);
    await this._maybeBootstrap();
  }

  /**
   * Runs once on first peer contact. Three cases:
   *
   * 1. Empty DB — fetch full snapshot from the peer with the highest HLC,
   *    then delta-sync with all remaining peers to catch anything they had.
   *
   * 2. Non-empty DB, never peer-synced — pre-loaded snapshot or freshly
   *    created node: sync with ALL peers for bidirectional delta coverage.
   *
   * 3. Returning client — same as (2) for bootstrap; subsequent reconnects
   *    are handled by _onOnline().
   */
  private async _maybeBootstrap(): Promise<void> {
    if (this.bootstrapped || this.bootstrapping) return;
    this.bootstrapping = true;

    try {
      const isEmpty = await this._isDBEmpty();

      if (isEmpty) {
        const bestPeer = this._findBestPeer();
        if (!bestPeer) return; // wait for more peer-hellos before deciding

        try {
          const snapshot = await requestSnapshot(this.transport, bestPeer);
          await importSnapshot(this.adapter, snapshot);
        } catch {
          // snapshot failed — fall back to delta from that peer
          await this._syncWithPeer(bestPeer).catch(() => undefined);
        }

        // Catch any changes the best peer didn't have
        const others = this.transport.peers().filter((p) => p !== bestPeer);
        await Promise.allSettled(others.map((p) => this._syncWithPeer(p)));
      } else {
        // Pre-loaded snapshot (case 1) or first reconnect (case 3):
        // sync all peers so no offline writes are missed
        await this._syncAllPeers();
      }

      this.bootstrapped = true;
    } finally {
      this.bootstrapping = false;
    }
  }

  private _findBestPeer(): string | null {
    const connected = new Set(this.transport.peers());
    let bestPeer: string | null = null;
    let bestHLC: HLCTimestamp = '' as HLCTimestamp;

    for (const [peerId, hlc] of this.peerHLCs) {
      if (connected.has(peerId) && hlc > bestHLC) {
        bestHLC = hlc;
        bestPeer = peerId;
      }
    }
    return bestPeer;
  }

  private async _isDBEmpty(): Promise<boolean> {
    for (const collection of Object.keys(this.collections)) {
      const docs = await this.adapter.query(collection, { limit: 1, includeDeleted: true });
      if (docs.length > 0) return false;
    }
    return true;
  }

  // ─── Reconnect ────────────────────────────────────────────────────────────────

  private async _onOnline(): Promise<void> {
    if (this.bootstrapped) {
      // Returning client: sync ALL peers to collect every peer's offline writes
      await this._syncAllPeers();
    } else {
      void this._gossipRound();
    }
  }

  // ─── Gossip ───────────────────────────────────────────────────────────────────

  private async _gossipRound(): Promise<void> {
    const peers = this.transport.peers();
    if (peers.length === 0) return;

    const selected = _pickRandom(peers, FANOUT);
    await Promise.allSettled(selected.map((p) => this._syncWithPeer(p)));
  }

  private async _syncAllPeers(): Promise<void> {
    const peers = this.transport.peers();
    await Promise.allSettled(peers.map((p) => this._syncWithPeer(p)));
  }

  private async _syncWithPeer(peerId: string): Promise<void> {
    const since = await this._getLastSyncHLC(peerId);
    const requestId = uuidv4();
    const nodeId = this.adapter.nodeId;

    const request: SyncRequestMessage = {
      type: 'sync-request',
      since,
      collections: Object.keys(this.collections),
      fromNodeId: nodeId,
    };

    const response = await new Promise<SyncResponseMessage>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(requestId);
        reject(new Error(`Sync timeout with peer ${peerId}`));
      }, 15_000);

      this.pendingRequests.set(requestId, (msg) => {
        clearTimeout(timeout);
        resolve(msg);
      });

      this.transport.send(peerId, { ...request, requestId } as SyncMessage);
    });

    await this._applyRemoteChanges(response.docs);
    await this._setLastSyncHLC(peerId, this.adapter.getHLC().now());

    // Send back any changes the peer is missing
    const myChanges = await this.adapter.changes(since);
    if (myChanges.length > 0) {
      const myDocs = await _fetchDocsForChanges(this.adapter, myChanges);
      const reply: SyncResponseMessage = {
        type: 'sync-response',
        changes: myChanges,
        docs: myDocs,
        fromNodeId: nodeId,
        requestId: uuidv4(),
      };
      this.transport.send(peerId, reply);
    }
  }

  private async _handleMessage(peerId: string, msg: SyncMessage): Promise<void> {
    if (msg.type === 'sync-request') {
      await this._handleSyncRequest(peerId, msg);
    } else if (msg.type === 'sync-response') {
      const handler = this.pendingRequests.get(msg.requestId);
      if (handler) {
        this.pendingRequests.delete(msg.requestId);
        handler(msg);
      } else {
        // Unsolicited push from a peer's broadcastDoc
        await this._applyRemoteChanges(msg.docs);
      }
    } else if (msg.type === 'peer-hello') {
      await this._handlePeerHello(peerId, msg);
    } else if (msg.type === 'snapshot-request') {
      await sendSnapshotTo(this.transport, this.adapter, peerId, msg.requestId);
    }
  }

  private async _handleSyncRequest(
    peerId: string,
    msg: SyncRequestMessage & { requestId?: string },
  ): Promise<void> {
    const since: HLCTimestamp = msg.since;
    const changes = await this.adapter.changes(since);
    const docs = await _fetchDocsForChanges(this.adapter, changes);

    const response: SyncResponseMessage = {
      type: 'sync-response',
      changes,
      docs,
      fromNodeId: this.adapter.nodeId,
      requestId: msg.requestId ?? uuidv4(),
    };
    this.transport.send(peerId, response);
  }

  private async _applyRemoteChanges(docs: Doc[]): Promise<void> {
    const hlc = this.adapter.getHLC();

    for (const remote of docs) {
      hlc.update(remote._updatedAt);
      const collection = remote['_collection'] as string | undefined;
      if (!collection || !this.collections[collection]) continue;

      const local = await this.adapter.get(collection, remote._id);
      let resolved: Doc;

      if (!local) {
        resolved = remote;
      } else if (local._rev === remote._rev) {
        continue;
      } else {
        const strategy = this.collections[collection]?.conflictStrategy ?? 'lww';
        resolved = resolveConflict(local, remote, strategy);
        if (resolved !== local) {
          await logConflict(this.adapter, collection, local, remote, resolved);
        }
      }

      if (resolved !== local) {
        await this.adapter.bulkInsert(collection, [resolved]);
      }
    }
  }

  private async _getLastSyncHLC(peerId: string): Promise<HLCTimestamp> {
    const val = await this.adapter.getMetaValue(`${META_PEER_SYNC_PREFIX}${peerId}`);
    return (val as HLCTimestamp | undefined) ?? ('' as HLCTimestamp);
  }

  private async _setLastSyncHLC(peerId: string, hlc: HLCTimestamp): Promise<void> {
    await this.adapter.setMetaValue(`${META_PEER_SYNC_PREFIX}${peerId}`, hlc);
  }
}

function _pickRandom<T>(arr: T[], k: number): T[] {
  const copy = arr.slice();
  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy.slice(0, k);
}

async function _fetchDocsForChanges(
  adapter: IStorageAdapter,
  changes: Array<{ id: string; collection: string }>,
): Promise<Doc[]> {
  const docs: Doc[] = [];
  for (const entry of changes) {
    const doc = await adapter.get(entry.collection, entry.id);
    if (doc) {
      docs.push({ ...doc, _collection: entry.collection } as Doc & { _collection: string });
    }
  }
  return docs;
}
