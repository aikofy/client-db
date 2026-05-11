import { v4 as uuidv4 } from 'uuid';
import { resolveConflict, logConflict } from '../core/conflict.js';
import {
  requestSnapshot,
  sendSnapshotTo,
  handleSnapshotStreamStart,
  handleSnapshotStreamBatch,
  handleSnapshotStreamEnd,
} from './snapshot.js';
import type {
  IStorageAdapter,
  HLCTimestamp,
  Doc,
  SyncRequestMessage,
  SyncResponseMessage,
  PeerHelloMessage,
  SyncMessage,
  CollectionSchema,
  SnapshotStreamStartMessage,
  SnapshotStreamBatchMessage,
  SnapshotStreamEndMessage,
} from '../core/types.js';
import type { WebRTCTransport } from './webrtc-transport.js';
import type { IndexedDBAdapter } from '../storage/indexeddb.js';

const GOSSIP_INTERVAL_MS = 30_000;
const FANOUT = 3;
const META_PEER_SYNC_PREFIX = 'lastSync:';
const SYNC_PAGE_SIZE = 500;
// Wait this long after first peer-hello before picking the "best" peer for bootstrap,
// so we have time to collect hellos from all initially-connected peers.
const BOOTSTRAP_DELAY_MS = 300;

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
  private bootstrapTimer: ReturnType<typeof setTimeout> | null = null;
  // Fix A: broadcasts that arrived while snapshot was being imported
  private pendingBroadcasts: Doc[][] = [];

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
    if (this.bootstrapTimer) clearTimeout(this.bootstrapTimer);
    if (this.onlineListener && typeof window !== 'undefined') {
      window.removeEventListener('online', this.onlineListener);
    }
    this.pendingBroadcasts = [];
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

    if (this.bootstrapped) {
      // Fix E: post-bootstrap peer arrived — sync if they have data we don't
      const myHLC = this.adapter.getHLC().now();
      if (msg.currentHLC > myHLC) {
        await this._syncWithPeer(peerId).catch(() => undefined);
      }
      // If their HLC ≤ ours they will bootstrap from us via their own _maybeBootstrap
      return;
    }

    if (this.bootstrapping) return;

    // Fix B: debounce — collect peer-hellos for BOOTSTRAP_DELAY_MS before picking the best peer
    if (this.bootstrapTimer) clearTimeout(this.bootstrapTimer);
    this.bootstrapTimer = setTimeout(() => void this._maybeBootstrap(), BOOTSTRAP_DELAY_MS);
  }

  /**
   * Runs once per session on first peer contact. Three cases:
   *
   * 1. Empty DB — fetch full snapshot from the peer with the highest HLC,
   *    then delta-sync with all remaining peers to catch anything they had.
   *
   * 2. Non-empty DB, never peer-synced — pre-loaded snapshot or freshly
   *    created node: sync with ALL peers for bidirectional delta coverage.
   *
   * 3. Returning client — same as (2) for the initial bootstrap; subsequent
   *    reconnects are handled by _onOnline().
   */
  private async _maybeBootstrap(): Promise<void> {
    if (this.bootstrapped || this.bootstrapping) return;
    this.bootstrapping = true;

    try {
      const isEmpty = await this._isDBEmpty();

      if (isEmpty) {
        const bestPeer = this._findBestPeer();
        if (!bestPeer) {
          // No peers with known HLC yet — re-arm the timer so we retry when more arrive
          this.bootstrapTimer = setTimeout(() => void this._maybeBootstrap(), BOOTSTRAP_DELAY_MS);
          return;
        }

        try {
          await requestSnapshot(this.transport, bestPeer, this.adapter);
        } catch {
          // snapshot failed — fall back to full delta from that peer
          await this._syncWithPeer(bestPeer).catch(() => undefined);
        }

        // Catch any changes the best peer didn't have
        const others = this.transport.peers().filter((p) => p !== bestPeer);
        await Promise.allSettled(others.map((p) => this._syncWithPeer(p)));
      } else {
        // Pre-loaded snapshot (case 1) or returning client (case 3):
        // sync all peers so no offline writes are missed
        await this._syncAllPeers();
      }

      // Fix A: drain broadcasts that arrived while the snapshot was being imported
      for (const docs of this.pendingBroadcasts) {
        await this._applyRemoteChanges(docs);
      }
      this.pendingBroadcasts = [];

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

  private async _requestPage(
    peerId: string,
    since: HLCTimestamp,
    cursor: HLCTimestamp | undefined,
    requestId: string,
  ): Promise<SyncResponseMessage> {
    return new Promise<SyncResponseMessage>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(requestId);
        reject(new Error(`Sync timeout with peer ${peerId}`));
      }, 15_000);

      this.pendingRequests.set(requestId, (msg) => {
        clearTimeout(timeout);
        resolve(msg);
      });

      const request: SyncRequestMessage = {
        type: 'sync-request',
        since,
        collections: Object.keys(this.collections),
        fromNodeId: this.adapter.nodeId,
        requestId,
        cursor,
        pageSize: SYNC_PAGE_SIZE,
      };
      this.transport.send(peerId, request);
    });
  }

  private async _syncWithPeer(peerId: string): Promise<void> {
    const since = await this._getLastSyncHLC(peerId);
    const nodeId = this.adapter.nodeId;

    // Phase 1: paginated pull from peer
    let cursor: HLCTimestamp | undefined;
    let hasMore = true;
    while (hasMore) {
      const response = await this._requestPage(peerId, since, cursor, uuidv4());
      await this._applyRemoteChanges(response.docs);
      cursor = response.nextCursor;
      hasMore = response.hasMore ?? false;
    }

    await this._setLastSyncHLC(peerId, this.adapter.getHLC().now());

    // Phase 2: paged push — send our changes to the peer in page-sized messages
    const myChanges = await this.adapter.changes(since);
    for (let i = 0; i < myChanges.length; i += SYNC_PAGE_SIZE) {
      const pageChanges = myChanges.slice(i, i + SYNC_PAGE_SIZE);
      const pageDocs = await _fetchDocsForChanges(this.adapter, pageChanges);
      const isLast = i + SYNC_PAGE_SIZE >= myChanges.length;
      await this.transport.sendAsync(peerId, {
        type: 'sync-response',
        changes: pageChanges,
        docs: pageDocs,
        fromNodeId: nodeId,
        requestId: uuidv4(),
        hasMore: !isLast,
      });
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
        // Fix A: unsolicited broadcast — buffer during bootstrap to avoid snapshot overwrite race
        if (this.bootstrapping) {
          this.pendingBroadcasts.push(msg.docs);
        } else {
          await this._applyRemoteChanges(msg.docs);
        }
      }
    } else if (msg.type === 'peer-hello') {
      await this._handlePeerHello(peerId, msg);
    } else if (msg.type === 'snapshot-request') {
      await sendSnapshotTo(this.transport, this.adapter, peerId, msg.requestId);
    } else if (msg.type === 'snapshot-stream-start') {
      await handleSnapshotStreamStart(this.adapter, msg as SnapshotStreamStartMessage);
    } else if (msg.type === 'snapshot-stream-batch') {
      await handleSnapshotStreamBatch(this.adapter, msg as SnapshotStreamBatchMessage);
    } else if (msg.type === 'snapshot-stream-end') {
      await handleSnapshotStreamEnd(this.adapter, msg as SnapshotStreamEndMessage);
    }
  }

  private async _handleSyncRequest(
    peerId: string,
    msg: SyncRequestMessage,
  ): Promise<void> {
    const since: HLCTimestamp = msg.cursor ?? msg.since;
    const pageSize = msg.pageSize ?? SYNC_PAGE_SIZE;

    // Fetch one extra to detect whether more pages exist
    const changes = await this.adapter.changes(since, pageSize + 1);
    const hasMore = changes.length > pageSize;
    const page = hasMore ? changes.slice(0, pageSize) : changes;
    const nextCursor = page.length > 0 ? page[page.length - 1]._updatedAt : since;

    const docs = await _fetchDocsForChanges(this.adapter, page);

    const response: SyncResponseMessage = {
      type: 'sync-response',
      changes: page,
      docs,
      fromNodeId: this.adapter.nodeId,
      requestId: msg.requestId,
      hasMore,
      nextCursor,
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
