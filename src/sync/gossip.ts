import { v4 as uuidv4 } from 'uuid';
import { isValidHLC, parseHLC } from '../core/hlc.js';
import { resolveConflict, logConflict } from '../core/conflict.js';
import {
  requestSnapshot,
  sendSnapshotTo,
  handleSnapshotStreamStart,
  handleSnapshotStreamBatch,
  handleSnapshotStreamEnd,
  cancelActiveStreams,
} from './snapshot.js';
import type {
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
/** Default bound on how far ahead of the local clock a remote HLC may be (24 h). */
export const DEFAULT_MAX_CLOCK_DRIFT_MS = 24 * 60 * 60 * 1000;
/** Defensive cap on `_id` length for docs accepted from the network. */
const MAX_REMOTE_ID_LENGTH = 1024;
// Wait this long after first peer-hello before picking the "best" peer for bootstrap,
// so we have time to collect hellos from all initially-connected peers.
const BOOTSTRAP_DELAY_MS = 300;

export class GossipSync {
  private transport: WebRTCTransport;
  private adapter: IndexedDBAdapter;
  private collections: Record<string, CollectionSchema>;
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private pruneIntervalId: ReturnType<typeof setInterval> | null = null;
  private pendingRequests = new Map<string, (msg: SyncResponseMessage) => void>();
  private onlineListener: (() => void) | null = null;
  private readonly ttlMs: number;
  private stopped = false;
  /** peers with an in-flight _syncWithPeer, to avoid overlapping duplicate syncs. */
  private readonly _syncing = new Set<string>();

  // Bootstrap state
  private peerHLCs = new Map<string, HLCTimestamp>();
  private bootstrapped = false;
  private bootstrapping = false;
  private bootstrapTimer: ReturnType<typeof setTimeout> | null = null;
  // Fix A: broadcasts that arrived while snapshot was being imported
  private pendingBroadcasts: Doc[][] = [];

  private readonly maxClockDriftMs: number;

  constructor(
    transport: WebRTCTransport,
    adapter: IndexedDBAdapter,
    collections: Record<string, CollectionSchema>,
    ttlDays = 30,
    maxClockDriftMs = DEFAULT_MAX_CLOCK_DRIFT_MS,
  ) {
    this.transport = transport;
    this.adapter = adapter;
    this.collections = collections;
    this.ttlMs = ttlDays > 0 ? ttlDays * 24 * 60 * 60 * 1000 : 0;
    this.maxClockDriftMs = maxClockDriftMs;
  }

  /** True iff `ts` is a well-formed HLC whose physical time is not unacceptably far
   *  in the future. Every network-received timestamp must pass before touching the
   *  local clock — see SyncConfig.maxClockDriftMs for the threat model. */
  private _acceptableHlc(ts: unknown): ts is HLCTimestamp {
    if (!isValidHLC(ts)) return false;
    return parseHLC(ts).physicalMs <= Date.now() + this.maxClockDriftMs;
  }

  start(): void {
    this.transport.onMessage = this._handleMessage.bind(this);

    // Chain onto whatever db.ts wired (e.g. syncStatus update)
    const upstreamConnected = this.transport.onPeerConnected;
    this.transport.onPeerConnected = (peerId) => {
      upstreamConnected(peerId);
      this._onPeerConnected(peerId);
    };

    // Forget a peer's advertised HLC when it disconnects so the map doesn't grow
    // across a churny session and stale entries can't influence best-peer choice.
    const upstreamDisconnected = this.transport.onPeerDisconnected;
    this.transport.onPeerDisconnected = (peerId) => {
      upstreamDisconnected(peerId);
      this.peerHLCs.delete(peerId);
    };

    this.intervalId = setInterval(() => void this._gossipRound(), GOSSIP_INTERVAL_MS);

    if (typeof window !== 'undefined') {
      this.onlineListener = () => void this._onOnline();
      window.addEventListener('online', this.onlineListener);
    }

    // Initial gossip after connections settle
    setTimeout(() => void this._gossipRound(), 1000);

    if (this.ttlMs > 0) {
      const prune = (): void => {
        void this.adapter.pruneChanges(this.ttlMs);
        // The conflict audit log shares the TTL — without pruning it grows forever.
        void this.adapter.pruneConflicts(this.ttlMs);
      };
      prune();
      this.pruneIntervalId = setInterval(prune, 24 * 60 * 60 * 1000);
    }
  }

  stop(): void {
    this.stopped = true;
    if (this.intervalId) clearInterval(this.intervalId);
    if (this.pruneIntervalId) clearInterval(this.pruneIntervalId);
    if (this.bootstrapTimer) clearTimeout(this.bootstrapTimer);
    if (this.onlineListener && typeof window !== 'undefined') {
      window.removeEventListener('online', this.onlineListener);
    }
    // Reject any in-flight snapshot receives for this adapter so a teardown can't
    // leave a 120s timer running or a hanging requestSnapshot promise.
    cancelActiveStreams(this.adapter);
    this.peerHLCs.clear();
    this.pendingBroadcasts = [];
  }

  /** Push a single doc to all connected peers immediately (no round-trip). */
  broadcastDoc(collection: string, doc: Doc): void {
    this.broadcastDocs(collection, [doc]);
  }

  /**
   * Push a batch of docs (one collection) to all connected peers as ONE message.
   * Applying a 500-doc sync page used to re-broadcast 500 individual messages
   * per peer; batching keeps the flooding semantics at 1/500th the frames.
   */
  broadcastDocs(collection: string, docs: Doc[]): void {
    if (docs.length === 0) return;
    const peers = this.transport.peers();
    if (peers.length === 0) return;

    const message: SyncResponseMessage = {
      type: 'sync-response',
      changes: docs.map((doc) => ({
        id: doc._id,
        collection,
        _rev: doc._rev,
        _updatedAt: doc._updatedAt,
        operation: doc._deleted ? ('delete' as const) : ('put' as const),
        origin: 'local' as const,
      })),
      docs: docs.map((doc) => ({ ...doc, _collection: collection }) as Doc & { _collection: string }),
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
    // A malformed or far-future HLC must not enter best-peer selection (it would
    // make a poisoned peer the bootstrap source) nor the post-bootstrap pull check.
    if (!this._acceptableHlc(msg.currentHLC)) return;
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
    this.bootstrapTimer = setTimeout(() => void this._maybeBootstrap().catch(() => undefined), BOOTSTRAP_DELAY_MS);
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
    if (this.bootstrapped || this.bootstrapping || this.stopped) return;
    this.bootstrapping = true;

    try {
      const isEmpty = await this._isDBEmpty();

      if (isEmpty) {
        const bestPeer = this._findBestPeer();
        if (!bestPeer) {
          // No peers with known HLC yet — re-arm the timer so we retry when more arrive
          this.bootstrapTimer = setTimeout(() => void this._maybeBootstrap().catch(() => undefined), BOOTSTRAP_DELAY_MS);
          return;
        }

        try {
          await requestSnapshot(this.transport, bestPeer, this.adapter, this.maxClockDriftMs);
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
    // One readonly transaction with count() (no doc deserialization), vs one
    // transaction + one cursor materialization per collection.
    return this.adapter.areStoresEmpty(Object.keys(this.collections));
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
    // Skip if a sync with this peer is already in flight — overlapping gossip
    // rounds, the initial round, _onOnline and post-bootstrap hellos can all
    // target the same peer and would otherwise duplicate the whole phase-2 push.
    if (this._syncing.has(peerId)) return;
    this._syncing.add(peerId);
    try {
      const since = await this._getLastSyncHLC(peerId);
      const nodeId = this.adapter.nodeId;

      // Phase 1: paginated pull from peer
      let cursor: HLCTimestamp | undefined;
      let hasMore = true;
      while (hasMore && !this.stopped) {
        const response = await this._requestPage(peerId, since, cursor, uuidv4());

        if (response.needsFullSync) {
          // Our watermark predates the peer's oldest change entry — re-bootstrap
          // from a snapshot. Only advance the watermark if the snapshot actually
          // succeeds; otherwise the un-streamed changes would be skipped forever.
          try {
            await requestSnapshot(this.transport, peerId, this.adapter, this.maxClockDriftMs);
            await this._setLastSyncHLC(peerId, this.adapter.getHLC().now());
          } catch {
            // leave watermark; next round re-detects needsFullSync and retries
          }
          return;
        }

        await this._applyRemoteChanges(response.docs);
        cursor = response.nextCursor;
        hasMore = response.hasMore ?? false;
      }
      if (this.stopped) return;

      await this._setLastSyncHLC(peerId, this.adapter.getHLC().now());

      // Phase 2: paged push — page the change log by keyset (each page is one
      // bounded read) instead of materializing the entire change set at once.
      let pushCursor: HLCTimestamp = since;
      for (;;) {
        if (this.stopped) return;
        const fetched = await this.adapter.changes(pushCursor, SYNC_PAGE_SIZE + 1);
        if (fetched.length === 0) break;
        const isLast = fetched.length <= SYNC_PAGE_SIZE;
        const pageChanges = isLast ? fetched : fetched.slice(0, SYNC_PAGE_SIZE);
        const pageDocs = await _fetchDocsForChanges(this.adapter, pageChanges);
        await this.transport.sendAsync(peerId, {
          type: 'sync-response',
          changes: pageChanges,
          docs: pageDocs,
          fromNodeId: nodeId,
          requestId: uuidv4(),
          hasMore: !isLast,
        });
        pushCursor = pageChanges[pageChanges.length - 1]._updatedAt;
        if (isLast) break;
      }
    } finally {
      this._syncing.delete(peerId);
    }
  }

  private async _handleMessage(peerId: string, msg: SyncMessage): Promise<void> {
    if (this.stopped) return;
    // This runs fire-and-forget from the transport. Catch here so a failed apply
    // (quota, tx abort, closed DB) can't surface as an unhandled rejection or
    // silently drop the rest of the dispatch. A dropped message is re-pulled next
    // gossip round (lastSync only advances on success), so no writes are lost.
    try {
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
    } catch (err) {
      queueMicrotask(() => {
        throw err;
      });
    }
  }

  private async _handleSyncRequest(
    peerId: string,
    msg: SyncRequestMessage,
  ): Promise<void> {
    const since: HLCTimestamp = msg.cursor ?? msg.since;

    if (this.ttlMs > 0) {
      const oldest = await this.adapter.getOldestChangesHlc();
      if (oldest !== null && since < oldest) {
        this.transport.send(peerId, {
          type: 'sync-response',
          changes: [],
          docs: [],
          fromNodeId: this.adapter.nodeId,
          requestId: msg.requestId,
          needsFullSync: true,
        });
        return;
      }
    }

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
    const before = hlc.now();

    // Validate, advance the HLC for every accepted remote doc (in order), and
    // group docs by collection. Validation comes FIRST: a malformed `_rev` /
    // `_updatedAt` would corrupt the clock (NaN), and a far-future timestamp
    // would permanently poison LWW for the whole replica — reject both, along
    // with docs missing a usable string `_id`. Unknown collections still advance
    // the HLC (they are well-formed data we simply don't store).
    const byCollection = new Map<string, Doc[]>();
    for (const remote of docs) {
      if (remote === null || typeof remote !== 'object') continue;
      if (typeof remote._id !== 'string' || remote._id.length === 0 || remote._id.length > MAX_REMOTE_ID_LENGTH) continue;
      if (!this._acceptableHlc(remote._updatedAt) || !this._acceptableHlc(remote._rev)) continue;
      hlc.update(remote._updatedAt);
      const collection = remote['_collection'] as string | undefined;
      if (!collection || !this.collections[collection]) continue;
      let arr = byCollection.get(collection);
      if (!arr) byCollection.set(collection, (arr = []));
      arr.push(remote);
    }

    for (const [collection, remotes] of byCollection) {
      // Dedupe by _id keeping the last occurrence. Within a single batch the
      // duplicates carry the same current doc, so this matches the old
      // "apply once, later identical entries are no-ops" behaviour while
      // emitting exactly one change/broadcast per id.
      const lastById = new Map<string, Doc>();
      for (const r of remotes) lastById.set(r._id, r);
      const unique = Array.from(lastById.values());

      // Prefetch all local docs for this collection in ONE readonly transaction,
      // then resolve in memory and write the survivors in ONE bulkInsert — instead
      // of a get + single-doc bulkInsert (two transactions) per remote doc.
      const locals = await this.adapter.getMany(collection, unique.map((r) => r._id));
      const strategy = this.collections[collection]?.conflictStrategy ?? 'lww';
      const toWrite: Doc[] = [];

      for (let i = 0; i < unique.length; i++) {
        const remote = unique[i];
        const local = locals[i];
        let resolved: Doc;

        if (!local) {
          resolved = remote;
        } else if (local._rev === remote._rev) {
          continue;
        } else {
          resolved = resolveConflict(local, remote, strategy);
          if (resolved !== local) {
            await logConflict(this.adapter, collection, local, remote, resolved);
          }
        }

        if (resolved !== local) {
          // Strip the transport-only `_collection` routing tag so it is never
          // persisted into the user's object store (it isn't part of SystemFields).
          const { _collection, ...clean } = resolved as Doc & { _collection?: string };
          void _collection;
          toWrite.push(clean as Doc);
        }
      }

      if (toWrite.length > 0) await this.adapter.bulkInsert(collection, toWrite);
    }

    // Persist the HLC watermark if a remote advanced it. put()/delete() persist on
    // local writes, but bulkInsert does not — without this, a reload before the
    // next local write would restore a stale watermark and a later local edit could
    // mint a _rev ≤ an already-stored remote rev and silently lose under LWW.
    if (hlc.now() !== before) await this.adapter.persistHLC();
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
  adapter: IndexedDBAdapter,
  changes: Array<{ id: string; collection: string }>,
): Promise<Doc[]> {
  // One readonly transaction for the whole page instead of N sequential gets.
  return adapter.getManyForChanges(changes);
}
