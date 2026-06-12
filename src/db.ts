import { IndexedDBAdapter, type BatchOp } from './storage/indexeddb.js';
import { WebRTCTransport } from './sync/webrtc-transport.js';
import { GossipSync } from './sync/gossip.js';
import { RpcServer } from './rpc/server.js';
import type { RpcRouter } from './rpc/router.js';
import type { TokenVerifier, RpcServerConfig } from './rpc/server.js';
import type {
  DBConfig,
  Doc,
  HLCTimestamp,
  QueryOptions,
  ScanOptions,
  Snapshot,
  SnapshotChunk,
  ChangeEntry,
  CollectionSchema,
} from './core/types.js';

export type SyncStatus = 'online' | 'offline' | 'syncing';

/**
 * Turns a Normal Client into an RPC server for Consumer Clients. `router` holds the
 * read/write/stream handlers consumers may call. Requires `sync` (consumers connect over the
 * same WebRTC transport); ignored if `sync` is absent.
 */
export interface RpcConfig {
  router: RpcRouter;
  /**
   * Verifies a Consumer's token → identity (e.g. from `createTokenVerifier`). If omitted, every
   * connection is trusted as `{ id: consumerId, scopes: [] }` — DEV ONLY; handlers with declared
   * `scopes` will be denied since a trusted stub identity carries none.
   */
  verifyToken?: TokenVerifier;
  /** TTL (ms) for the write idempotency dedupe cache. Default 10 min. */
  idempotencyTtlMs?: number;
  /** Max time (ms) to wait for the local replica to catch up to a request's `readAfter`
   *  watermark before replying UNAVAILABLE. Default 2000. */
  readAfterTimeoutMs?: number;
  /** Override server limits: `maxPayloadBytes`, `defaultDeadlineMs`, `rateLimit.perMin`,
   *  `maxInflight`. */
  limits?: RpcServerConfig['limits'];
  /** Observability hook — invoked once per Consumer call (metrics/audit). */
  onCall?: RpcServerConfig['onCall'];
}

type ChangeCallback = (changes: ChangeEntry[]) => void;

export interface CollectionProxy<T extends Record<string, unknown> = Record<string, unknown>> {
  put(doc: T & { _id?: string }): Promise<Doc<T>>;
  get(id: string): Promise<Doc<T> | null>;
  query(options?: QueryOptions<T>): Promise<Doc<T>[]>;
  /**
   * Streaming, bounded-memory iteration in `_id` order. Use for large scans
   * where `query()` would materialize the whole collection.
   * `for await (const doc of db.todos.scan({ where: { status: 'open' } })) { … }`
   */
  scan(options?: ScanOptions<T>): AsyncGenerator<Doc<T>>;
  delete(id: string): Promise<void>;
  onChange(cb: ChangeCallback): () => void;
}

/** Staging surface inside `db.transaction(fn)` — collects writes, commits nothing itself. */
export interface TxCollectionProxy<T extends Record<string, unknown> = Record<string, unknown>> {
  /** Stage an upsert. System fields are stamped at commit. */
  put(doc: T & { _id?: string }): void;
  /** Stage a soft-delete. A missing id is a committed no-op (same as `delete()`). */
  delete(id: string): void;
}

/** The collections map handed to a `db.transaction(fn)` callback. */
export type TransactionProxy<C extends Record<string, CollectionSchema>> = {
  [K in keyof C]: TxCollectionProxy;
};

/** Base methods available on every DB instance. */
export interface DBBase {
  export(): Promise<Snapshot>;
  import(snapshot: Snapshot): Promise<void>;
  /**
   * Streaming export — yields a header then bounded doc batches. Peak memory is
   * one batch, so it scales to multi-GB stores where `export()` would OOM.
   * Pair with `snapshotChunksToNdjsonStream` to back up to a file/Drive.
   */
  exportStream(batchSize?: number): AsyncGenerator<SnapshotChunk>;
  /** Streaming restore — consumes chunks from `exportStream` (or NDJSON). */
  importStream(chunks: AsyncIterable<SnapshotChunk>): Promise<void>;
  readonly syncStatus: SyncStatus;
  close(): Promise<void>;
}

/**
 * A typed DB where C is the collections config map.
 * Each key of C becomes a CollectionProxy property.
 */
export type TypedDB<C extends Record<string, CollectionSchema>> = DBBase & {
  [K in keyof C]: CollectionProxy;
} & {
  /**
   * Atomic write batch. `fn` runs synchronously and only STAGES puts/deletes
   * (no reads — read before, write inside); everything staged then commits in
   * ONE IndexedDB transaction, together with the change-log entries and HLC
   * watermark. All-or-nothing: if any write fails, nothing is persisted, no
   * change events fire, nothing is broadcast. Resolves with the committed docs
   * in op order (tombstones included; no-op deletes omitted).
   *
   * Atomicity is LOCAL. Peers receive the batch as one frame per collection and
   * bulk-apply it, but conflict resolution stays per-doc — a concurrent remote
   * edit can win LWW for one doc of the batch and lose for another.
   *
   *   await db.transaction((tx) => {
   *     tx.orders.put({ _id: orderId, status: 'placed' });
   *     tx.orderItems.put({ orderId, sku: 'a' });
   *     tx.carts.delete(cartId);
   *   });
   */
  transaction(fn: (tx: TransactionProxy<C>) => void): Promise<Doc[]>;
};

function makeCollectionProxy<T extends Record<string, unknown>>(
  name: string,
  adapter: IndexedDBAdapter,
  listeners: Map<string, Set<ChangeCallback>>,
): CollectionProxy<T> {
  return {
    async put(doc) {
      return adapter.put(name, doc as Record<string, unknown>) as Promise<Doc<T>>;
    },
    async get(id) {
      return adapter.get(name, id) as Promise<Doc<T> | null>;
    },
    async query(options) {
      return adapter.query(name, options as QueryOptions) as Promise<Doc<T>[]>;
    },
    scan(options) {
      return adapter.scan(name, options as ScanOptions) as AsyncGenerator<Doc<T>>;
    },
    async delete(id) {
      return adapter.delete(name, id);
    },
    onChange(cb) {
      if (!listeners.has(name)) listeners.set(name, new Set());
      listeners.get(name)!.add(cb);
      return () => listeners.get(name)?.delete(cb);
    },
  };
}

/**
 * Open DBs are deduped by `name`. Concurrent or repeated calls to `createDB`
 * with the same name return the same in-flight promise — preventing the
 * "two transports, same nodeId" reconnect loop under React StrictMode and
 * other double-invocation patterns. The registry entry is cleared when the
 * DB is closed or when initial open rejects, so close-then-reopen works.
 */
const _openDbs = new Map<string, Promise<unknown>>();

export function createDB<C extends Record<string, CollectionSchema>>(
  config: DBConfig & { collections: C; rpc?: RpcConfig },
): Promise<TypedDB<C>> {
  const existing = _openDbs.get(config.name);
  if (existing) return existing as Promise<TypedDB<C>>;
  const promise = _createDB(config);
  _openDbs.set(config.name, promise);
  promise.catch(() => { _openDbs.delete(config.name); });
  return promise;
}

async function _createDB<C extends Record<string, CollectionSchema>>(
  config: DBConfig & { collections: C; rpc?: RpcConfig },
): Promise<TypedDB<C>> {
  const adapter = new IndexedDBAdapter(config.name, config.version, config.collections);
  await adapter.open();

  const changeListeners = new Map<string, Set<ChangeCallback>>();

  adapter.onChangeBatch((collection, entries, docs) => {
    const cbs = changeListeners.get(collection);
    if (cbs && cbs.size > 0) {
      for (const cb of cbs) cb(entries);
    }
    // The docs are already in hand from the write — broadcast them directly
    // instead of re-reading from IndexedDB, and as ONE message per committed
    // batch (a 1000-doc bulkInsert is 1 frame per peer, not 1000).
    if (gossip) gossip.broadcastDocs(collection, docs);
  });

  let transport: WebRTCTransport | null = null;
  let gossip: GossipSync | null = null;
  let syncStatus: SyncStatus = 'offline';
  let syncSettleTimer: ReturnType<typeof setTimeout> | null = null;
  let onWindowOnline: (() => void) | null = null;
  let onWindowOffline: (() => void) | null = null;

  if (config.sync) {
    const nodeId = adapter.nodeId;
    transport = new WebRTCTransport({
      signalingServerUrl: config.sync.signalingServer,
      iceServers: config.sync.iceServers,
      nodeId: config.sync.nodeId ?? nodeId,
      room: config.name,
      serveConsumers: config.sync.serveConsumers,
    });
    // Consumer ('rpc') connections are isolated from gossip by the transport. The RPC
    // server that handles transport.onConsumer{Connected,Disconnected,Message} is wired
    // in Phase 2; until then a connecting consumer simply has no handlers to call.

    gossip = new GossipSync(
      transport,
      adapter,
      config.collections as Record<string, CollectionSchema>,
      config.sync.changeLogTtlDays,
      config.sync.maxClockDriftMs,
    );

    const SYNC_SETTLE_MS = 1500;

    transport.onPeerConnected = () => {
      syncStatus = 'syncing';
      if (syncSettleTimer) clearTimeout(syncSettleTimer);
      syncSettleTimer = setTimeout(() => { syncStatus = 'online'; }, SYNC_SETTLE_MS);
    };

    transport.onPeerDisconnected = () => {
      if (transport!.peers().length === 0) {
        syncStatus = typeof navigator !== 'undefined' && !navigator.onLine ? 'offline' : 'online';
      }
    };

    if (config.sync.initialSnapshot !== undefined) {
      try {
        const snapshot = await Promise.resolve(config.sync.initialSnapshot);
        if (snapshot) {
          // Existence check only — limit 1 instead of materializing the whole log.
          const existing = await adapter.changes('0' as HLCTimestamp, 1);
          if (existing.length === 0) await adapter.import(snapshot);
        }
      } catch {
        // Drive error — connect with empty DB, bootstrap from best peer (Case 2)
      }
    }

    transport.connect();
    gossip.start();

    // Serve Consumer Clients: route the transport's isolated 'rpc' connections to the RPC
    // server. Consumers never enter gossip (Phase 1), so this only ever sees consumer frames.
    if (config.rpc) {
      const rpcServer = new RpcServer({
        router: config.rpc.router,
        adapter,
        hlc: () => adapter.getHLC().now(),
        nodeId: config.sync.nodeId ?? nodeId,
        send: (id, frame) => transport!.sendToConsumer(id, frame),
        sendAsync: (id, frame) => transport!.sendToConsumerAsync(id, frame),
        verifyToken: config.rpc.verifyToken,
        idempotencyTtlMs: config.rpc.idempotencyTtlMs,
        readAfterTimeoutMs: config.rpc.readAfterTimeoutMs,
        limits: config.rpc.limits,
        onCall: config.rpc.onCall,
      });
      transport.onConsumerConnected = (id) => rpcServer.onConnect(id);
      transport.onConsumerDisconnected = (id) => rpcServer.onDisconnect(id);
      transport.onConsumerMessage = (id, data) => { void rpcServer.handleMessage(id, data); };
    }

    if (typeof window !== 'undefined') {
      syncStatus = navigator.onLine ? 'online' : 'offline';
      onWindowOnline = () => { syncStatus = 'online'; };
      onWindowOffline = () => { syncStatus = 'offline'; };
      window.addEventListener('online', onWindowOnline);
      window.addEventListener('offline', onWindowOffline);
    }
  }

  const collectionProxies: Record<string, CollectionProxy> = {};
  for (const name of Object.keys(config.collections)) {
    collectionProxies[name] = makeCollectionProxy(name, adapter, changeListeners);
  }

  const db = {
    ...collectionProxies,

    async export(): Promise<Snapshot> {
      return adapter.export();
    },

    async import(snapshot: Snapshot): Promise<void> {
      await adapter.import(snapshot);
    },

    exportStream(batchSize?: number): AsyncGenerator<SnapshotChunk> {
      return adapter.exportStream(batchSize);
    },

    async importStream(chunks: AsyncIterable<SnapshotChunk>): Promise<void> {
      await adapter.importStream(chunks);
    },

    async transaction(fn: (tx: TransactionProxy<C>) => void): Promise<Doc[]> {
      // Staging is synchronous and pure JS — the IndexedDB transaction is opened
      // only afterwards, inside applyBatch, so it can never auto-commit early
      // under a user `await` (the classic IndexedDB transaction footgun).
      const ops: BatchOp[] = [];
      const staging: Record<string, TxCollectionProxy> = {};
      for (const name of Object.keys(config.collections)) {
        staging[name] = {
          put(doc) {
            ops.push({ type: 'put', collection: name, doc });
          },
          delete(id) {
            ops.push({ type: 'delete', collection: name, id });
          },
        };
      }
      fn(staging as TransactionProxy<C>);
      return adapter.applyBatch(ops);
    },

    get syncStatus(): SyncStatus {
      return syncStatus;
    },

    async close(): Promise<void> {
      _openDbs.delete(config.name);
      if (syncSettleTimer) clearTimeout(syncSettleTimer);
      if (typeof window !== 'undefined') {
        if (onWindowOnline) window.removeEventListener('online', onWindowOnline);
        if (onWindowOffline) window.removeEventListener('offline', onWindowOffline);
      }
      gossip?.stop();
      transport?.disconnect();
      await adapter.close();
    },
  } as TypedDB<C>;

  return db;
}
