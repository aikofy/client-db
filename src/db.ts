import { IndexedDBAdapter } from './storage/indexeddb.js';
import { WebRTCTransport } from './sync/webrtc-transport.js';
import { GossipSync } from './sync/gossip.js';
import { serveSnapshot } from './sync/snapshot.js';
import type {
  DBConfig,
  Doc,
  QueryOptions,
  Snapshot,
  ChangeEntry,
  CollectionSchema,
} from './core/types.js';

export type SyncStatus = 'online' | 'offline' | 'syncing';

type ChangeCallback = (changes: ChangeEntry[]) => void;

export interface CollectionProxy<T extends Record<string, unknown> = Record<string, unknown>> {
  put(doc: T & { _id?: string }): Promise<Doc<T>>;
  get(id: string): Promise<Doc<T> | null>;
  query(options?: QueryOptions<T>): Promise<Doc<T>[]>;
  delete(id: string): Promise<void>;
  onChange(cb: ChangeCallback): () => void;
}

/** Base methods available on every DB instance. */
export interface DBBase {
  export(): Promise<Snapshot>;
  import(snapshot: Snapshot): Promise<void>;
  readonly syncStatus: SyncStatus;
  close(): Promise<void>;
}

/**
 * A typed DB where C is the collections config map.
 * Each key of C becomes a CollectionProxy property.
 */
export type TypedDB<C extends Record<string, CollectionSchema>> = DBBase & {
  [K in keyof C]: CollectionProxy;
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

export async function createDB<C extends Record<string, CollectionSchema>>(
  config: DBConfig & { collections: C },
): Promise<TypedDB<C>> {
  const adapter = new IndexedDBAdapter(config.name, config.version, config.collections);
  await adapter.open();

  const changeListeners = new Map<string, Set<ChangeCallback>>();

  adapter.onChangeEntry((entry) => {
    const cbs = changeListeners.get(entry.collection);
    if (cbs && cbs.size > 0) {
      for (const cb of cbs) cb([entry]);
    }
    if (gossip) {
      void adapter.get(entry.collection, entry.id).then((doc) => {
        if (doc) gossip!.broadcastDoc(entry.collection, doc);
      });
    }
  });

  let transport: WebRTCTransport | null = null;
  let gossip: GossipSync | null = null;
  let syncStatus: SyncStatus = 'offline';

  if (config.sync) {
    const nodeId = adapter.nodeId;
    transport = new WebRTCTransport({
      signalingServerUrl: config.sync.signalingServer,
      iceServers: config.sync.iceServers,
      nodeId: config.sync.nodeId ?? nodeId,
    });

    gossip = new GossipSync(
      transport,
      adapter,
      config.collections as Record<string, CollectionSchema>,
    );

    transport.onPeerConnected = () => {
      syncStatus = 'syncing';
    };

    serveSnapshot(transport, adapter);
    transport.connect();
    gossip.start();

    if (typeof window !== 'undefined') {
      syncStatus = navigator.onLine ? 'online' : 'offline';
      window.addEventListener('online', () => { syncStatus = 'online'; });
      window.addEventListener('offline', () => { syncStatus = 'offline'; });
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

    get syncStatus(): SyncStatus {
      return syncStatus;
    },

    async close(): Promise<void> {
      gossip?.stop();
      transport?.disconnect();
      await adapter.close();
    },
  } as TypedDB<C>;

  return db;
}
