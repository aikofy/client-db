import { openDB, type IDBPDatabase } from 'idb';
import { v4 as uuidv4 } from 'uuid';
import { HLC } from '../core/hlc.js';
import { CHANGES_STORE } from '../core/change-log.js';
import { buildStoreDefinition } from './schema.js';
import type {
  IStorageAdapter,
  Doc,
  QueryOptions,
  ChangeEntry,
  HLCTimestamp,
  Snapshot,
  CollectionSchema,
} from '../core/types.js';

const META_STORE = '_meta';
const BULK_INSERT_BATCH_SIZE = 1000;
const CONFLICTS_STORE = '_conflicts';
const SYSTEM_STORES = [CHANGES_STORE, META_STORE, CONFLICTS_STORE];

export class IndexedDBAdapter implements IStorageAdapter {
  private db!: IDBPDatabase;
  private hlc!: HLC;
  private readonly dbName: string;
  readonly version: number;
  private readonly collections: Record<string, CollectionSchema>;
  private changeListeners: Array<(entry: ChangeEntry) => void> = [];

  constructor(
    dbName: string,
    version: number,
    collections: Record<string, CollectionSchema>,
  ) {
    this.dbName = dbName;
    this.version = version;
    this.collections = collections;
  }

  async open(): Promise<void> {
    const storeDefs = Object.entries(this.collections).map(([name, schema]) =>
      buildStoreDefinition(name, schema),
    );
    const conflictsStoreDef = buildStoreDefinition(CONFLICTS_STORE, {});

    this.db = await openDB(this.dbName, this.version, {
      upgrade(db, _oldVersion, _newVersion, tx) {
        // meta store
        if (!db.objectStoreNames.contains(META_STORE)) {
          db.createObjectStore(META_STORE, { keyPath: 'key' });
        }

        // changes store
        if (!db.objectStoreNames.contains(CHANGES_STORE)) {
          const changesStore = db.createObjectStore(CHANGES_STORE, { keyPath: '_rev' });
          changesStore.createIndex('_updatedAt', '_updatedAt');
          changesStore.createIndex('collection', 'collection');
        }

        // conflicts store
        if (!db.objectStoreNames.contains(CONFLICTS_STORE)) {
          const cs = db.createObjectStore(conflictsStoreDef.name, { keyPath: '_id' });
          cs.createIndex('_updatedAt', '_updatedAt');
          cs.createIndex('_deleted', '_deleted');
        }

        // user collections
        for (const def of storeDefs) {
          const store = db.objectStoreNames.contains(def.name)
            ? tx.objectStore(def.name)
            : db.createObjectStore(def.name, {
                keyPath: def.keyPath,
                autoIncrement: def.autoIncrement,
              });

          for (const idx of def.indexes) {
            if (!store.indexNames.contains(idx.name)) {
              store.createIndex(idx.name, idx.keyPath, { unique: idx.unique ?? false });
            }
          }
        }
      },
    });

    await this._initHLC();
  }

  private async _initHLC(): Promise<void> {
    const tx = this.db.transaction(META_STORE, 'readwrite');
    let nodeRecord = await tx.store.get('nodeId');
    if (!nodeRecord) {
      nodeRecord = { key: 'nodeId', value: uuidv4() };
      await tx.store.put(nodeRecord);
    }
    const hlcRecord = await tx.store.get('hlc');
    await tx.done;

    const nodeId: string = nodeRecord.value;
    const lastHLC: HLCTimestamp | undefined = hlcRecord?.value;
    this.hlc = new HLC(nodeId, lastHLC);
  }

  private async _persistHLC(): Promise<void> {
    await this.persistHLC();
  }

  async persistHLC(): Promise<void> {
    const tx = this.db.transaction(META_STORE, 'readwrite');
    await tx.store.put({ key: 'hlc', value: this.hlc.now() });
    await tx.done;
  }

  get nodeId(): string {
    return this.hlc.nodeId;
  }

  getHLC(): HLC {
    return this.hlc;
  }

  onChangeEntry(cb: (entry: ChangeEntry) => void): () => void {
    this.changeListeners.push(cb);
    return () => {
      this.changeListeners = this.changeListeners.filter((l) => l !== cb);
    };
  }

  private _emitChange(entry: ChangeEntry): void {
    for (const cb of this.changeListeners) cb(entry);
  }

  async put(collection: string, doc: Record<string, unknown>): Promise<Doc> {
    const rev = this.hlc.tick();
    const record: Doc = {
      ...doc,
      _id: (doc['_id'] as string) ?? uuidv4(),
      _rev: rev,
      _deleted: false,
      _updatedAt: rev,
    };

    const changeEntry: ChangeEntry = {
      id: record._id,
      collection,
      _rev: rev,
      _updatedAt: rev,
      operation: 'put',
      origin: 'local',
    };

    const tx = this.db.transaction([collection, CHANGES_STORE], 'readwrite');
    await tx.objectStore(collection).put(record);
    await tx.objectStore(CHANGES_STORE).put(changeEntry);
    await tx.done;

    await this._persistHLC();
    this._emitChange(changeEntry);
    return record;
  }

  async get(collection: string, id: string): Promise<Doc | null> {
    const record = await this.db.get(collection, id);
    return (record as Doc | undefined) ?? null;
  }

  private _matchesWhere(record: Doc, where?: QueryOptions['where']): boolean {
    if (!where) return true;
    for (const [k, v] of Object.entries(where)) {
      if (k === '_updatedAt') continue;
      if ((record as Record<string, unknown>)[k] !== v) return false;
    }
    return true;
  }

  private async _queryCursor(collection: string, options: QueryOptions): Promise<Doc[]> {
    const { where, limit, offset = 0, includeDeleted = false } = options;
    const tx = this.db.transaction(collection, 'readonly');
    const results: Doc[] = [];
    let skipped = 0;

    let cursor = await tx.objectStore(collection).openCursor();
    while (cursor) {
      const record = cursor.value as Doc;
      if ((includeDeleted || !record._deleted) && this._matchesWhere(record, where)) {
        if (skipped < offset) {
          skipped++;
        } else {
          results.push(record);
          if (results.length >= (limit ?? Infinity)) break;
        }
      }
      cursor = await cursor.continue();
    }
    await tx.done;
    return results;
  }

  async query(collection: string, options: QueryOptions = {}): Promise<Doc[]> {
    const {
      where,
      orderBy,
      orderDir = 'asc',
      limit,
      offset = 0,
      includeDeleted = false,
    } = options;

    // Fast path: early-exit cursor when limit is set and no sort needed
    if (limit !== undefined && !orderBy) {
      return this._queryCursor(collection, options);
    }

    let records: Doc[];

    // Use _updatedAt index if filtering by it
    if (where && '_updatedAt' in where && where['_updatedAt']) {
      const tx = this.db.transaction(collection, 'readonly');
      const index = tx.objectStore(collection).index('_updatedAt');
      const range = IDBKeyRange.lowerBound(where['_updatedAt'], true);
      records = (await index.getAll(range)) as Doc[];
      await tx.done;
    } else {
      records = (await this.db.getAll(collection)) as Doc[];
    }

    if (!includeDeleted) {
      records = records.filter((r) => !r._deleted);
    }

    if (where) {
      records = records.filter((r) => this._matchesWhere(r, where));
    }

    if (orderBy) {
      const key = orderBy as string;
      records.sort((a, b) => {
        const av = (a as Record<string, unknown>)[key] as string;
        const bv = (b as Record<string, unknown>)[key] as string;
        return orderDir === 'asc' ? (av < bv ? -1 : av > bv ? 1 : 0) : av > bv ? -1 : av < bv ? 1 : 0;
      });
    }

    return records.slice(offset, limit !== undefined ? offset + limit : undefined);
  }

  async delete(collection: string, id: string): Promise<void> {
    const existing = await this.get(collection, id);
    if (!existing) return;

    const rev = this.hlc.tick();
    const tombstone: Doc = { ...existing, _deleted: true, _rev: rev, _updatedAt: rev };

    const changeEntry: ChangeEntry = {
      id,
      collection,
      _rev: rev,
      _updatedAt: rev,
      operation: 'delete',
      origin: 'local',
    };

    const tx = this.db.transaction([collection, CHANGES_STORE], 'readwrite');
    await tx.objectStore(collection).put(tombstone);
    await tx.objectStore(CHANGES_STORE).put(changeEntry);
    await tx.done;

    await this._persistHLC();
    this._emitChange(changeEntry);
  }

  async bulkInsert(collection: string, docs: Doc[]): Promise<void> {
    if (docs.length === 0) return;
    for (let start = 0; start < docs.length; start += BULK_INSERT_BATCH_SIZE) {
      const batch = docs.slice(start, start + BULK_INSERT_BATCH_SIZE);
      const tx = this.db.transaction([collection, CHANGES_STORE], 'readwrite');
      const store = tx.objectStore(collection);
      const changesStore = tx.objectStore(CHANGES_STORE);
      await Promise.all(
        batch.map((d) => {
          const entry: ChangeEntry = {
            id: d._id,
            collection,
            _rev: d._rev,
            _updatedAt: d._updatedAt,
            operation: d._deleted ? 'delete' : 'put',
            origin: 'peer',
          };
          return Promise.all([store.put(d), changesStore.put(entry)]);
        }),
      );
      await tx.done;
      for (const doc of batch) {
        this._emitChange({
          id: doc._id,
          collection,
          _rev: doc._rev,
          _updatedAt: doc._updatedAt,
          operation: doc._deleted ? 'delete' : 'put',
          origin: 'peer',
        });
      }
    }
  }

  async changes(since: HLCTimestamp, limit?: number): Promise<ChangeEntry[]> {
    const tx = this.db.transaction(CHANGES_STORE, 'readonly');
    const index = tx.objectStore(CHANGES_STORE).index('_updatedAt');
    const range = IDBKeyRange.lowerBound(since, true);
    const results = limit !== undefined
      ? await index.getAll(range, limit)
      : await index.getAll(range);
    await tx.done;
    return results as ChangeEntry[];
  }

  async export(): Promise<Snapshot> {
    const collectionNames = await this.collectionNames();
    const collections: Record<string, Doc[]> = {};
    for (const name of collectionNames) {
      collections[name] = (await this.db.getAll(name)) as Doc[];
    }

    const tx = this.db.transaction(META_STORE, 'readonly');
    const allMeta = await tx.store.getAll();
    await tx.done;
    const meta: Record<string, unknown> = {};
    for (const record of allMeta) {
      meta[(record as { key: string; value: unknown }).key] =
        (record as { key: string; value: unknown }).value;
    }

    return {
      version: this.version,
      hlc: this.hlc.now(),
      collections,
      meta,
    };
  }

  async import(snapshot: Snapshot): Promise<void> {
    for (const [name, docs] of Object.entries(snapshot.collections)) {
      if (!SYSTEM_STORES.includes(name)) {
        await this.bulkInsert(name, docs);
      }
    }

    // Restore HLC watermark
    this.hlc.update(snapshot.hlc);
    await this._persistHLC();
  }

  async collectionNames(): Promise<string[]> {
    return Array.from(this.db.objectStoreNames).filter(
      (n) => !SYSTEM_STORES.includes(n),
    );
  }

  async close(): Promise<void> {
    this.db.close();
  }

  async getMetaValue(key: string): Promise<unknown> {
    const record = await this.db.get(META_STORE, key);
    return (record as { key: string; value: unknown } | undefined)?.value;
  }

  async setMetaValue(key: string, value: unknown): Promise<void> {
    const tx = this.db.transaction(META_STORE, 'readwrite');
    await tx.store.put({ key, value });
    await tx.done;
  }
}
