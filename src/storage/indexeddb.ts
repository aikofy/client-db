import { openDB, type IDBPDatabase } from 'idb';
import { v4 as uuidv4 } from 'uuid';
import { HLC, formatHLC } from '../core/hlc.js';
import { CHANGES_STORE } from '../core/change-log.js';
import { buildStoreDefinition } from './schema.js';
import type {
  IStorageAdapter,
  Doc,
  QueryOptions,
  ScanOptions,
  ChangeEntry,
  HLCTimestamp,
  Snapshot,
  SnapshotChunk,
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
  private changeListeners: Array<(entry: ChangeEntry, doc: Doc) => void> = [];
  /** collection → set of single-field index keyPaths usable for equality lookups. */
  private readonly singleFieldIndexes = new Map<string, Set<string>>();

  constructor(
    dbName: string,
    version: number,
    collections: Record<string, CollectionSchema>,
  ) {
    this.dbName = dbName;
    this.version = version;
    this.collections = collections;

    for (const [name, schema] of Object.entries(collections)) {
      const def = buildStoreDefinition(name, schema);
      const set = new Set<string>();
      for (const idx of def.indexes) {
        // Only single-field indexes whose key is a usable equality key.
        // Exclude '_updatedAt' (range-queried separately) and '_deleted'
        // (boolean keys are invalid in IndexedDB, so that index is empty).
        if (typeof idx.keyPath === 'string' && idx.keyPath !== '_updatedAt' && idx.keyPath !== '_deleted') {
          set.add(idx.keyPath);
        }
      }
      this.singleFieldIndexes.set(name, set);
    }
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

  onChangeEntry(cb: (entry: ChangeEntry, doc: Doc) => void): () => void {
    this.changeListeners.push(cb);
    return () => {
      this.changeListeners = this.changeListeners.filter((l) => l !== cb);
    };
  }

  private _emitChange(entry: ChangeEntry, doc: Doc): void {
    // Runs AFTER the write transaction has committed. A throwing listener must
    // not reject the (already durable) put/delete/bulkInsert, nor starve later
    // listeners — surface it out of band instead.
    for (const cb of this.changeListeners) {
      try {
        cb(entry, doc);
      } catch (err) {
        queueMicrotask(() => {
          throw err;
        });
      }
    }
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

    // Persist the data, change entry, and HLC watermark in ONE transaction:
    // one round-trip instead of two, and atomic — the watermark can never lag
    // behind a durable write across a crash.
    const tx = this.db.transaction([collection, CHANGES_STORE, META_STORE], 'readwrite');
    await tx.objectStore(collection).put(record);
    await tx.objectStore(CHANGES_STORE).put(changeEntry);
    await tx.objectStore(META_STORE).put({ key: 'hlc', value: rev });
    await tx.done;

    this._emitChange(changeEntry, record);
    return record;
  }

  async get(collection: string, id: string): Promise<Doc | null> {
    const record = await this.db.get(collection, id);
    return (record as Doc | undefined) ?? null;
  }

  /**
   * Internal: fetch many ids from one collection in a single readonly transaction.
   * Returns docs in the same order as `ids`; `null` for missing ids.
   */
  async getMany(collection: string, ids: string[]): Promise<(Doc | null)[]> {
    if (ids.length === 0) return [];
    const tx = this.db.transaction(collection, 'readonly');
    const store = tx.objectStore(collection);
    // Issue all gets synchronously so the tx stays open until they settle.
    const reqs = ids.map((id) => store.get(id) as Promise<Doc | undefined>);
    const records = await Promise.all(reqs);
    await tx.done;
    return records.map((r) => r ?? null);
  }

  /**
   * Internal: fetch many ids across collections in a single readonly transaction.
   * Returns docs (tagged with `_collection`) in `items` order, skipping missing ids.
   * Equivalent in output to N sequential get()s but with one transaction.
   */
  async getManyForChanges(
    items: Array<{ id: string; collection: string }>,
  ): Promise<Array<Doc & { _collection: string }>> {
    if (items.length === 0) return [];
    const collections = Array.from(new Set(items.map((i) => i.collection)));
    const tx = this.db.transaction(collections, 'readonly');
    const reqs = items.map(
      (it) => tx.objectStore(it.collection).get(it.id) as Promise<Doc | undefined>,
    );
    const records = await Promise.all(reqs);
    await tx.done;
    const out: Array<Doc & { _collection: string }> = [];
    for (let i = 0; i < records.length; i++) {
      const rec = records[i];
      if (rec) out.push({ ...rec, _collection: items[i].collection });
    }
    return out;
  }

  /**
   * Internal (snapshot streaming only): forward keyset page by primary key (`_id`).
   * Returns up to `limit` docs with `_id` strictly greater than `afterId`, in
   * ascending key order, INCLUDING tombstones. O(batch) per call — unlike offset
   * paging which re-scans skipped records, making full export O(n) overall.
   */
  async _querySnapshotBatch(
    collection: string,
    afterId: string | null,
    limit: number,
  ): Promise<Doc[]> {
    // IndexedDB treats getAll(range, 0) as "no limit" (return everything), which
    // would break bounded-memory paging — clamp to >= 1.
    const size = Math.max(1, Math.trunc(limit));
    const tx = this.db.transaction(collection, 'readonly');
    const range = afterId === null ? undefined : IDBKeyRange.lowerBound(afterId, true);
    const records = (await tx.objectStore(collection).getAll(range, size)) as Doc[];
    await tx.done;
    return records;
  }

  /** Pre-compute the [key, value] pairs of a where clause once, dropping the
   *  special-cased `_updatedAt` key (handled via its index, never as equality). */
  private _buildWherePairs(where?: QueryOptions['where']): Array<[string, unknown]> {
    return where
      ? (Object.entries(where).filter(([k]) => k !== '_updatedAt') as Array<[string, unknown]>)
      : [];
  }

  private _matchesPairs(record: Doc, pairs: Array<[string, unknown]>): boolean {
    for (const [k, v] of pairs) {
      if ((record as Record<string, unknown>)[k] !== v) return false;
    }
    return true;
  }

  /** A `_updatedAt` where-clause is an exclusive lower bound (`_updatedAt > x`),
   *  matching the canonical query path's index range. Returns the bound or null. */
  private _updatedAtLowerBound(where?: QueryOptions['where']): string | null {
    return where && '_updatedAt' in where && where['_updatedAt']
      ? (where['_updatedAt'] as unknown as string)
      : null;
  }

  /** Pick the first where-field backed by a single-field index whose value is a
   *  usable equality key (string/number). Returns null when a full scan is needed. */
  private _pickIndexedWhereField(collection: string, where?: QueryOptions['where']): string | null {
    if (!where) return null;
    const idxs = this.singleFieldIndexes.get(collection);
    if (!idxs) return null;
    for (const k of Object.keys(where)) {
      if (k === '_updatedAt') continue;
      const v = (where as Record<string, unknown>)[k];
      if ((typeof v === 'string' || typeof v === 'number') && idxs.has(k)) return k;
    }
    return null;
  }

  private async _queryCursor(collection: string, options: QueryOptions): Promise<Doc[]> {
    const { where, limit, offset = 0, includeDeleted = false } = options;
    const tx = this.db.transaction(collection, 'readonly');
    const results: Doc[] = [];
    const wherePairs = this._buildWherePairs(where);
    const updatedAtLower = this._updatedAtLowerBound(where);
    let skipped = 0;

    // Equality-index fast path: when a where-field is backed by a single-field
    // index, cursor only its matching key value instead of the whole store.
    // For one index key, IndexedDB yields records in ascending primary-key (_id)
    // order — identical to the bare object-store cursor — so the predicates,
    // offset skipping and early-exit-on-limit below produce byte-identical
    // output, just touching O(matches) records instead of O(store). (Not routing
    // _updatedAt through its index here: that cursor iterates in _updatedAt order
    // and would change this path's _id-ordered output.)
    const indexedField = this._pickIndexedWhereField(collection, where);
    const store = tx.objectStore(collection);
    let cursor = indexedField
      ? await store
          .index(indexedField)
          .openCursor(IDBKeyRange.only((where as Record<string, unknown>)[indexedField] as IDBValidKey))
      : await store.openCursor();
    while (cursor) {
      const record = cursor.value as Doc;
      if (
        (includeDeleted || !record._deleted) &&
        (updatedAtLower === null || (record._updatedAt as string) > updatedAtLower) &&
        this._matchesPairs(record, wherePairs)
      ) {
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
      // Use a single-field index for equality when one exists — fetch only the
      // matching records instead of scanning the whole store. Index `getAll`
      // returns records in primary-key (_id) order for a single key value, which
      // is identical to the full-store getAll order, so results are unchanged.
      const indexedField = this._pickIndexedWhereField(collection, where);
      if (indexedField) {
        const tx = this.db.transaction(collection, 'readonly');
        const index = tx.objectStore(collection).index(indexedField);
        const only = IDBKeyRange.only((where as Record<string, unknown>)[indexedField] as IDBValidKey);
        records = (await index.getAll(only)) as Doc[];
        await tx.done;
      } else {
        records = (await this.db.getAll(collection)) as Doc[];
      }
    }

    if (!includeDeleted) {
      records = records.filter((r) => !r._deleted);
    }

    if (where) {
      const wherePairs = this._buildWherePairs(where);
      records = records.filter((r) => this._matchesPairs(r, wherePairs));
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
    // Read, write tombstone + change entry, and persist the HLC watermark all in
    // one transaction. The existence read happens inside the same tx (no separate
    // round-trip); the clock only ticks once we know there is something to delete.
    const tx = this.db.transaction([collection, CHANGES_STORE, META_STORE], 'readwrite');
    const store = tx.objectStore(collection);
    const existing = (await store.get(id)) as Doc | undefined;
    if (!existing) return; // no-op: no tick, no write, no emit; tx auto-closes

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

    await store.put(tombstone);
    await tx.objectStore(CHANGES_STORE).put(changeEntry);
    await tx.objectStore(META_STORE).put({ key: 'hlc', value: rev });
    await tx.done;

    this._emitChange(changeEntry, tombstone);
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
        }, doc);
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
    const arrays = await Promise.all(
      collectionNames.map((name) => this.db.getAll(name) as Promise<Doc[]>),
    );
    const collections: Record<string, Doc[]> = {};
    collectionNames.forEach((name, i) => {
      collections[name] = arrays[i];
    });

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

  /**
   * Streaming export — yields a header chunk then bounded per-collection batches,
   * keyset-paged by `_id`. Peak memory is one batch (`batchSize` docs), so this
   * scales to multi-GB stores where `export()` would exhaust the heap. Tombstones
   * are included. Not a locking snapshot: concurrent writes during iteration may
   * or may not appear (same caveat as `export()`); run it on a quiescent DB.
   */
  async *exportStream(batchSize = 1000): AsyncGenerator<SnapshotChunk> {
    const size = Math.max(1, Math.trunc(batchSize));
    const collectionNames = await this.collectionNames();

    const tx = this.db.transaction(META_STORE, 'readonly');
    const allMeta = await tx.store.getAll();
    await tx.done;
    const meta: Record<string, unknown> = {};
    for (const record of allMeta) {
      meta[(record as { key: string; value: unknown }).key] =
        (record as { key: string; value: unknown }).value;
    }

    yield { kind: 'header', version: this.version, hlc: this.hlc.now(), meta };

    for (const name of collectionNames) {
      let afterId: string | null = null;
      let done = false;
      while (!done) {
        const docs = await this._querySnapshotBatch(name, afterId, size);
        done = docs.length < size;
        if (docs.length > 0) {
          yield { kind: 'batch', collection: name, docs };
          afterId = docs[docs.length - 1]._id;
        }
      }
    }
  }

  /**
   * Streaming import — consumes a header + batches produced by `exportStream`
   * (possibly relayed through NDJSON / a network stream), writing each batch via
   * `bulkInsert`. Peak memory is one batch. The HLC watermark is applied only
   * after every batch lands, so a crash mid-import never advances the watermark
   * past un-imported data. Unknown / system stores are skipped.
   */
  async importStream(chunks: AsyncIterable<SnapshotChunk>): Promise<void> {
    let headerHlc: HLCTimestamp | null = null;
    for await (const chunk of chunks) {
      if (chunk.kind === 'header') {
        headerHlc = chunk.hlc;
      } else {
        const name = chunk.collection;
        if (!SYSTEM_STORES.includes(name) && this.db.objectStoreNames.contains(name)) {
          await this.bulkInsert(name, chunk.docs);
        }
      }
    }
    if (headerHlc !== null) {
      this.hlc.update(headerHlc);
      await this._persistHLC();
    }
  }

  /**
   * Streaming, bounded-memory iteration over a collection in `_id` order.
   * Pages by primary key (`batchSize` docs per IndexedDB read) and yields each
   * doc that passes the `where` filter; excludes tombstones unless
   * `includeDeleted`. Peak memory is one page — use this instead of an unbounded
   * `query()` for large scans.
   */
  async *scan(collection: string, options: ScanOptions = {}): AsyncGenerator<Doc> {
    const { where, includeDeleted = false, batchSize = 1000 } = options;
    const size = Math.max(1, Math.trunc(batchSize));
    const wherePairs = this._buildWherePairs(where);
    // `query()` treats a `_updatedAt` clause as an exclusive lower bound
    // (_updatedAt > x); apply the same predicate here so scan() and query()
    // agree. HLC strings are lexicographically ordered.
    const updatedAtLower = this._updatedAtLowerBound(where);
    let afterId: string | null = null;
    while (true) {
      const batch = await this._querySnapshotBatch(collection, afterId, size);
      if (batch.length === 0) break;
      afterId = batch[batch.length - 1]._id;
      for (const doc of batch) {
        if (updatedAtLower !== null && !((doc._updatedAt as string) > updatedAtLower)) continue;
        if ((includeDeleted || !doc._deleted) && this._matchesPairs(doc, wherePairs)) {
          yield doc;
        }
      }
      if (batch.length < size) break;
    }
  }

  async collectionNames(): Promise<string[]> {
    return Array.from(this.db.objectStoreNames).filter(
      (n) => !SYSTEM_STORES.includes(n),
    );
  }

  /**
   * Internal: true iff none of the given stores hold any record (tombstones
   * included). One readonly transaction; `count()` avoids deserializing any doc.
   */
  async areStoresEmpty(collections: string[]): Promise<boolean> {
    if (collections.length === 0) return true;
    const tx = this.db.transaction(collections, 'readonly');
    try {
      for (const name of collections) {
        const n = await tx.objectStore(name).count();
        if (n > 0) return false;
      }
      return true;
    } finally {
      await tx.done;
    }
  }

  async close(): Promise<void> {
    this.db.close();
  }

  async pruneChanges(olderThanMs: number): Promise<void> {
    const cutoffMs = Date.now() - olderThanMs;
    const cutoffHlc = formatHLC({ physicalMs: cutoffMs, counter: 0, nodeId: '' });

    const tx = this.db.transaction(CHANGES_STORE, 'readwrite');
    const index = tx.objectStore(CHANGES_STORE).index('_updatedAt');
    const range = IDBKeyRange.upperBound(cutoffHlc, false);
    let cursor = await index.openCursor(range);
    const deletes: Promise<void>[] = [];
    while (cursor) {
      deletes.push(cursor.delete()); // fire; only continue() round-trips per entry
      cursor = await cursor.continue();
    }
    await Promise.all(deletes);
    await tx.done;

    // Cache the new oldest entry so _handleSyncRequest can check cheaply
    const tx2 = this.db.transaction(CHANGES_STORE, 'readonly');
    const first = await tx2.objectStore(CHANGES_STORE).index('_updatedAt').openCursor();
    await tx2.done;
    const oldest = first ? (first.value as ChangeEntry)._updatedAt : null;
    await this.setMetaValue('oldestChangesHlc', oldest);
  }

  async getOldestChangesHlc(): Promise<HLCTimestamp | null> {
    const cached = await this.getMetaValue('oldestChangesHlc');
    if (cached !== undefined && cached !== null) return cached as HLCTimestamp;

    const tx = this.db.transaction(CHANGES_STORE, 'readonly');
    const cursor = await tx.objectStore(CHANGES_STORE).index('_updatedAt').openCursor();
    await tx.done;
    return cursor ? (cursor.value as ChangeEntry)._updatedAt : null;
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
