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

/** One staged operation of an atomic write batch (see `applyBatch` / `db.transaction`). */
export type BatchOp =
  | { type: 'put'; collection: string; doc: Record<string, unknown> }
  | { type: 'delete'; collection: string; id: string };

export class IndexedDBAdapter implements IStorageAdapter {
  private db!: IDBPDatabase;
  private hlc!: HLC;
  private readonly dbName: string;
  readonly version: number;
  private readonly collections: Record<string, CollectionSchema>;
  private changeListeners: Array<(entry: ChangeEntry, doc: Doc) => void> = [];
  private changeBatchListeners: Array<
    (collection: string, entries: ChangeEntry[], docs: Doc[]) => void
  > = [];
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

  /**
   * Batched change notification: one callback per committed write transaction
   * (a single put/delete, or one bulkInsert chunk — always one collection).
   * Prefer this over `onChangeEntry` for fan-out work (UI listeners, gossip
   * re-broadcast): a 1000-doc sync batch costs 1 callback instead of 1000.
   */
  onChangeBatch(cb: (collection: string, entries: ChangeEntry[], docs: Doc[]) => void): () => void {
    this.changeBatchListeners.push(cb);
    return () => {
      this.changeBatchListeners = this.changeBatchListeners.filter((l) => l !== cb);
    };
  }

  private _emitChanges(collection: string, entries: ChangeEntry[], docs: Doc[]): void {
    // Runs AFTER the write transaction has committed. A throwing listener must
    // not reject the (already durable) put/delete/bulkInsert, nor starve later
    // listeners — surface it out of band instead.
    if (this.changeListeners.length > 0) {
      for (let i = 0; i < entries.length; i++) {
        for (const cb of this.changeListeners) {
          try {
            cb(entries[i], docs[i]);
          } catch (err) {
            queueMicrotask(() => {
              throw err;
            });
          }
        }
      }
    }
    for (const cb of this.changeBatchListeners) {
      try {
        cb(collection, entries, docs);
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

    this._emitChanges(collection, [changeEntry], [record]);
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

  /**
   * Internal (scan only): forward keyset page over a single-field equality index.
   * Returns up to `limit` docs whose `field === value` and `_id > afterId`, in
   * ascending `_id` order — the same subset/order `_querySnapshotBatch` + an
   * in-memory equality filter would produce, but touching O(matches) records
   * instead of O(store).
   */
  private async _queryIndexBatch(
    collection: string,
    field: string,
    value: IDBValidKey,
    afterId: string | null,
    limit: number,
  ): Promise<Doc[]> {
    const size = Math.max(1, Math.trunc(limit));
    const tx = this.db.transaction(collection, 'readonly');
    const index = tx.objectStore(collection).index(field);
    // For one index key, cursor order is ascending primary key (_id).
    let cursor = await index.openCursor(IDBKeyRange.only(value));
    if (cursor && afterId !== null) {
      const pk = cursor.primaryKey as string;
      if (pk === afterId) {
        cursor = await cursor.continue();
      } else if (pk < afterId) {
        // Jump straight to afterId (continuePrimaryKey is inclusive, so step past it).
        cursor = await cursor.continuePrimaryKey(value, afterId);
        if (cursor && (cursor.primaryKey as string) === afterId) cursor = await cursor.continue();
      }
      // pk > afterId: already past the watermark — keep the cursor as is.
    }
    const out: Doc[] = [];
    while (cursor && out.length < size) {
      out.push(cursor.value as Doc);
      cursor = await cursor.continue();
    }
    await tx.done;
    return out;
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

    this._emitChanges(collection, [changeEntry], [tombstone]);
  }

  /**
   * Atomic write batch: apply `ops` (puts/deletes across any user collections), in
   * order, inside ONE IndexedDB transaction together with their change-log entries
   * and the HLC watermark. All-or-nothing — if any op fails the transaction is
   * aborted and nothing is persisted or emitted. Each op gets its own `_rev`
   * (per-doc LWW stays intact); a delete of a missing id is a no-op; ops on the
   * same id apply in submitted order (a delete sees a put staged before it).
   * Returns the committed docs (tombstones included, no-op deletes omitted).
   *
   * Atomicity is LOCAL: peers receive the batch as one frame per collection and
   * bulk-apply it, but cross-peer conflict resolution remains per-doc.
   */
  async applyBatch(ops: BatchOp[]): Promise<Doc[]> {
    if (ops.length === 0) return [];
    const collections = Array.from(new Set(ops.map((o) => o.collection)));
    const tx = this.db.transaction([...collections, CHANGES_STORE, META_STORE], 'readwrite');
    const written: Doc[] = [];
    const entries: ChangeEntry[] = [];
    try {
      const changesStore = tx.objectStore(CHANGES_STORE);
      let lastRev: HLCTimestamp | null = null;
      for (const op of ops) {
        const store = tx.objectStore(op.collection);
        let record: Doc;
        let operation: ChangeEntry['operation'];
        if (op.type === 'put') {
          const rev = this.hlc.tick();
          record = {
            ...op.doc,
            _id: (op.doc['_id'] as string) ?? uuidv4(),
            _rev: rev,
            _deleted: false,
            _updatedAt: rev,
          };
          operation = 'put';
        } else {
          // Read inside the same tx so a put staged earlier in this batch is seen.
          // Missing id → no-op, matching delete().
          const existing = (await store.get(op.id)) as Doc | undefined;
          if (!existing) continue;
          const rev = this.hlc.tick();
          record = { ...existing, _deleted: true, _rev: rev, _updatedAt: rev };
          operation = 'delete';
        }
        const entry: ChangeEntry = {
          id: record._id,
          collection: op.collection,
          _rev: record._rev,
          _updatedAt: record._updatedAt,
          operation,
          origin: 'local',
        };
        await store.put(record);
        await changesStore.put(entry);
        written.push(record);
        entries.push(entry);
        lastRev = record._rev;
      }
      if (lastRev === null) return []; // every op was a no-op; tx auto-closes
      await tx.objectStore(META_STORE).put({ key: 'hlc', value: lastRev });
    } catch (err) {
      // Without an explicit abort, the tx would auto-COMMIT the ops already
      // queued once the request queue drains — the opposite of atomicity.
      // The abort makes tx.done reject; that rejection is expected — swallow it
      // so it can't surface as an unhandled rejection (we rethrow the cause).
      tx.done.catch(() => undefined);
      try {
        tx.abort();
      } catch {
        // already aborted (e.g. the failing request aborted it)
      }
      throw err;
    }
    await tx.done;

    // Emit AFTER commit, one batch per collection (op order preserved within each),
    // matching bulkInsert: listeners get one callback, gossip one frame per collection.
    const grouped = new Map<string, { entries: ChangeEntry[]; docs: Doc[] }>();
    for (let i = 0; i < entries.length; i++) {
      let g = grouped.get(entries[i].collection);
      if (!g) grouped.set(entries[i].collection, (g = { entries: [], docs: [] }));
      g.entries.push(entries[i]);
      g.docs.push(written[i]);
    }
    for (const [collection, g] of grouped) this._emitChanges(collection, g.entries, g.docs);
    return written;
  }

  async bulkInsert(collection: string, docs: Doc[]): Promise<void> {
    if (docs.length === 0) return;
    for (let start = 0; start < docs.length; start += BULK_INSERT_BATCH_SIZE) {
      const batch = docs.slice(start, start + BULK_INSERT_BATCH_SIZE);
      const entries: ChangeEntry[] = batch.map((d) => ({
        id: d._id,
        collection,
        _rev: d._rev,
        _updatedAt: d._updatedAt,
        operation: d._deleted ? 'delete' : 'put',
        origin: 'peer',
      }));
      const tx = this.db.transaction([collection, CHANGES_STORE], 'readwrite');
      const store = tx.objectStore(collection);
      const changesStore = tx.objectStore(CHANGES_STORE);
      await Promise.all(
        batch.map((d, i) => Promise.all([store.put(d), changesStore.put(entries[i])])),
      );
      await tx.done;
      this._emitChanges(collection, entries, batch);
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
    // Equality-index fast path: page over the index's single key value instead of
    // the whole store. Per-key index order is ascending _id, so the yielded docs
    // and their order are identical to the full-store path filtered in memory.
    const indexedField = this._pickIndexedWhereField(collection, where);
    const indexedValue = indexedField
      ? ((where as Record<string, unknown>)[indexedField] as IDBValidKey)
      : null;
    let afterId: string | null = null;
    while (true) {
      const batch: Doc[] = indexedField
        ? await this._queryIndexBatch(collection, indexedField, indexedValue!, afterId, size)
        : await this._querySnapshotBatch(collection, afterId, size);
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

  /**
   * Write a conflict-audit record directly to `_conflicts`. Unlike `put()`, this
   * does NOT tick the HLC, append a change-log entry, or notify change listeners —
   * conflict records are local diagnostics and must not enter the change log
   * (where gossip would push them to every peer) nor trigger re-broadcast.
   */
  async putConflict(record: Record<string, unknown> & { _id: string }): Promise<void> {
    // Wall-clock stamp (not hlc.now(), which is 0 on a fresh replica until the
    // first local write) — pruneConflicts compares against a wall-clock cutoff.
    const now = formatHLC({ physicalMs: Date.now(), counter: 0, nodeId: this.hlc.nodeId });
    const tx = this.db.transaction(CONFLICTS_STORE, 'readwrite');
    await tx.store.put({ ...record, _rev: now, _updatedAt: now, _deleted: false });
    await tx.done;
  }

  /** Delete `_conflicts` records older than `olderThanMs` (same cutoff math as
   *  `pruneChanges`) so the audit log can't grow without bound. */
  async pruneConflicts(olderThanMs: number): Promise<void> {
    const cutoffMs = Date.now() - olderThanMs;
    const cutoffHlc = formatHLC({ physicalMs: cutoffMs, counter: 0, nodeId: '' });

    const tx = this.db.transaction(CONFLICTS_STORE, 'readwrite');
    const index = tx.objectStore(CONFLICTS_STORE).index('_updatedAt');
    let cursor = await index.openCursor(IDBKeyRange.upperBound(cutoffHlc, false));
    const deletes: Promise<void>[] = [];
    while (cursor) {
      deletes.push(cursor.delete());
      cursor = await cursor.continue();
    }
    await Promise.all(deletes);
    await tx.done;
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
