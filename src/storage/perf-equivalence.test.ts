import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from './indexeddb.js';
import type { Doc, HLCTimestamp, QueryOptions } from '../core/types.js';

// These tests guard the performance optimizations against behavior changes.
// The core idea: an adapter that CAN use a single-field index must return
// byte-identical results to an adapter that has no such index (full scan).

function rev(n: number): HLCTimestamp {
  return (String(n).padStart(16, '0') + '-000000-node') as HLCTimestamp;
}

// Deterministic pseudo-data (no Math.random → stable across runs).
function buildDocs(n: number): Doc[] {
  const statuses = ['open', 'done', 'archived', 'open', 'pending'];
  const docs: Doc[] = [];
  for (let i = 0; i < n; i++) {
    docs.push({
      _id: `id-${String(i).padStart(5, '0')}`,
      _rev: rev(1000 + i),
      _updatedAt: rev(1000 + i),
      _deleted: i % 7 === 0, // ~1/7 tombstones
      status: statuses[i % statuses.length],
      priority: i % 3, // numeric field, also indexed below
      label: `label-${i % 11}`, // non-indexed field
    } as Doc);
  }
  return docs;
}

describe('query() index fast-path is equivalent to full scan', () => {
  let indexed: IndexedDBAdapter;
  let plain: IndexedDBAdapter;
  const docs = buildDocs(400);

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    // `indexed` has indexes on status + priority → query() uses them.
    indexed = new IndexedDBAdapter(`idx-${Math.random()}`, 1, {
      todos: { indexes: ['status', 'priority'] },
    });
    // `plain` has NO field indexes → query() falls back to a full getAll scan.
    plain = new IndexedDBAdapter(`plain-${Math.random()}`, 1, {
      todos: { indexes: [] },
    });
    await indexed.open();
    await plain.open();
    await indexed.bulkInsert('todos', docs);
    await plain.bulkInsert('todos', docs);
  });

  const cases: QueryOptions[] = [
    { where: { status: 'open' } },
    { where: { status: 'done' } },
    { where: { status: 'nonexistent' } },
    { where: { priority: 1 } },
    { where: { status: 'open' }, includeDeleted: true },
    { where: { status: 'open' }, orderBy: 'label', orderDir: 'asc' },
    { where: { status: 'open' }, orderBy: 'label', orderDir: 'desc' },
    { where: { status: 'open' }, orderBy: '_id', orderDir: 'desc' },
    { where: { status: 'open', label: 'label-3' } }, // index + extra filter
    { where: { status: 'open', priority: 2 } }, // two indexed fields in where
    { where: { status: 'open' }, offset: 5, limit: 10 },
    { where: { status: 'open' }, limit: 10 }, // cursor fast-path on both
    { where: { status: 'archived' }, offset: 2, limit: 5 }, // sparse value via index cursor
    { where: { status: 'archived' }, limit: 3 },
    { where: { priority: 2 }, offset: 1, limit: 4 }, // numeric indexed field, cursor path
    { where: { label: 'label-3' } }, // non-indexed where → full scan on both
    {}, // no where
    { includeDeleted: true },
  ];

  for (const opts of cases) {
    it(`matches full scan for ${JSON.stringify(opts)}`, async () => {
      const a = await indexed.query('todos', opts);
      const b = await plain.query('todos', opts);
      expect(a).toEqual(b);
    });
  }

  it('boolean-valued where never uses the (invalid) _deleted index', async () => {
    // _deleted is boolean → not a usable IDB key; must fall back to scan and
    // still return correct results.
    const a = await indexed.query('todos', { where: { _deleted: true } as never, includeDeleted: true });
    const b = await plain.query('todos', { where: { _deleted: true } as never, includeDeleted: true });
    expect(a).toEqual(b);
    expect(a.every((d) => d._deleted === true)).toBe(true);
  });
});

describe('_querySnapshotBatch keyset paging covers all docs once, in order', () => {
  let adapter: IndexedDBAdapter;
  const docs = buildDocs(2500); // > several batches

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(`snap-${Math.random()}`, 1, { todos: { indexes: ['status'] } });
    await adapter.open();
    await adapter.bulkInsert('todos', docs);
  });

  it('union of batches equals getAll, no dupes, ascending _id, includes tombstones', async () => {
    const BATCH = 500;
    let afterId: string | null = null;
    let done = false;
    const collected: Doc[] = [];
    while (!done) {
      const page = await adapter._querySnapshotBatch('todos', afterId, BATCH);
      done = page.length < BATCH;
      collected.push(...page);
      if (page.length > 0) afterId = page[page.length - 1]._id;
    }

    // Same count as raw store (tombstones included).
    expect(collected.length).toBe(docs.length);

    // Strictly ascending _id, no duplicates.
    for (let i = 1; i < collected.length; i++) {
      expect(collected[i]._id > collected[i - 1]._id).toBe(true);
    }

    // Set equality with the input ids.
    const got = new Set(collected.map((d) => d._id));
    expect(got.size).toBe(docs.length);
    for (const d of docs) expect(got.has(d._id)).toBe(true);

    // Tombstones preserved.
    expect(collected.some((d) => d._deleted)).toBe(true);
  });
});

describe('write path: HLC watermark persisted atomically + delete no-op', () => {
  beforeEach(() => {
    globalThis.indexedDB = new IDBFactory();
  });

  it('persists the HLC watermark on put (survives reopen)', async () => {
    const name = `hlc-${Math.random()}`;
    const a = new IndexedDBAdapter(name, 1, { todos: { indexes: [] } });
    await a.open();
    const d = await a.put('todos', { _id: '1', title: 'x' });
    await a.close();

    const b = new IndexedDBAdapter(name, 1, { todos: { indexes: [] } });
    await b.open();
    // Reopened clock must be >= the rev we just wrote (watermark restored).
    expect(b.getHLC().now() >= d._rev).toBe(true);
    await b.close();
  });

  it('delete of a missing id is a true no-op (no clock tick, no change entry)', async () => {
    const a = new IndexedDBAdapter(`noop-${Math.random()}`, 1, { todos: { indexes: [] } });
    await a.open();
    await a.put('todos', { _id: 'real' });
    const before = a.getHLC().now();
    const changesBefore = await a.changes('' as HLCTimestamp);

    await a.delete('todos', 'does-not-exist');

    expect(a.getHLC().now()).toBe(before); // no tick
    const changesAfter = await a.changes('' as HLCTimestamp);
    expect(changesAfter.length).toBe(changesBefore.length); // no new change entry
    await a.close();
  });
});
