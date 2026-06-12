import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from './indexeddb.js';
import { logConflict } from '../core/conflict.js';
import type { Doc, HLCTimestamp, ScanOptions } from '../core/types.js';

// Proves the equality-index fast path in scan() is output-equivalent to the
// full-store keyset path (same docs, same _id order), and covers the
// conflict-log changes (no change-log pollution, prunable).

const collections = {
  todos: { indexes: ['status'] },
  plain: {}, // no user index → always full-store path
};

async function collect(adapter: IndexedDBAdapter, collection: string, options?: ScanOptions): Promise<Doc[]> {
  const out: Doc[] = [];
  for await (const doc of adapter.scan(collection, options)) out.push(doc);
  return out;
}

describe('scan() equality-index fast path', () => {
  let adapter: IndexedDBAdapter;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(`scan-${Math.random()}`, 1, collections);
    await adapter.open();
    // 120 docs, 3 statuses, some deleted — enough to cross several batch pages.
    for (let i = 0; i < 120; i++) {
      const doc = await adapter.put('todos', {
        _id: `id-${String(i).padStart(3, '0')}`,
        status: ['open', 'done', 'archived'][i % 3],
        n: i,
      });
      if (i % 10 === 0) await adapter.delete('todos', doc._id);
    }
  });

  it('returns the same docs in the same order as a full scan filtered in memory', async () => {
    // Indexed path (status is an indexed field)
    const indexed = await collect(adapter, 'todos', { where: { status: 'open' }, batchSize: 7 });
    // Reference: full scan, filter in memory
    const all = await collect(adapter, 'todos', { batchSize: 7 });
    const reference = all.filter((d) => d['status'] === 'open');
    expect(indexed.map((d) => d._id)).toEqual(reference.map((d) => d._id));
    expect(indexed.length).toBeGreaterThan(30); // sanity: actually matched things
  });

  it('respects includeDeleted and small page sizes across page boundaries', async () => {
    const withDeleted = await collect(adapter, 'todos', {
      where: { status: 'open' },
      includeDeleted: true,
      batchSize: 1, // worst case: one doc per page
    });
    const withoutDeleted = await collect(adapter, 'todos', { where: { status: 'open' } });
    expect(withDeleted.length).toBeGreaterThan(withoutDeleted.length);
    const ids = withDeleted.map((d) => d._id);
    expect([...ids].sort()).toEqual(ids); // still ascending _id order
  });

  it('combines the indexed field with extra non-indexed predicates', async () => {
    const docs = await collect(adapter, 'todos', { where: { status: 'done', n: 4 }, batchSize: 5 });
    expect(docs.length).toBe(1);
    expect(docs[0]['n']).toBe(4);
  });

  it('falls back to the full-store path when no usable index exists', async () => {
    await adapter.put('plain', { _id: 'a', kind: 'x' });
    await adapter.put('plain', { _id: 'b', kind: 'y' });
    const docs = await collect(adapter, 'plain', { where: { kind: 'x' } });
    expect(docs.map((d) => d._id)).toEqual(['a']);
  });
});

describe('conflict log isolation and pruning', () => {
  let adapter: IndexedDBAdapter;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(`conf-${Math.random()}`, 1, collections);
    await adapter.open();
  });

  function doc(id: string, rev: string): Doc {
    return { _id: id, _rev: rev as HLCTimestamp, _updatedAt: rev as HLCTimestamp, _deleted: false };
  }

  it('logConflict writes no change-log entry (conflicts must not gossip)', async () => {
    const before = (await adapter.changes('' as HLCTimestamp)).length;
    await logConflict(
      adapter,
      'todos',
      doc('t1', '0000000000000001-000000-a'),
      doc('t1', '0000000000000002-000000-b'),
      doc('t1', '0000000000000002-000000-b'),
    );
    const after = (await adapter.changes('' as HLCTimestamp)).length;
    expect(after).toBe(before); // nothing entered the change log
    const conflicts = await adapter.query('_conflicts', { includeDeleted: true });
    expect(conflicts.length).toBe(1); // but the audit record is there
  });

  it('logConflict does not notify change listeners (no re-broadcast hook)', async () => {
    let notified = 0;
    adapter.onChangeBatch(() => { notified += 1; });
    adapter.onChangeEntry(() => { notified += 1; });
    await logConflict(
      adapter,
      'todos',
      doc('t1', '0000000000000001-000000-a'),
      doc('t1', '0000000000000002-000000-b'),
      doc('t1', '0000000000000002-000000-b'),
    );
    expect(notified).toBe(0);
  });

  it('pruneConflicts removes aged records and keeps fresh ones', async () => {
    await logConflict(
      adapter,
      'todos',
      doc('t1', '0000000000000001-000000-a'),
      doc('t1', '0000000000000002-000000-b'),
      doc('t1', '0000000000000002-000000-b'),
    );
    expect((await adapter.query('_conflicts', { includeDeleted: true })).length).toBe(1);

    // Cutoff in the past → record is fresh → kept.
    await adapter.pruneConflicts(60_000);
    expect((await adapter.query('_conflicts', { includeDeleted: true })).length).toBe(1);

    // Cutoff in the future (negative age) → record is "old" → pruned.
    await adapter.pruneConflicts(-60_000);
    expect((await adapter.query('_conflicts', { includeDeleted: true })).length).toBe(0);
  });
});
