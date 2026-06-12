import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { createDB } from './db.js';
import { IndexedDBAdapter } from './storage/indexeddb.js';
import type { ChangeEntry, Doc, HLCTimestamp } from './core/types.js';

const collections = { todos: { indexes: ['status'] }, notes: {} };

describe('db.transaction (atomic write batch)', () => {
  beforeEach(() => {
    globalThis.indexedDB = new IDBFactory();
  });

  async function open() {
    return createDB({ name: `tx-${Math.random()}`, version: 1, collections });
  }

  it('commits puts and deletes across collections atomically, in op order', async () => {
    const db = await open();
    await db.notes.put({ _id: 'n1', body: 'old' });

    const written = await db.transaction((tx) => {
      tx.todos.put({ _id: 't1', status: 'open' });
      tx.todos.put({ _id: 't2', status: 'done' });
      tx.notes.delete('n1');
    });

    expect(written.map((d) => d._id)).toEqual(['t1', 't2', 'n1']);
    expect(written[2]._deleted).toBe(true); // tombstone returned
    expect((await db.todos.get('t1'))?.['status']).toBe('open');
    expect((await db.todos.get('t2'))?.['status']).toBe('done');
    expect(await db.notes.get('n1')).not.toBeNull(); // tombstone still readable via get
    expect((await db.notes.query()).length).toBe(0); // …but excluded from queries
    // Revs are strictly increasing in op order (per-doc LWW intact).
    expect(written[0]._rev < written[1]._rev && written[1]._rev < written[2]._rev).toBe(true);
    await db.close();
  });

  it('rolls back EVERYTHING when one op fails (no docs, no change entries, no events)', async () => {
    const db = await open();
    let events = 0;
    db.todos.onChange(() => { events += 1; });

    await expect(
      db.transaction((tx) => {
        tx.todos.put({ _id: 'ok', status: 'open' });
        // Functions are not structured-cloneable → this put fails inside the tx.
        tx.notes.put({ _id: 'boom', fn: () => 1 } as never);
      }),
    ).rejects.toThrow();

    expect(await db.todos.get('ok')).toBeNull(); // first op rolled back too
    expect(events).toBe(0);
    await db.close();
  });

  it('a delete sees a put staged earlier in the same batch', async () => {
    const db = await open();
    const written = await db.transaction((tx) => {
      tx.todos.put({ _id: 'x', status: 'open' });
      tx.todos.delete('x');
    });
    expect(written.length).toBe(2);
    expect(written[1]._deleted).toBe(true);
    expect((await db.todos.query()).length).toBe(0);
    await db.close();
  });

  it('deleting a missing id is a no-op; an all-no-op batch commits nothing', async () => {
    const db = await open();
    const written = await db.transaction((tx) => {
      tx.todos.delete('ghost');
    });
    expect(written).toEqual([]);
    await db.close();
  });

  it('an empty callback resolves without opening a transaction', async () => {
    const db = await open();
    expect(await db.transaction(() => undefined)).toEqual([]);
    await db.close();
  });

  it('notifies onChange ONCE per collection with the batch entries', async () => {
    const db = await open();
    const batches: ChangeEntry[][] = [];
    db.todos.onChange((entries) => batches.push(entries));

    await db.transaction((tx) => {
      tx.todos.put({ _id: 'a', status: 'open' });
      tx.todos.put({ _id: 'b', status: 'open' });
      tx.notes.put({ _id: 'n', body: 'hi' });
    });

    expect(batches.length).toBe(1); // one callback, not two
    expect(batches[0].map((e) => e.id)).toEqual(['a', 'b']);
    expect(batches[0].every((e) => e.origin === 'local' && e.operation === 'put')).toBe(true);
    await db.close();
  });
});

describe('IndexedDBAdapter.applyBatch durability', () => {
  beforeEach(() => {
    globalThis.indexedDB = new IDBFactory();
  });

  it('persists change entries and the HLC watermark with the docs', async () => {
    const adapter = new IndexedDBAdapter(`ab-${Math.random()}`, 1, collections);
    await adapter.open();
    const written = await adapter.applyBatch([
      { type: 'put', collection: 'todos', doc: { _id: 't1', status: 'open' } },
      { type: 'put', collection: 'notes', doc: { _id: 'n1', body: 'x' } },
    ]);
    const changes = await adapter.changes('' as HLCTimestamp);
    expect(changes.map((c) => c.id).sort()).toEqual(['n1', 't1']);
    // Watermark persisted to meta: equals the last committed rev.
    expect(await adapter.getMetaValue('hlc')).toBe(written[1]._rev);
    await adapter.close();
  });

  it('a failed batch leaves the change log and watermark untouched', async () => {
    const adapter = new IndexedDBAdapter(`ab-${Math.random()}`, 1, collections);
    await adapter.open();
    const seeded: Doc = await adapter.put('todos', { _id: 'seed', status: 'open' });

    await expect(
      adapter.applyBatch([
        { type: 'put', collection: 'todos', doc: { _id: 'p1', status: 'open' } },
        { type: 'put', collection: 'notes', doc: { _id: 'p2', fn: () => 1 } as never },
      ]),
    ).rejects.toThrow();

    const changes = await adapter.changes('' as HLCTimestamp);
    expect(changes.map((c) => c.id)).toEqual(['seed']);
    expect(await adapter.getMetaValue('hlc')).toBe(seeded._rev);
    await adapter.close();
  });
});
