import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from './indexeddb.js';

function makeAdapter(name = 'test-db') {
  return new IndexedDBAdapter(name, 1, {
    todos: { indexes: ['status'] },
  });
}

describe('IndexedDBAdapter', () => {
  let adapter: IndexedDBAdapter;

  beforeEach(async () => {
    // Fresh IDB factory per test to avoid cross-test pollution
    globalThis.indexedDB = new IDBFactory();
    adapter = makeAdapter(`db-${Math.random()}`);
    await adapter.open();
  });

  it('put() inserts a document with system fields', async () => {
    const doc = await adapter.put('todos', { _id: '1', title: 'Hello', status: 'open' });
    expect(doc._id).toBe('1');
    expect(doc._deleted).toBe(false);
    expect(typeof doc._rev).toBe('string');
    expect(typeof doc._updatedAt).toBe('string');
  });

  it('get() retrieves an inserted document', async () => {
    await adapter.put('todos', { _id: 'abc', title: 'Test' });
    const found = await adapter.get('todos', 'abc');
    expect(found).not.toBeNull();
    expect(found!._id).toBe('abc');
  });

  it('get() returns null for missing documents', async () => {
    const found = await adapter.get('todos', 'missing');
    expect(found).toBeNull();
  });

  it('query() returns all non-deleted documents', async () => {
    await adapter.put('todos', { _id: '1', status: 'open' });
    await adapter.put('todos', { _id: '2', status: 'done' });
    const results = await adapter.query('todos');
    expect(results.length).toBe(2);
  });

  it('query() filters by field', async () => {
    await adapter.put('todos', { _id: '1', status: 'open' });
    await adapter.put('todos', { _id: '2', status: 'done' });
    const results = await adapter.query('todos', { where: { status: 'open' } });
    expect(results.length).toBe(1);
    expect(results[0]._id).toBe('1');
  });

  it('delete() creates a tombstone', async () => {
    await adapter.put('todos', { _id: 'x', title: 'bye' });
    await adapter.delete('todos', 'x');
    const found = await adapter.get('todos', 'x');
    expect(found!._deleted).toBe(true);

    const results = await adapter.query('todos');
    expect(results.length).toBe(0);
  });

  it('delete() with includeDeleted shows tombstone', async () => {
    await adapter.put('todos', { _id: 'x', title: 'bye' });
    await adapter.delete('todos', 'x');
    const results = await adapter.query('todos', { includeDeleted: true });
    expect(results.length).toBe(1);
    expect(results[0]._deleted).toBe(true);
  });

  it('changes() returns entries after the given HLC', async () => {
    const d1 = await adapter.put('todos', { _id: '1' });
    const since = d1._updatedAt;
    await adapter.put('todos', { _id: '2' });
    const changes = await adapter.changes(since);
    expect(changes.length).toBe(1);
    expect(changes[0].id).toBe('2');
  });

  it('bulkInsert() inserts multiple docs in one shot', async () => {
    const now = '0000001700000000-000000-test' as ReturnType<typeof adapter.getHLC>['tick'] extends (...args: unknown[]) => infer R ? R : never;
    await adapter.bulkInsert('todos', [
      { _id: 'b1', _rev: now, _deleted: false, _updatedAt: now },
      { _id: 'b2', _rev: now, _deleted: false, _updatedAt: now },
    ]);
    const b1 = await adapter.get('todos', 'b1');
    const b2 = await adapter.get('todos', 'b2');
    expect(b1?._id).toBe('b1');
    expect(b2?._id).toBe('b2');
  });

  it('collectionNames() lists only user collections', async () => {
    const names = await adapter.collectionNames();
    expect(names).toContain('todos');
    expect(names).not.toContain('_changes');
    expect(names).not.toContain('_meta');
  });

  it('export() + import() round-trips data', async () => {
    await adapter.put('todos', { _id: 'e1', title: 'export me' });
    const snapshot = await adapter.export();
    expect(snapshot.collections['todos'].length).toBe(1);

    const adapter2 = new IndexedDBAdapter(`db-${Math.random()}`, 1, {
      todos: { indexes: [] },
    });
    await adapter2.open();
    await adapter2.import(snapshot);
    const found = await adapter2.get('todos', 'e1');
    expect(found?._id).toBe('e1');
    await adapter2.close();
  });
});
