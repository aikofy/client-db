import { describe, it, expect, beforeEach, vi } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from './indexeddb.js';
import { GossipSync } from '../sync/gossip.js';
import type { Doc, HLCTimestamp, CollectionSchema } from '../core/types.js';

function rev(n: number, node = 'remote'): HLCTimestamp {
  return (String(n).padStart(16, '0') + `-000000-${node}`) as HLCTimestamp;
}
function remoteDoc(collection: string, id: string, r: number, extra: Record<string, unknown> = {}): Doc {
  return { _id: id, _rev: rev(r), _updatedAt: rev(r), _deleted: false, _collection: collection, ...extra } as Doc;
}
const collections: Record<string, CollectionSchema> = { todos: { indexes: ['status'] } };
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const apply = (g: GossipSync, docs: Doc[]) => (g as any)._applyRemoteChanges(docs) as Promise<void>;

describe('change-listener error isolation (B8)', () => {
  beforeEach(() => {
    globalThis.indexedDB = new IDBFactory();
  });

  it('a throwing onChange listener does not reject the write and does not starve later listeners', async () => {
    // Run any queued microtask synchronously and capture its throw so the
    // deliberate listener error never surfaces as an unhandled rejection.
    const deferred: unknown[] = [];
    const spy = vi.spyOn(globalThis, 'queueMicrotask').mockImplementation((cb: () => void) => {
      try {
        cb();
      } catch (e) {
        deferred.push(e);
      }
    });

    try {
      const a = new IndexedDBAdapter(`b8-${Math.random()}`, 1, collections);
      await a.open();
      let secondRan = false;
      a.onChangeEntry(() => {
        throw new Error('listener-boom');
      });
      a.onChangeEntry(() => {
        secondRan = true;
      });

      // Must resolve (write is durable) despite the first listener throwing.
      const written = await a.put('todos', { _id: '1', status: 'open' });
      expect(written._id).toBe('1');
      expect(secondRan).toBe(true); // later listener still fired
      expect((await a.get('todos', '1'))?._id).toBe('1'); // durably committed
      expect(deferred.some((e) => (e as Error).message === 'listener-boom')).toBe(true);
      await a.close();
    } finally {
      spy.mockRestore();
    }
  });
});

describe('areStoresEmpty (P4)', () => {
  let a: IndexedDBAdapter;
  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    a = new IndexedDBAdapter(`p4-${Math.random()}`, 1, collections);
    await a.open();
  });

  it('reports empty/non-empty correctly and counts tombstones as non-empty', async () => {
    expect(await a.areStoresEmpty(['todos'])).toBe(true);
    await a.put('todos', { _id: '1', status: 'open' });
    expect(await a.areStoresEmpty(['todos'])).toBe(false);
    await a.delete('todos', '1'); // tombstone remains
    expect(await a.areStoresEmpty(['todos'])).toBe(false);
    expect(await a.areStoresEmpty([])).toBe(true);
    await a.close();
  });
});

describe('_applyRemoteChanges hardening (B4 HLC persist, B9 _collection strip)', () => {
  let adapter: IndexedDBAdapter;
  let gossip: GossipSync;
  const name = `apply-${Math.random()}`;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(name, 1, collections);
    await adapter.open();
    gossip = new GossipSync({} as never, adapter, collections, 0);
  });

  it('B9: the transport _collection tag is never persisted into the store', async () => {
    await apply(gossip, [remoteDoc('todos', 't1', 10, { status: 'open' })]);
    const stored = await adapter.get('todos', 't1');
    expect(stored).not.toBeNull();
    expect('_collection' in (stored as Record<string, unknown>)).toBe(false);
    // also not leaked through query
    const [q] = await adapter.query('todos', { where: { status: 'open' } });
    expect('_collection' in (q as Record<string, unknown>)).toBe(false);
  });

  it('B4: HLC watermark advanced by a remote apply survives a reopen', async () => {
    await apply(gossip, [remoteDoc('todos', 't1', 999999)]);
    expect(adapter.getHLC().now() >= rev(999999)).toBe(true);
    await adapter.close();

    // Reopen the SAME db (same IDBFactory, not reset) — watermark must be restored.
    const reopened = new IndexedDBAdapter(name, 1, collections);
    await reopened.open();
    expect(reopened.getHLC().now() >= rev(999999)).toBe(true);
    await reopened.close();
  });
});
