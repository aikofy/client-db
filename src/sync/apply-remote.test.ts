import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from '../storage/indexeddb.js';
import { GossipSync } from './gossip.js';
import type { Doc, HLCTimestamp, CollectionSchema } from '../core/types.js';

// Exercises the batched _applyRemoteChanges directly (no transport needed) to
// prove the prefetch + grouped-bulkInsert optimization keeps the exact same
// conflict-resolution, HLC-advancement, dedupe and conflict-logging semantics.

function rev(n: number, node = 'remote'): HLCTimestamp {
  return (String(n).padStart(16, '0') + `-000000-${node}`) as HLCTimestamp;
}

function remote(collection: string, id: string, r: number, extra: Record<string, unknown> = {}): Doc {
  return {
    _id: id,
    _rev: rev(r),
    _updatedAt: rev(r),
    _deleted: false,
    _collection: collection,
    ...extra,
  } as Doc;
}

const collections: Record<string, CollectionSchema> = {
  todos: { indexes: ['status'], conflictStrategy: 'lww' },
  notes: { conflictStrategy: 'first-write-wins' },
};

function makeGossip(adapter: IndexedDBAdapter): GossipSync {
  // Transport is never touched by _applyRemoteChanges.
  return new GossipSync({} as never, adapter, collections, 0);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const apply = (g: GossipSync, docs: Doc[]) => (g as any)._applyRemoteChanges(docs) as Promise<void>;

describe('_applyRemoteChanges (batched)', () => {
  let adapter: IndexedDBAdapter;
  let gossip: GossipSync;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(`ar-${Math.random()}`, 1, collections);
    await adapter.open();
    gossip = makeGossip(adapter);
  });

  it('inserts new docs across multiple collections', async () => {
    await apply(gossip, [
      remote('todos', 't1', 10, { status: 'open' }),
      remote('notes', 'n1', 11, { body: 'hi' }),
      remote('todos', 't2', 12, { status: 'done' }),
    ]);
    expect((await adapter.get('todos', 't1'))?._rev).toBe(rev(10));
    expect((await adapter.get('notes', 'n1'))?._rev).toBe(rev(11));
    expect((await adapter.get('todos', 't2'))?._rev).toBe(rev(12));
  });

  it('lww: higher-rev remote overwrites local; lower-rev remote is ignored', async () => {
    await apply(gossip, [remote('todos', 't1', 50, { status: 'open' })]);

    // Higher rev wins
    await apply(gossip, [remote('todos', 't1', 90, { status: 'win' })]);
    expect((await adapter.get('todos', 't1'))?.['status']).toBe('win');

    // Lower rev loses (no change)
    await apply(gossip, [remote('todos', 't1', 20, { status: 'stale' })]);
    expect((await adapter.get('todos', 't1'))?.['status']).toBe('win');
  });

  it('first-write-wins: lower-rev remote wins for the notes collection', async () => {
    await apply(gossip, [remote('notes', 'n1', 80, { body: 'late' })]);
    await apply(gossip, [remote('notes', 'n1', 30, { body: 'early' })]);
    // FWW keeps the lower rev
    expect((await adapter.get('notes', 'n1'))?.['body']).toBe('early');
  });

  it('identical rev is a no-op: no extra change-log entry written', async () => {
    const r = remote('todos', 't1', 40, { status: 'open' });
    await apply(gossip, [r]);
    const after1 = (await adapter.changes('' as HLCTimestamp)).length;
    await apply(gossip, [remote('todos', 't1', 40, { status: 'open' })]);
    const after2 = (await adapter.changes('' as HLCTimestamp)).length;
    expect(after2).toBe(after1); // nothing re-written
  });

  it('logs a conflict when resolution changes the local doc', async () => {
    await apply(gossip, [remote('todos', 't1', 50, { status: 'a' })]);
    await apply(gossip, [remote('todos', 't1', 70, { status: 'b' })]); // conflict → b wins, logged
    const conflicts = await adapter.query('_conflicts', { includeDeleted: true });
    expect(conflicts.length).toBe(1);
  });

  it('advances the HLC to at least the max remote _updatedAt', async () => {
    await apply(gossip, [
      remote('todos', 't1', 100),
      remote('todos', 't2', 999999),
      remote('notes', 'n1', 500),
    ]);
    expect(adapter.getHLC().now() >= rev(999999)).toBe(true);
  });

  it('dedupes duplicate ids within one batch: applied once', async () => {
    await apply(gossip, [
      remote('todos', 't1', 60, { status: 'open' }),
      remote('todos', 't1', 60, { status: 'open' }),
      remote('todos', 't1', 60, { status: 'open' }),
    ]);
    const entries = (await adapter.changes('' as HLCTimestamp)).filter((c) => c.id === 't1');
    expect(entries.length).toBe(1); // one write, not three
  });

  it('ignores docs for unknown collections but still advances HLC', async () => {
    await apply(gossip, [
      { _id: 'x', _rev: rev(123), _updatedAt: rev(123), _deleted: false, _collection: 'unknown' } as Doc,
    ]);
    expect(adapter.getHLC().now() >= rev(123)).toBe(true);
    expect(await adapter.get('todos', 'x')).toBeNull();
  });
});

describe('_applyRemoteChanges — hardening against malicious/buggy peers', () => {
  let adapter: IndexedDBAdapter;
  let gossip: GossipSync;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(`arh-${Math.random()}`, 1, collections);
    await adapter.open();
    gossip = makeGossip(adapter);
  });

  it('skips docs with malformed HLC timestamps without corrupting the clock', async () => {
    const before = adapter.getHLC().now();
    await apply(gossip, [
      { _id: 'bad1', _rev: 'garbage', _updatedAt: 'garbage', _deleted: false, _collection: 'todos' } as unknown as Doc,
      { _id: 'bad2', _rev: undefined, _updatedAt: undefined, _deleted: false, _collection: 'todos' } as unknown as Doc,
      remote('todos', 'good', 10, { status: 'open' }),
    ]);
    expect(await adapter.get('todos', 'bad1')).toBeNull();
    expect(await adapter.get('todos', 'bad2')).toBeNull();
    expect((await adapter.get('todos', 'good'))?._rev).toBe(rev(10));
    // Clock advanced normally (no NaN poisoning)
    const after = adapter.getHLC().now();
    expect(after >= before).toBe(true);
    expect(after.includes('NaN')).toBe(false);
  });

  it('skips docs without a usable string _id', async () => {
    await apply(gossip, [
      { _rev: rev(10), _updatedAt: rev(10), _deleted: false, _collection: 'todos' } as unknown as Doc,
      { _id: 42, _rev: rev(11), _updatedAt: rev(11), _deleted: false, _collection: 'todos' } as unknown as Doc,
      null as unknown as Doc,
    ]);
    expect((await adapter.changes('' as HLCTimestamp)).length).toBe(0);
  });

  it('rejects far-future timestamps (clock poisoning) and leaves the local HLC intact', async () => {
    const farFuture = (String(Date.now() + 365 * 24 * 60 * 60 * 1000).padStart(16, '0') +
      '-000000-evil') as HLCTimestamp;
    await apply(gossip, [
      { _id: 'p1', _rev: farFuture, _updatedAt: farFuture, _deleted: false, _collection: 'todos' } as Doc,
    ]);
    expect(await adapter.get('todos', 'p1')).toBeNull();
    // Local clock NOT dragged a year forward — a subsequent local write still
    // carries a near-now timestamp, so it can win LWW against honest peers.
    const localRev = (await adapter.put('todos', { _id: 'mine', status: 'open' }))._rev;
    expect(localRev < farFuture).toBe(true);
  });

  it('accepts realistic skew within maxClockDriftMs (default 24 h)', async () => {
    const skewMs = Date.now() + 60 * 60 * 1000; // 1 h ahead — plausible device skew
    const skewed = (String(skewMs).padStart(16, '0') + '-000000-skewed') as HLCTimestamp;
    await apply(gossip, [
      { _id: 's1', _rev: skewed, _updatedAt: skewed, _deleted: false, _collection: 'todos' } as Doc,
    ]);
    expect((await adapter.get('todos', 's1'))?._rev).toBe(skewed);
  });
});

describe('broadcastDocs (batched re-broadcast)', () => {
  it('sends one sync-response frame for a whole batch', async () => {
    globalThis.indexedDB = new IDBFactory();
    const adapter = new IndexedDBAdapter(`bb-${Math.random()}`, 1, collections);
    await adapter.open();

    const broadcasts: Array<{ changes: unknown[]; docs: Doc[] }> = [];
    const stubTransport = {
      peers: () => ['p1', 'p2'],
      broadcast: (m: { changes: unknown[]; docs: Doc[] }) => broadcasts.push(m),
    };
    const gossip = new GossipSync(stubTransport as never, adapter, collections, 0);

    // Wire like db.ts does: one broadcast per committed batch.
    adapter.onChangeBatch((collection, _entries, docs) => gossip.broadcastDocs(collection, docs));

    const docs = Array.from({ length: 5 }, (_, i) => remote('todos', `d${i}`, 100 + i));
    await adapter.bulkInsert('todos', docs);

    expect(broadcasts.length).toBe(1); // not 5
    expect(broadcasts[0].docs.length).toBe(5);
    expect(broadcasts[0].changes.length).toBe(5);
    expect((broadcasts[0].docs[0] as Doc & { _collection: string })._collection).toBe('todos');
  });
});
