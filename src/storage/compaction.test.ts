import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from './indexeddb.js';
import type { Doc, HLCTimestamp } from '../core/types.js';

function makeAdapter(name = `compact-${Math.random()}`) {
  return new IndexedDBAdapter(name, 1, {
    todos: { indexes: ['status'] },
    notes: {},
  });
}

/** A horizon that makes EVERY existing record eligible (cutoff = now + 1s, so all
 *  past revs are "older"). Note: compact() also prunes the whole change log here. */
const PURGE_ALL = -1000;

describe('compaction — pruneTombstones / compact', () => {
  let adapter: IndexedDBAdapter;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = makeAdapter();
    await adapter.open();
  });

  // ── Criterion 1: physical purge ───────────────────────────────────────────────
  it('compact() physically removes an aged tombstone (gone even with includeDeleted)', async () => {
    await adapter.put('todos', { _id: 'x', title: 'bye', status: 'open' });
    await adapter.delete('todos', 'x');
    // The tombstone is physically present before compaction.
    expect(await adapter.get('todos', 'x')).not.toBeNull();
    expect((await adapter.query('todos', { includeDeleted: true })).length).toBe(1);
    const before = await adapter.estimateSizeBytes(['todos']);

    const result = await adapter.compact({ olderThanMs: PURGE_ALL });

    expect(result.tombstonesPurged).toBe(1);
    expect(result.bytesReclaimed).toBeGreaterThan(0);
    expect(await adapter.get('todos', 'x')).toBeNull();
    expect((await adapter.query('todos', { includeDeleted: true })).length).toBe(0);
    expect(await adapter.estimateSizeBytes(['todos'])).toBeLessThan(before);
  });

  it('compact() reports the number of collections scanned', async () => {
    const all = await adapter.compact({ olderThanMs: PURGE_ALL });
    expect(all.collections).toBe(2);
    const one = await adapter.compact({ olderThanMs: PURGE_ALL, collections: ['todos'] });
    expect(one.collections).toBe(1);
  });

  // ── Criterion 3 (local): tombstones within the horizon are retained ────────────
  it('compact() with the default horizon retains a fresh tombstone', async () => {
    await adapter.put('todos', { _id: 'x', status: 'open' });
    await adapter.delete('todos', 'x');

    const result = await adapter.compact(); // default 30-day horizon

    expect(result.tombstonesPurged).toBe(0);
    const t = await adapter.get('todos', 'x');
    expect(t).not.toBeNull();
    expect(t!._deleted).toBe(true);
  });

  // ── Safety clamp: never purge a tombstone whose delete delta still exists ───────
  it('pruneTombstones() refuses to purge while the deletion is still in the change log', async () => {
    await adapter.put('todos', { _id: 'x', status: 'open' });
    await adapter.delete('todos', 'x');

    // Even with an all-encompassing horizon, the primitive must NOT purge: the
    // deletion's change entry survives, so a lagging peer could still need the
    // delete delta (which would be doc-less if we purged now).
    const purged = await adapter.pruneTombstones(PURGE_ALL);
    expect(purged).toBe(0);
    expect(await adapter.get('todos', 'x')).not.toBeNull();

    // Once the change log is pruned past the deletion, purging becomes safe.
    await adapter.pruneChanges(PURGE_ALL);
    const purged2 = await adapter.pruneTombstones(PURGE_ALL);
    expect(purged2).toBe(1);
    expect(await adapter.get('todos', 'x')).toBeNull();
  });

  // ── Criterion 4: live data + indexes intact ────────────────────────────────────
  it('leaves live docs and their indexes untouched', async () => {
    await adapter.put('todos', { _id: 'live1', status: 'open' });
    await adapter.put('todos', { _id: 'live2', status: 'done' });
    await adapter.put('todos', { _id: 'dead', status: 'open' });
    await adapter.delete('todos', 'dead');

    const result = await adapter.compact({ olderThanMs: PURGE_ALL });
    expect(result.tombstonesPurged).toBe(1);

    // Live docs survive, full-fidelity.
    expect((await adapter.get('todos', 'live1'))?.['status']).toBe('open');
    expect((await adapter.get('todos', 'live2'))?.['status']).toBe('done');
    // Index-backed equality query still correct (and excludes the purged doc).
    const open = await adapter.query('todos', { where: { status: 'open' } });
    expect(open.map((d) => d._id).sort()).toEqual(['live1']);
    expect((await adapter.query('todos')).length).toBe(2);
  });

  // ── Criterion 5: idempotent + concurrent-safe ──────────────────────────────────
  it('is idempotent — a second compaction purges nothing', async () => {
    await adapter.put('todos', { _id: 'x', status: 'open' });
    await adapter.delete('todos', 'x');
    expect((await adapter.compact({ olderThanMs: PURGE_ALL })).tombstonesPurged).toBe(1);
    expect((await adapter.compact({ olderThanMs: PURGE_ALL })).tombstonesPurged).toBe(0);
  });

  it('survives writes interleaved with an in-flight compaction', async () => {
    for (let i = 0; i < 20; i++) {
      await adapter.put('todos', { _id: `t${i}`, status: 'open' });
      await adapter.delete('todos', `t${i}`);
    }
    // Kick off compaction and a concurrent live write without awaiting in between.
    const compaction = adapter.compact({ olderThanMs: PURGE_ALL });
    const write = adapter.put('todos', { _id: 'fresh', status: 'open' });
    const [result] = await Promise.all([compaction, write]);

    // The concurrently-written doc has a now-current _rev, so it is never eligible
    // for purge and must survive intact.
    expect(await adapter.get('todos', 'fresh')).not.toBeNull();
    expect(result.tombstonesPurged).toBeGreaterThanOrEqual(0);
    // Everything that remains is internally consistent: no tombstones older than
    // the oldest change entry linger after a follow-up compaction.
    await adapter.compact({ olderThanMs: PURGE_ALL });
    expect((await adapter.query('todos', { includeDeleted: true })).map((d) => d._id)).toEqual(['fresh']);
  });

  // ── Criterion 6: bounded memory — paging over a large store ─────────────────────
  it('pages a large store with a small batchSize (bounded peak) and purges all', async () => {
    const N = 60;
    for (let i = 0; i < N; i++) {
      await adapter.put('todos', { _id: `dead${i}`, status: 'x' });
      await adapter.delete('todos', `dead${i}`);
    }
    for (let i = 0; i < 10; i++) await adapter.put('todos', { _id: `live${i}`, status: 'y' });
    await adapter.pruneChanges(PURGE_ALL); // age out all deltas so every tombstone is eligible

    // Count readwrite transactions opened on the collection to prove paging.
    const realTx = adapter['db'].transaction.bind(adapter['db']);
    let rwTxOnTodos = 0;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (adapter as any).db.transaction = (stores: any, mode?: any, ...rest: any[]) => {
      if (mode === 'readwrite' && (stores === 'todos' || (Array.isArray(stores) && stores.includes('todos')))) {
        rwTxOnTodos++;
      }
      return realTx(stores, mode, ...rest);
    };

    const batchSize = 10;
    const purged = await adapter.pruneTombstones(PURGE_ALL, { collections: ['todos'], batchSize });

    expect(purged).toBe(N);
    // ceil(scanned / batchSize) pages, each its own bounded transaction → > 1.
    expect(rwTxOnTodos).toBeGreaterThan(1);
    expect((await adapter.query('todos')).length).toBe(10);
    expect((await adapter.query('todos', { includeDeleted: true })).length).toBe(10);
  });

  // ── predicate: replicated soft-delete of matching live docs ─────────────────────
  it('predicate soft-deletes matching live docs (replicated; not purged this run)', async () => {
    await adapter.put('todos', { _id: 'keep', status: 'open' });
    await adapter.put('todos', { _id: 'drop', status: 'done' });

    const result = await adapter.compact({ predicate: (d) => d['status'] === 'done' });

    // The matched live doc becomes a fresh tombstone (a replicated delete), not a
    // physical purge — it survives this compaction and only ages out later.
    expect(result.tombstonesPurged).toBe(0);
    expect((await adapter.get('todos', 'drop'))?._deleted).toBe(true);
    expect((await adapter.get('todos', 'keep'))?._deleted).toBe(false);
    expect((await adapter.query('todos')).map((d) => d._id)).toEqual(['keep']);
  });
});

describe('hardDelete — sync-unsafe physical removal', () => {
  let adapter: IndexedDBAdapter;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = makeAdapter();
    await adapter.open();
  });

  it('physically removes a record without ticking the HLC or logging a change', async () => {
    await adapter.put('todos', { _id: 'x', status: 'open' });
    const changesBefore = (await adapter.changes('' as HLCTimestamp)).length;
    const hlcBefore = adapter.getHLC().now();

    const removed = await adapter.hardDelete('todos', 'x');

    expect(removed).toBe(true);
    expect(await adapter.get('todos', 'x')).toBeNull();
    // No change-log entry appended, no clock advance — it is not a replicated event.
    expect((await adapter.changes('' as HLCTimestamp)).length).toBe(changesBefore);
    expect(adapter.getHLC().now()).toBe(hlcBefore);
  });

  it('returns false for a missing record', async () => {
    expect(await adapter.hardDelete('todos', 'nope')).toBe(false);
  });

  it('can remove an existing tombstone too', async () => {
    await adapter.put('notes', { _id: 'n' });
    await adapter.delete('notes', 'n');
    expect(((await adapter.query('notes', { includeDeleted: true })) as Doc[]).length).toBe(1);
    expect(await adapter.hardDelete('notes', 'n')).toBe(true);
    expect((await adapter.query('notes', { includeDeleted: true })).length).toBe(0);
  });
});
