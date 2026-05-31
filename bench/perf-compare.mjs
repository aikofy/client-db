// Before/after comparison for the snapshot + sync-apply hot paths, exercising
// the ACTUAL new code (_querySnapshotBatch keyset paging, batched
// _applyRemoteChanges) vs the old patterns. Run: bun bench/perf-compare.mjs
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from '../src/storage/indexeddb.ts';
import { GossipSync } from '../src/sync/gossip.ts';

let txRW = 0, txRO = 0;
const origTx = IDBDatabase.prototype.transaction;
IDBDatabase.prototype.transaction = function (s, mode, ...r) {
  if (mode === 'readwrite') txRW++; else txRO++;
  return origTx.call(this, s, mode, ...r);
};
const reset = () => { txRW = 0; txRO = 0; };
const now = () => Number(process.hrtime.bigint() / 1000n) / 1000;

function fresh(schema = { todos: { indexes: ['status'] } }) {
  globalThis.indexedDB = new IDBFactory();
  return new IndexedDBAdapter('cmp', 1, schema);
}
const rev = (n) => String(n).padStart(16, '0') + '-000000-node';
function makeDocs(n, coll = null) {
  const d = [];
  for (let i = 0; i < n; i++) {
    const o = { _id: `id-${String(i).padStart(6, '0')}`, _rev: rev(1000 + i), _updatedAt: rev(1000 + i), _deleted: false, status: 'open' };
    if (coll) o._collection = coll;
    d.push(o);
  }
  return d;
}

// ── SNAPSHOT export: old offset paging vs new keyset paging ────────────────
async function snapshotOld(a, name, batch) {
  let offset = 0, done = false;
  while (!done) {
    const d = await a.query(name, { limit: batch, offset, includeDeleted: true });
    done = d.length < batch; offset += d.length;
  }
}
async function snapshotNew(a, name, batch) {
  let afterId = null, done = false;
  while (!done) {
    const d = await a._querySnapshotBatch(name, afterId, batch);
    done = d.length < batch; if (d.length) afterId = d[d.length - 1]._id;
  }
}
console.log('\n=== SNAPSHOT EXPORT (offset O(n^2)  vs  keyset O(n)) ===');
for (const N of [2000, 8000, 16000]) {
  const a = fresh(); await a.open(); await a.bulkInsert('todos', makeDocs(N));
  let t = now(); await snapshotOld(a, 'todos', 500); const old = now() - t;
  t = now(); await snapshotNew(a, 'todos', 500); const neu = now() - t;
  console.log(`  N=${String(N).padStart(6)}  old ${old.toFixed(1).padStart(7)}ms   new ${neu.toFixed(1).padStart(6)}ms   speedup ${(old / neu).toFixed(1)}x`);
  await a.close();
}

// ── SYNC APPLY: old 1-by-1 (get + single bulkInsert) vs batched ────────────
console.log('\n=== SYNC APPLY 500 remote docs (transactions + time) ===');
{
  // OLD pattern, simulated exactly as the previous _applyRemoteChanges did.
  const a = fresh(); await a.open();
  const docs = makeDocs(500, 'todos');
  reset(); const t = now();
  const hlc = a.getHLC();
  for (const remote of docs) {
    hlc.update(remote._updatedAt);
    const local = await a.get('todos', remote._id);
    if (!local) await a.bulkInsert('todos', [remote]);
  }
  const dt = now() - t;
  console.log(`  OLD (per-doc)   ${dt.toFixed(1).padStart(6)}ms   tx: ${txRW} rw + ${txRO} ro = ${txRW + txRO}`);
  await a.close();
}
{
  // NEW: the real batched _applyRemoteChanges via GossipSync.
  const a = fresh(); await a.open();
  const gossip = new GossipSync({}, a, { todos: { indexes: ['status'] } }, 0);
  const docs = makeDocs(500, 'todos');
  reset(); const t = now();
  await gossip._applyRemoteChanges(docs);
  const dt = now() - t;
  console.log(`  NEW (batched)   ${dt.toFixed(1).padStart(6)}ms   tx: ${txRW} rw + ${txRO} ro = ${txRW + txRO}`);
  await a.close();
}

console.log('\nDone.');
