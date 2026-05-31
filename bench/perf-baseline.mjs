// Baseline perf probe — measures IDB transaction counts & algorithmic scaling
// against the CURRENT implementation. Run: bun bench/perf-baseline.mjs
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from '../src/storage/indexeddb.ts';

// ── instrument: count transactions by mode ────────────────────────────────
let txReadwrite = 0, txReadonly = 0;
const origTx = IDBDatabase.prototype.transaction;
IDBDatabase.prototype.transaction = function (stores, mode, ...rest) {
  if (mode === 'readwrite') txReadwrite++; else txReadonly++;
  return origTx.call(this, stores, mode, ...rest);
};
const resetTx = () => { txReadwrite = 0; txReadonly = 0; };

function freshDB() {
  globalThis.indexedDB = new IDBFactory();
  return new IndexedDBAdapter('bench', 1, { todos: { indexes: ['status'] } });
}

const now = () => Number(process.hrtime.bigint() / 1000n) / 1000; // ms

// ── 1. write path: transactions per put ───────────────────────────────────
{
  const a = freshDB(); await a.open();
  resetTx();
  const N = 200;
  const t0 = now();
  for (let i = 0; i < N; i++) await a.put('todos', { _id: 't' + i, status: 'open', n: i });
  const dt = now() - t0;
  console.log(`\n[WRITE] ${N} puts: ${dt.toFixed(1)}ms  (${(dt / N).toFixed(3)} ms/put)`);
  console.log(`        readwrite tx: ${txReadwrite}  (= ${(txReadwrite / N).toFixed(2)} per put)  readonly tx: ${txReadonly}`);
  await a.close();
}

// ── 2. query with where on an INDEXED field (full scan today?) ─────────────
{
  const a = freshDB(); await a.open();
  const N = 5000;
  const docs = [];
  for (let i = 0; i < N; i++) docs.push({ _id: 'd' + i, _rev: ('' + i).padStart(22, '0'), _updatedAt: ('' + i).padStart(22, '0'), _deleted: false, status: i === 0 ? 'open' : 'closed' });
  await a.bulkInsert('todos', docs);
  resetTx();
  const t0 = now();
  let r;
  for (let k = 0; k < 50; k++) r = await a.query('todos', { where: { status: 'open' } });
  const dt = now() - t0;
  console.log(`\n[QUERY] 50x where{status:'open'} over ${N} docs (1 match): ${dt.toFixed(1)}ms  (${(dt / 50).toFixed(3)} ms/query)`);
  console.log(`        matched: ${r.length}  readonly tx: ${txReadonly}  (index 'status' exists but unused?)`);
  await a.close();
}

// ── 3. snapshot-style offset pagination — O(n^2) check ─────────────────────
async function snapshotScan(a, name, batch) {
  let offset = 0, done = false, batches = 0;
  while (!done) {
    const d = await a.query(name, { limit: batch, offset, includeDeleted: true });
    done = d.length < batch;
    offset += d.length; batches++;
  }
  return batches;
}
{
  for (const N of [1000, 2000, 4000, 8000]) {
    const a = freshDB(); await a.open();
    const docs = [];
    for (let i = 0; i < N; i++) docs.push({ _id: 'd' + i, _rev: ('' + i).padStart(22, '0'), _updatedAt: ('' + i).padStart(22, '0'), _deleted: false, status: 'open' });
    await a.bulkInsert('todos', docs);
    const t0 = now();
    const batches = await snapshotScan(a, 'todos', 500);
    const dt = now() - t0;
    console.log(`[SNAPSHOT] export ${N} docs (batch 500, ${batches} batches): ${dt.toFixed(1)}ms  (${(dt / N * 1000).toFixed(1)} us/doc)`);
    await a.close();
  }
  console.log('        ^ if us/doc grows with N, paging is super-linear (O(n^2)).');
}

// ── 4. applyRemoteChanges-style: get+single-doc bulkInsert per doc ─────────
{
  const a = freshDB(); await a.open();
  const N = 500;
  resetTx();
  const t0 = now();
  for (let i = 0; i < N; i++) {
    await a.get('todos', 'r' + i); // local lookup (miss)
    await a.bulkInsert('todos', [{ _id: 'r' + i, _rev: ('' + i).padStart(22, '0'), _updatedAt: ('' + i).padStart(22, '0'), _deleted: false, status: 'open' }]);
  }
  const dt = now() - t0;
  console.log(`\n[SYNC-APPLY] apply ${N} remote docs (current 1-by-1): ${dt.toFixed(1)}ms`);
  console.log(`        readwrite tx: ${txReadwrite} (${(txReadwrite / N).toFixed(2)}/doc)  readonly tx: ${txReadonly} (${(txReadonly / N).toFixed(2)}/doc)`);
  await a.close();
}

console.log('\nDone.');
