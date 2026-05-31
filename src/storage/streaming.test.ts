import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from './indexeddb.js';
import { createDB } from '../db.js';
import {
  snapshotChunksToNdjsonStream,
  ndjsonStreamToSnapshotChunks,
} from '../snapshot-stream.js';
import type { Doc, HLCTimestamp, SnapshotChunk } from '../core/types.js';

function rev(n: number): HLCTimestamp {
  return (String(n).padStart(16, '0') + '-000000-node') as HLCTimestamp;
}

function buildDocs(prefix: string, n: number): Doc[] {
  const docs: Doc[] = [];
  for (let i = 0; i < n; i++) {
    docs.push({
      _id: `${prefix}-${String(i).padStart(5, '0')}`,
      _rev: rev(1000 + i),
      _updatedAt: rev(1000 + i),
      _deleted: i % 9 === 0,
      status: i % 2 === 0 ? 'open' : 'done',
      n: i,
    } as Doc);
  }
  return docs;
}

const schema = { todos: { indexes: ['status'] }, notes: { indexes: [] } };

async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of it) out.push(x);
  return out;
}
async function* fromArray<T>(arr: T[]): AsyncGenerator<T> {
  for (const x of arr) yield x;
}

// Sort by _id so comparisons are order-independent of storage internals.
const byId = (a: Doc, b: Doc) => (a._id < b._id ? -1 : a._id > b._id ? 1 : 0);

describe('streaming export/import', () => {
  let src: IndexedDBAdapter;
  const todos = buildDocs('t', 1500);
  const notes = buildDocs('n', 1000);

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    src = new IndexedDBAdapter(`src-${Math.random()}`, 1, schema);
    await src.open();
    await src.bulkInsert('todos', todos);
    await src.bulkInsert('notes', notes);
  });

  it('exportStream pages: one header + multiple bounded batches (no batch exceeds batchSize)', async () => {
    const chunks = await collect(src.exportStream(500));
    const headers = chunks.filter((c) => c.kind === 'header');
    const batches = chunks.filter((c) => c.kind === 'batch');

    expect(headers.length).toBe(1);
    // 1500 todos / 500 = 3 batches, 1000 notes / 500 = 2 batches.
    expect(batches.length).toBe(5);
    for (const b of batches) {
      if (b.kind === 'batch') expect(b.docs.length).toBeLessThanOrEqual(500);
    }
  });

  it('round-trips all docs (including tombstones) into a fresh DB', async () => {
    const chunks = await collect(src.exportStream(500));
    const header = chunks.find((c) => c.kind === 'header');

    const dst = new IndexedDBAdapter(`dst-${Math.random()}`, 1, schema);
    await dst.open();
    await dst.importStream(fromArray(chunks));

    const srcTodos = (await src.query('todos', { includeDeleted: true })).sort(byId);
    const dstTodos = (await dst.query('todos', { includeDeleted: true })).sort(byId);
    const srcNotes = (await src.query('notes', { includeDeleted: true })).sort(byId);
    const dstNotes = (await dst.query('notes', { includeDeleted: true })).sort(byId);

    expect(dstTodos).toEqual(srcTodos);
    expect(dstNotes).toEqual(srcNotes);
    // HLC watermark restored from the header.
    if (header && header.kind === 'header') {
      expect(dst.getHLC().now() >= header.hlc).toBe(true);
    }
    await dst.close();
  });

  it('round-trips through an NDJSON byte stream (bounded-memory backup format)', async () => {
    const bytes = snapshotChunksToNdjsonStream(src.exportStream(400));

    // Tee so we can both inspect the raw NDJSON and feed the import.
    const [a, b] = bytes.tee();

    // (a) the wire format is newline-delimited JSON objects with a 'kind'.
    const text = await new Response(a).text();
    const lines = text.split('\n').filter((l) => l.length > 0);
    expect(lines.length).toBeGreaterThan(1);
    for (const line of lines) {
      const obj = JSON.parse(line) as SnapshotChunk;
      expect(obj.kind === 'header' || obj.kind === 'batch').toBe(true);
    }

    // (b) decode → import → data matches the source.
    const dst = new IndexedDBAdapter(`ndjson-${Math.random()}`, 1, schema);
    await dst.open();
    await dst.importStream(ndjsonStreamToSnapshotChunks(b));

    const srcAll = (await src.query('todos', { includeDeleted: true })).sort(byId);
    const dstAll = (await dst.query('todos', { includeDeleted: true })).sort(byId);
    expect(dstAll).toEqual(srcAll);
    await dst.close();
  });

  it('importStream skips unknown / non-existent collections without throwing', async () => {
    const chunks: SnapshotChunk[] = [
      { kind: 'header', version: 1, hlc: rev(5000), meta: {} },
      { kind: 'batch', collection: 'ghost', docs: buildDocs('g', 3) },
      { kind: 'batch', collection: 'todos', docs: buildDocs('t', 3) },
    ];
    const dst = new IndexedDBAdapter(`skip-${Math.random()}`, 1, schema);
    await dst.open();
    await expect(dst.importStream(fromArray(chunks))).resolves.toBeUndefined();
    expect((await dst.query('todos', { includeDeleted: true })).length).toBe(3);
    await dst.close();
  });
});

describe('scan() streaming iterator', () => {
  let adapter: IndexedDBAdapter;
  const todos = buildDocs('t', 1500);

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(`scan-${Math.random()}`, 1, schema);
    await adapter.open();
    await adapter.bulkInsert('todos', todos);
  });

  it('yields the same docs as an equivalent query(), in _id order', async () => {
    const scanned = await collect(adapter.scan('todos', { where: { status: 'open' } }));
    const queried = await adapter.query('todos', { where: { status: 'open' } });
    expect(scanned).toEqual(queried);

    // ascending _id, no duplicates
    for (let i = 1; i < scanned.length; i++) {
      expect(scanned[i]._id > scanned[i - 1]._id).toBe(true);
    }
  });

  it('excludes tombstones by default and includes them with includeDeleted', async () => {
    const live = await collect(adapter.scan('todos'));
    const all = await collect(adapter.scan('todos', { includeDeleted: true }));
    expect(live.every((d) => !d._deleted)).toBe(true);
    expect(all.length).toBeGreaterThan(live.length);
    expect(all.length).toBe(todos.length);
  });

  it('respects a small batchSize (still returns the full filtered set)', async () => {
    const scanned = await collect(adapter.scan('todos', { where: { status: 'done' }, batchSize: 7 }));
    const queried = await adapter.query('todos', { where: { status: 'done' } });
    expect(scanned).toEqual(queried);
  });
});

describe('review regressions', () => {
  beforeEach(() => {
    globalThis.indexedDB = new IDBFactory();
  });

  it('batchSize=0 does not hang exportStream and still round-trips (clamped to 1)', async () => {
    const a = new IndexedDBAdapter(`bs0-${Math.random()}`, 1, schema);
    await a.open();
    await a.bulkInsert('todos', buildDocs('t', 5));

    // Bounded collect — if the clamp regressed, this throws instead of hanging.
    const chunks: SnapshotChunk[] = [];
    let guard = 0;
    for await (const c of a.exportStream(0)) {
      chunks.push(c);
      if (++guard > 1000) throw new Error('exportStream did not terminate with batchSize=0');
    }
    const batches = chunks.filter((c) => c.kind === 'batch');
    // clamped to 1 → one batch per doc
    expect(batches.length).toBe(5);

    const scanned = await collect(a.scan('todos', { batchSize: 0, includeDeleted: true }));
    expect(scanned.length).toBe(5);
    await a.close();
  });

  it('scan honors a _updatedAt lower bound identically to query (range)', async () => {
    const a = new IndexedDBAdapter(`ua-${Math.random()}`, 1, schema);
    await a.open();
    const docs = buildDocs('t', 20);
    await a.bulkInsert('todos', docs);
    const mid = docs[9]._updatedAt; // exclusive lower bound

    const q = await a.query('todos', { where: { _updatedAt: mid }, includeDeleted: true });
    const s = await collect(a.scan('todos', { where: { _updatedAt: mid }, includeDeleted: true }));
    const qLimited = await a.query('todos', { where: { _updatedAt: mid }, includeDeleted: true, limit: 1000 });

    expect(s.map((d) => d._id).sort()).toEqual(q.map((d) => d._id).sort());
    // query() limit fast-path now honors _updatedAt too
    expect(qLimited.map((d) => d._id).sort()).toEqual(q.map((d) => d._id).sort());
    expect(q.every((d) => (d._updatedAt as string) > (mid as string))).toBe(true);
    await a.close();
  });

  it('NDJSON decoder tolerates CRLF and blank lines', async () => {
    const lines = [
      JSON.stringify({ kind: 'header', version: 1, hlc: rev(1), meta: {} }),
      '', // blank line
      JSON.stringify({ kind: 'batch', collection: 'todos', docs: buildDocs('t', 2) }),
    ];
    const text = lines.join('\r\n') + '\r\n\r\n'; // CRLF + trailing blank line
    const bytes = new TextEncoder().encode(text);
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        // split into two reads mid-record to exercise boundary buffering
        controller.enqueue(bytes.slice(0, 20));
        controller.enqueue(bytes.slice(20));
        controller.close();
      },
    });
    const out = await collect(ndjsonStreamToSnapshotChunks(stream));
    expect(out.length).toBe(2);
    expect(out[0].kind).toBe('header');
    expect(out[1].kind).toBe('batch');
  });

  it('NDJSON decoder cancels the upstream source when the consumer breaks early', async () => {
    let cancelled = false;
    const payload = new TextEncoder().encode(
      JSON.stringify({ kind: 'batch', collection: 'todos', docs: buildDocs('t', 1) }) + '\n',
    );
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(payload); // never closes on its own
      },
      cancel() {
        cancelled = true;
      },
    });

    for await (const _chunk of ndjsonStreamToSnapshotChunks(stream)) {
      void _chunk;
      break; // early break → generator return() → finally → reader.cancel()
    }
    expect(cancelled).toBe(true);
  });
});

describe('public API: db.todos.scan + db.exportStream/importStream', () => {
  beforeEach(() => {
    globalThis.indexedDB = new IDBFactory();
  });

  it('scan + streaming backup/restore work through createDB', async () => {
    const a = await createDB({ name: `pub-${Math.random()}`, version: 1, collections: schema });
    for (const d of buildDocs('t', 300)) await a.todos.put(d as never);

    // proxy scan yields filtered docs lazily
    let count = 0;
    for await (const doc of a.todos.scan({ where: { status: 'open' } })) {
      expect(doc.status).toBe('open');
      count++;
    }
    expect(count).toBeGreaterThan(0);

    // backup via NDJSON, restore into a second db
    const bytes = snapshotChunksToNdjsonStream(a.exportStream(100));
    const b = await createDB({ name: `pub2-${Math.random()}`, version: 1, collections: schema });
    await b.importStream(ndjsonStreamToSnapshotChunks(bytes));

    const restored = await b.todos.query({ where: { status: 'open' } });
    expect(restored.length).toBe(count);

    await a.close();
    await b.close();
  });
});
