import { describe, it, expect, beforeEach, vi } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from '../storage/indexeddb.js';
import { GossipSync } from './gossip.js';
import { formatHLC } from '../core/hlc.js';
import type { CollectionSchema, Doc, HLCTimestamp, SyncMessage } from '../core/types.js';

// End-to-end sync-correctness of compaction: two real GossipSync nodes wired
// together by an in-memory transport (no browser / WebRTC). Proves the
// no-resurrection boundary (compacted, beyond-TTL deletions force needsFullSync)
// and that within-TTL deletions still propagate as deltas after compaction.

const DAY = 24 * 60 * 60 * 1000;
const collections: Record<string, CollectionSchema> = { todos: { indexes: ['status'] } };

function hlcAt(physicalMs: number, node = 'remote'): HLCTimestamp {
  return formatHLC({ physicalMs, counter: 0, nodeId: node });
}

/**
 * Minimal in-memory transport: one of a linked pair. `send`/`sendAsync` deliver
 * (JSON-cloned, async) to the partner's `onMessage`, tagged with THIS node's id —
 * exactly the shape GossipSync drives. No timers, fully deterministic.
 */
class LinkedTransport {
  onMessage: (peerId: string, msg: SyncMessage) => void = () => {};
  onPeerConnected: (peerId: string) => void = () => {};
  onPeerDisconnected: (peerId: string) => void = () => {};
  link!: LinkedTransport;
  online = true;
  readonly config: { nodeId: string };
  constructor(public myNodeId: string) {
    this.config = { nodeId: myNodeId };
  }
  peers(): string[] {
    return this.online && this.link.online ? [this.link.myNodeId] : [];
  }
  send(_peerId: string, msg: SyncMessage): void {
    if (!this.online || !this.link.online) return;
    const clone = JSON.parse(JSON.stringify(msg)) as SyncMessage;
    queueMicrotask(() => {
      if (this.online && this.link.online) this.link.onMessage(this.myNodeId, clone);
    });
  }
  sendAsync(peerId: string, msg: SyncMessage): Promise<void> {
    this.send(peerId, msg);
    return Promise.resolve();
  }
  broadcast(msg: SyncMessage): void {
    this.send(this.link.myNodeId, msg);
  }
}

interface Node {
  adapter: IndexedDBAdapter;
  gossip: GossipSync;
  transport: LinkedTransport;
}

async function makeNode(tag: string): Promise<Node> {
  const adapter = new IndexedDBAdapter(`csync-${tag}-${Math.random()}`, 1, collections);
  await adapter.open();
  const transport = new LinkedTransport(adapter.nodeId);
  // ttlDays = 30 so _handleSyncRequest enforces the needsFullSync watermark.
  const gossip = new GossipSync(transport as never, adapter, collections, 30);
  return { adapter, gossip, transport };
}

function linkNodes(a: Node, b: Node): void {
  a.transport.link = b.transport;
  b.transport.link = a.transport;
  // Wire each transport's inbound to its gossip's message handler (what start() does).
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  a.transport.onMessage = (peerId, msg) => void (a.gossip as any)._handleMessage(peerId, msg);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  b.transport.onMessage = (peerId, msg) => void (b.gossip as any)._handleMessage(peerId, msg);
}

/** Drive one node's pull+push sync against a peer (the unit of a gossip round), then
 *  let any in-flight async settle. The snapshot receiver resolves on `stream-end`
 *  without awaiting the final batch's `bulkInsert`; over real WebRTC that's masked by
 *  network latency, but this zero-latency transport needs an explicit drain so the
 *  bootstrap is durable before we assert. */
async function sync(node: Node, peerNodeId: string): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  await ((node.gossip as any)._syncWithPeer(peerNodeId) as Promise<void>);
  await new Promise((r) => setTimeout(r, 15));
}

describe('compaction sync-correctness (two-node)', () => {
  let A: Node;
  let B: Node;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    A = await makeNode('A');
    B = await makeNode('B');
    linkNodes(A, B);
  });

  // ── Criterion 3: within TTL, a deletion still propagates as a delta ─────────────
  it('retains a within-TTL tombstone through compaction; the delete still syncs as a delta', async () => {
    await A.adapter.put('todos', { _id: 'x', status: 'open' });
    await sync(B, A.transport.myNodeId);
    expect((await B.adapter.get('todos', 'x'))?._deleted).toBe(false); // B learned the live doc

    await A.adapter.delete('todos', 'x');
    const result = await A.adapter.compact(); // default 30-day horizon
    expect(result.tombstonesPurged).toBe(0); // fresh tombstone retained
    expect((await A.adapter.get('todos', 'x'))?._deleted).toBe(true);

    // B, online and within TTL, receives the deletion as an ordinary delta.
    await sync(B, A.transport.myNodeId);
    expect((await B.adapter.get('todos', 'x'))?._deleted).toBe(true);
  });

  // ── Criterion 2: beyond TTL, a compacted deletion never resurrects ──────────────
  it('a fresh peer bootstrapping from a compacted replica never receives the purged doc', async () => {
    // A had X long ago and deleted it long ago (crafted aged tombstone), plus a
    // recent unrelated write so the change log keeps a recent oldest watermark.
    const oldRev = hlcAt(Date.now() - 60 * DAY, A.adapter.nodeId);
    await A.adapter.bulkInsert('todos', [
      { _id: 'x', _rev: oldRev, _updatedAt: oldRev, _deleted: true, status: 'open' } as Doc,
    ]);
    await A.adapter.put('todos', { _id: 'z', status: 'recent' });

    const result = await A.adapter.compact(); // 30-day horizon → aged tombstone purged
    expect(result.tombstonesPurged).toBe(1);
    expect(await A.adapter.get('todos', 'x')).toBeNull();

    // Empty B bootstraps from A: since='' < oldest → needsFullSync → snapshot, which
    // contains neither the live doc nor its tombstone, so X is correctly absent.
    await sync(B, A.transport.myNodeId);
    expect(await B.adapter.get('todos', 'z')).not.toBeNull(); // proof: B re-bootstrapped
    expect(await B.adapter.get('todos', 'x')).toBeNull(); // stays absent
  });

  it('a long-offline peer still holding the live doc is forced to re-bootstrap and cannot resurrect it on the compactor', async () => {
    // A: aged deletion of X + a recent write to keep the oldest watermark recent.
    const oldRev = hlcAt(Date.now() - 60 * DAY, A.adapter.nodeId);
    await A.adapter.bulkInsert('todos', [
      { _id: 'x', _rev: oldRev, _updatedAt: oldRev, _deleted: true, status: 'open' } as Doc,
    ]);
    await A.adapter.put('todos', { _id: 'z', status: 'recent' });
    expect((await A.adapter.compact()).tombstonesPurged).toBe(1);
    expect(await A.adapter.get('todos', 'x')).toBeNull();

    // B: still holds a (stale) LIVE copy of X and last synced before the deletion.
    const xRev = hlcAt(Date.now() - 65 * DAY, A.adapter.nodeId);
    await B.adapter.bulkInsert('todos', [
      { _id: 'x', _rev: xRev, _updatedAt: xRev, _deleted: false, status: 'open' } as Doc,
    ]);
    await B.adapter.setMetaValue(`lastSync:${A.transport.myNodeId}`, hlcAt(Date.now() - 70 * DAY));

    // B reconnects → its stale watermark predates A's oldest change → needsFullSync.
    // The needsFullSync path pulls a snapshot and SKIPS B's push, so B never sends
    // its stale live X to A. A second round must not push it either (B's watermark
    // has advanced past the old _rev).
    await sync(B, A.transport.myNodeId);
    expect(await A.adapter.get('todos', 'x')).toBeNull(); // no resurrection on the compactor
    expect(await B.adapter.get('todos', 'z')).not.toBeNull(); // B did re-bootstrap

    await sync(B, A.transport.myNodeId);
    expect(await A.adapter.get('todos', 'x')).toBeNull(); // still absent across rounds
  });
});

describe('autoCompact maintenance cycle', () => {
  let adapter: IndexedDBAdapter;

  beforeEach(async () => {
    globalThis.indexedDB = new IDBFactory();
    adapter = new IndexedDBAdapter(`autocompact-${Math.random()}`, 1, collections);
    await adapter.open();
  });

  /** A transport stub that satisfies GossipSync.start() without any real I/O. */
  function stubTransport(): never {
    return {
      onMessage: () => {},
      onPeerConnected: () => {},
      onPeerDisconnected: () => {},
      peers: () => [],
      send: () => {},
      sendAsync: () => Promise.resolve(),
      broadcast: () => {},
    } as never;
  }

  it('purges aged tombstones on the prune cycle when enabled, and not when disabled', async () => {
    // An aged deletion plus a recent write (keeps the oldest watermark recent).
    const oldRev = hlcAt(Date.now() - 60 * DAY, adapter.nodeId);
    await adapter.bulkInsert('todos', [
      { _id: 'x', _rev: oldRev, _updatedAt: oldRev, _deleted: true, status: 'open' } as Doc,
    ]);
    await adapter.put('todos', { _id: 'z', status: 'recent' });

    const pruneSpy = vi.spyOn(adapter, 'pruneTombstones');

    // autoCompact OFF → the cycle prunes the change log but keeps the tombstone.
    const off = new GossipSync(stubTransport(), adapter, collections, 30, undefined, false);
    off.start();
    await new Promise((r) => setTimeout(r, 20));
    off.stop();
    expect(pruneSpy).not.toHaveBeenCalled();
    expect(await adapter.get('todos', 'x')).not.toBeNull();

    // autoCompact ON → the same cycle physically reclaims it (right after pruneChanges).
    const on = new GossipSync(stubTransport(), adapter, collections, 30, undefined, true);
    on.start();
    await new Promise((r) => setTimeout(r, 20));
    on.stop();
    expect(pruneSpy).toHaveBeenCalled();
    expect(await adapter.get('todos', 'x')).toBeNull();
    expect(await adapter.get('todos', 'z')).not.toBeNull();
  });
});
