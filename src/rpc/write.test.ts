import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { IndexedDBAdapter } from '../storage/indexeddb.js';
import { RpcServer } from './server.js';
import { RpcRouter } from './router.js';
import type { ChangeEntry, Doc } from '../core/types.js';
import type { ServerFrame } from './protocol.js';
import { waitFor } from '../test-utils/fake-webrtc.js';

beforeEach(() => {
  globalThis.indexedDB = new IDBFactory();
});

let adapter: IndexedDBAdapter;
afterEach(async () => {
  await adapter?.close();
});

interface CreateParams {
  title: string;
  ownerId?: string;
}

/** Build a server backed by a REAL IndexedDB adapter, capturing sent frames and change entries. */
async function setup() {
  adapter = new IndexedDBAdapter(`rpc-write-${Math.random()}`, 1, { todos: {} });
  await adapter.open();

  const changes: Array<{ entry: ChangeEntry; doc: Doc }> = [];
  adapter.onChangeEntry((entry, doc) => changes.push({ entry, doc }));

  let handlerCalls = 0;
  const router = new RpcRouter().write<CreateParams, { id: string }>('todos.create', {
    handler: (ctx, params) =>
      ctx.idempotent(ctx.request.idempotencyKey, async () => {
        handlerCalls += 1;
        // ownerId comes from the verified identity, NOT from client input.
        const doc = await ctx.db.put('todos', { ...params, ownerId: ctx.consumer.id });
        return { id: doc._id };
      }),
  });

  const sent: ServerFrame[] = [];
  const server = new RpcServer({
    router,
    adapter,
    hlc: () => adapter.getHLC().now(),
    nodeId: 'A',
    send: (_id, frame) => sent.push(frame),
  });

  server.onConnect('c');
  await server.handleMessage('c', { type: 'auth', token: 't', protocolVersion: 1 });
  return { server, sent, changes, getHandlerCalls: () => handlerCalls };
}

async function frameById(sent: ServerFrame[], id: string): Promise<ServerFrame> {
  let found: ServerFrame | undefined;
  await waitFor(() => {
    found = sent.find((f) => (f as { id?: string }).id === id);
    return !!found;
  });
  return found!;
}

describe('RPC write path (Phase 5)', () => {
  it('persists the write, fires the change log (replication trigger), and stamps identity from the token', async () => {
    const { server, sent, changes } = await setup();

    await server.handleMessage('c', {
      type: 'req',
      id: '1',
      method: 'todos.create',
      params: { title: 'hello', ownerId: 'evil-attacker' }, // attempt to spoof owner
      idempotencyKey: 'k1',
    });

    const res = await frameById(sent, '1');
    if (res.type !== 'res') throw new Error('expected res');
    const body = res.body as { id: string };

    // (a) persisted
    const stored = await adapter.get('todos', body.id);
    expect(stored).not.toBeNull();
    // (b) identity from token (stub identity id = consumerId 'c'), NOT the client-supplied ownerId
    expect((stored as Record<string, unknown>)['ownerId']).toBe('c');
    // (c) the write fired onChangeEntry — this is what gossip.broadcastDoc hooks into
    expect(changes.some((ch) => ch.entry.id === body.id && ch.entry.operation === 'put')).toBe(true);
    // (d) the response carries an HLC watermark (read-your-writes)
    expect(typeof res.hlc).toBe('string');
  });

  it('an idempotent replay runs the handler once and returns the same result', async () => {
    const { server, sent, getHandlerCalls } = await setup();

    const req = (id: string) => ({
      type: 'req' as const,
      id,
      method: 'todos.create',
      params: { title: 'dup' },
      idempotencyKey: 'same-key',
    });

    await server.handleMessage('c', req('1'));
    const res1 = await frameById(sent, '1');
    await server.handleMessage('c', req('2'));
    const res2 = await frameById(sent, '2');

    if (res1.type !== 'res' || res2.type !== 'res') throw new Error('expected res');
    expect(getHandlerCalls()).toBe(1); // handler body ran once
    expect(res2.body).toEqual(res1.body); // replay got the cached result

    const all = await adapter.query('todos');
    expect(all.length).toBe(1); // only one doc created
  });
});
