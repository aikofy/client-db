import { describe, it, expect, afterEach } from 'vitest';
import { ConsumerClient } from './consumer.js';
import { WebRTCTransport } from './sync/webrtc-transport.js';
import { RpcServer } from './rpc/server.js';
import { RpcRouter } from './rpc/router.js';
import { RpcError } from './rpc/errors.js';
import type { Doc, HLCTimestamp, IStorageAdapter, QueryOptions } from './core/types.js';
import { installFakeWebRTC, uninstallFakeWebRTC, type FakeSignalingHub } from './test-utils/fake-webrtc.js';

afterEach(() => uninstallFakeWebRTC());

function fakeAdapter(rows: Array<Record<string, unknown>>): IStorageAdapter {
  return {
    query: async (_collection: string, _options?: QueryOptions) => rows as unknown as Doc[],
  } as unknown as IStorageAdapter;
}

/** Stand up a Normal Client transport + RpcServer wired through the fake hub. */
function setupNormal(router: RpcRouter, adapter: IStorageAdapter): WebRTCTransport {
  const transport = new WebRTCTransport({
    signalingServerUrl: 'ws://fake',
    iceServers: [],
    nodeId: 'normalA',
    room: 'r1',
  });
  const server = new RpcServer({
    router,
    adapter,
    hlc: () => 'hlc-1' as HLCTimestamp,
    nodeId: 'normalA',
    send: (id, frame) => transport.sendToConsumer(id, frame),
  });
  transport.onConsumerConnected = (id) => server.onConnect(id);
  transport.onConsumerDisconnected = (id) => server.onDisconnect(id);
  transport.onConsumerMessage = (id, data) => void server.handleMessage(id, data);
  transport.connect();
  return transport;
}

function makeConsumer(hub: FakeSignalingHub): ConsumerClient {
  void hub; // the hub is active via installFakeWebRTC(); ConsumerClient uses the global WebSocket
  return new ConsumerClient({
    signalingServerUrl: 'ws://fake',
    room: 'r1',
    nodeId: 'cons1',
    auth: { getToken: () => 'token-123' },
    deadlineMs: 2000,
  });
}

describe('ConsumerClient end-to-end (Phase 3)', () => {
  it('connects, authenticates, and invokes a read handler', async () => {
    const rows = [
      { _id: '1', owner: 'cons1', title: 'mine' },
      { _id: '2', owner: 'other', title: 'theirs' },
    ];
    const router = new RpcRouter().read('todos.listMine', {
      handler: async (ctx) => {
        const all = await ctx.db.query('todos');
        return all
          .filter((d) => (d as Record<string, unknown>)['owner'] === ctx.consumer.id)
          .map((d) => ({ id: d._id }));
      },
    });
    const hub = installFakeWebRTC();
    const transport = setupNormal(router, fakeAdapter(rows));
    const consumer = makeConsumer(hub);

    await consumer.connect();
    expect(consumer.methods).toContainEqual({ name: 'todos.listMine', kind: 'read', version: 1 });

    const result = await consumer.invoke('todos.listMine');
    expect(result).toEqual([{ id: '1' }]);

    consumer.close();
    transport.disconnect();
  });

  it('connects implicitly on the first invoke', async () => {
    const router = new RpcRouter().read('ping', { handler: () => 'pong' });
    const hub = installFakeWebRTC();
    const transport = setupNormal(router, fakeAdapter([]));
    const consumer = makeConsumer(hub);

    const result = await consumer.invoke('ping'); // no explicit connect()
    expect(result).toBe('pong');

    consumer.close();
    transport.disconnect();
  });

  it('surfaces a handler RpcError as a rejected RpcError with status', async () => {
    const router = new RpcRouter().read('secret', {
      handler: () => {
        throw new RpcError('PERMISSION_DENIED', 'nope');
      },
    });
    const hub = installFakeWebRTC();
    const transport = setupNormal(router, fakeAdapter([]));
    const consumer = makeConsumer(hub);

    await expect(consumer.invoke('secret')).rejects.toMatchObject({
      name: 'RpcError',
      status: 'PERMISSION_DENIED',
    });

    consumer.close();
    transport.disconnect();
  });

  it('passes typed params through to the handler', async () => {
    const router = new RpcRouter().read<{ n: number }, number>('double', {
      handler: (_ctx, { n }) => n * 2,
    });
    const hub = installFakeWebRTC();
    const transport = setupNormal(router, fakeAdapter([]));
    const consumer = makeConsumer(hub);

    const result = await consumer.invoke('double', { n: 21 });
    expect(result).toBe(42);

    consumer.close();
    transport.disconnect();
  });

  it('a cancelled invoke rejects with CANCELLED', async () => {
    const router = new RpcRouter().read('slow', {
      handler: () => new Promise(() => undefined),
    });
    const hub = installFakeWebRTC();
    const transport = setupNormal(router, fakeAdapter([]));
    const consumer = makeConsumer(hub);
    await consumer.connect();

    const ac = new AbortController();
    const p = consumer.invoke('slow', undefined, { signal: ac.signal });
    ac.abort();
    await expect(p).rejects.toMatchObject({ name: 'RpcError', status: 'CANCELLED' });

    consumer.close();
    transport.disconnect();
  });
});
