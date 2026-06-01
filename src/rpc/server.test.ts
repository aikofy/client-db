import { describe, it, expect, afterEach } from 'vitest';
import { WebRTCTransport } from '../sync/webrtc-transport.js';
import { RpcServer } from './server.js';
import { RpcRouter } from './router.js';
import { RpcError } from './errors.js';
import type { ServerFrame } from './protocol.js';
import type { Doc, HLCTimestamp, IStorageAdapter, QueryOptions } from '../core/types.js';
import {
  installFakeWebRTC,
  uninstallFakeWebRTC,
  waitFor,
  FakeConsumer,
  type FakeSignalingHub,
} from '../test-utils/fake-webrtc.js';

afterEach(() => uninstallFakeWebRTC());

// A tiny adapter exposing just the read surface handlers use here.
function fakeAdapter(rows: Array<Record<string, unknown>>): IStorageAdapter {
  return {
    query: async (_collection: string, _options?: QueryOptions) => rows as unknown as Doc[],
  } as unknown as IStorageAdapter;
}

interface Harness {
  hub: FakeSignalingHub;
  transport: WebRTCTransport;
}

function setup(router: RpcRouter, adapter: IStorageAdapter): Harness {
  const hub = installFakeWebRTC();
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
  return { hub, transport };
}

/** Connect a consumer, send `auth`, and resolve once `auth-ok` arrives. */
async function connectAuthed(hub: FakeSignalingHub, id = 'cons1'): Promise<FakeConsumer> {
  const consumer = new FakeConsumer(id, 'r1', hub);
  await waitFor(() => consumer.serverList.length > 0);
  await consumer.connectTo('normalA', 'rpc');
  await waitFor(() => consumer.connected);
  consumer.send({ type: 'auth', token: 't', protocolVersion: 1 });
  await waitFor(() => received(consumer).some((f) => f.type === 'auth-ok'));
  return consumer;
}

function received(consumer: FakeConsumer): ServerFrame[] {
  return consumer.received as ServerFrame[];
}

async function frame(consumer: FakeConsumer, match: (f: ServerFrame) => boolean): Promise<ServerFrame> {
  let found: ServerFrame | undefined;
  await waitFor(() => {
    found = received(consumer).find(match);
    return !!found;
  });
  return found!;
}

describe('RpcServer (Phase 2)', () => {
  it('auth → auth-ok advertises the method catalog', async () => {
    const router = new RpcRouter().read('todos.listMine', { handler: () => [] });
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = await connectAuthed(hub);

    const ok = await frame(consumer, (f) => f.type === 'auth-ok');
    if (ok.type !== 'auth-ok') throw new Error('expected auth-ok');
    expect(ok.server).toBe('normalA');
    expect(ok.methods).toContainEqual({ name: 'todos.listMine', kind: 'read', version: 1 });
    transport.disconnect();
  });

  it('a read handler returns a result filtered by the authenticated identity', async () => {
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
    const { hub, transport } = setup(router, fakeAdapter(rows));
    const consumer = await connectAuthed(hub);

    consumer.send({ type: 'req', id: '1', method: 'todos.listMine' });
    const res = await frame(consumer, (f) => f.type === 'res' && f.id === '1');
    if (res.type !== 'res') throw new Error('expected res');
    expect(res.body).toEqual([{ id: '1' }]);
    transport.disconnect();
  });

  it('a req before auth is rejected with UNAUTHENTICATED', async () => {
    const router = new RpcRouter().read('x', { handler: () => 1 });
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = new FakeConsumer('cons1', 'r1', hub);
    await waitFor(() => consumer.serverList.length > 0);
    await consumer.connectTo('normalA', 'rpc');
    await waitFor(() => consumer.connected);

    consumer.send({ type: 'req', id: '1', method: 'x' }); // no auth first
    const err = await frame(consumer, (f) => f.type === 'err' && f.id === '1');
    if (err.type !== 'err') throw new Error('expected err');
    expect(err.status).toBe('UNAUTHENTICATED');
    transport.disconnect();
  });

  it('an unknown method returns NOT_FOUND', async () => {
    const router = new RpcRouter().read('known', { handler: () => 1 });
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = await connectAuthed(hub);

    consumer.send({ type: 'req', id: '9', method: 'nope' });
    const err = await frame(consumer, (f) => f.type === 'err' && f.id === '9');
    if (err.type !== 'err') throw new Error('expected err');
    expect(err.status).toBe('NOT_FOUND');
    transport.disconnect();
  });

  it('input validation failure → INVALID_ARGUMENT', async () => {
    const router = new RpcRouter().read<{ n: number }, number>('double', {
      input: {
        parse(v: unknown) {
          const o = v as { n?: unknown };
          if (typeof o?.n !== 'number') throw new Error('n must be a number');
          return { n: o.n };
        },
      },
      handler: (_ctx, { n }) => n * 2,
    });
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = await connectAuthed(hub);

    consumer.send({ type: 'req', id: '3', method: 'double', params: { n: 'oops' } });
    const err = await frame(consumer, (f) => f.type === 'err' && f.id === '3');
    if (err.type !== 'err') throw new Error('expected err');
    expect(err.status).toBe('INVALID_ARGUMENT');
    expect(err.message).toContain('n must be a number');
    transport.disconnect();
  });

  it('a handler throwing RpcError maps to that status', async () => {
    const router = new RpcRouter().read('secret', {
      handler: () => {
        throw new RpcError('PERMISSION_DENIED', 'nope');
      },
    });
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = await connectAuthed(hub);

    consumer.send({ type: 'req', id: '4', method: 'secret' });
    const err = await frame(consumer, (f) => f.type === 'err' && f.id === '4');
    if (err.type !== 'err') throw new Error('expected err');
    expect(err.status).toBe('PERMISSION_DENIED');
    expect(err.retryable).toBe(false);
    transport.disconnect();
  });

  it('a non-RpcError throw maps to INTERNAL', async () => {
    const router = new RpcRouter().read('boom', {
      handler: () => {
        throw new Error('kaboom');
      },
    });
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = await connectAuthed(hub);

    consumer.send({ type: 'req', id: '5', method: 'boom' });
    const err = await frame(consumer, (f) => f.type === 'err' && f.id === '5');
    if (err.type !== 'err') throw new Error('expected err');
    expect(err.status).toBe('INTERNAL');
    transport.disconnect();
  });

  it('a handler exceeding its deadline → DEADLINE_EXCEEDED', async () => {
    const router = new RpcRouter().read('slow', {
      handler: () => new Promise(() => undefined), // never resolves
    });
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = await connectAuthed(hub);

    consumer.send({ type: 'req', id: '6', method: 'slow', deadlineMs: 50 });
    const err = await frame(consumer, (f) => f.type === 'err' && f.id === '6');
    if (err.type !== 'err') throw new Error('expected err');
    expect(err.status).toBe('DEADLINE_EXCEEDED');
    transport.disconnect();
  });

  it('ping → pong echoes the id', async () => {
    const router = new RpcRouter();
    const { hub, transport } = setup(router, fakeAdapter([]));
    const consumer = await connectAuthed(hub);

    consumer.send({ type: 'ping', id: 'p1', ts: 123 });
    const pong = await frame(consumer, (f) => f.type === 'pong');
    if (pong.type !== 'pong') throw new Error('expected pong');
    expect(pong.id).toBe('p1');
    expect(pong.ts).toBe(123);
    transport.disconnect();
  });
});
