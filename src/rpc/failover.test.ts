import { describe, it, expect, afterEach } from 'vitest';
import { ConsumerClient } from '../consumer.js';
import { WebRTCTransport } from '../sync/webrtc-transport.js';
import { RpcServer } from './server.js';
import { RpcRouter } from './router.js';
import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';
import { installFakeWebRTC, uninstallFakeWebRTC, tick } from '../test-utils/fake-webrtc.js';

afterEach(() => uninstallFakeWebRTC());

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A Normal Client whose handlers identify the node, so tests can see which one served. */
function setupNormal(nodeId: string): WebRTCTransport {
  const router = new RpcRouter()
    .read('whoami', { handler: () => nodeId })
    .write('slowWrite', {
      handler: async () => {
        await sleep(150);
        return { server: nodeId };
      },
    });
  const transport = new WebRTCTransport({
    signalingServerUrl: 'ws://fake',
    iceServers: [],
    nodeId,
    room: 'r1',
  });
  const server = new RpcServer({
    router,
    adapter: {} as IStorageAdapter,
    hlc: () => 'hlc-1' as HLCTimestamp,
    nodeId,
    send: (id, f) => transport.sendToConsumer(id, f),
    sendAsync: (id, f) => transport.sendToConsumerAsync(id, f),
  });
  transport.onConsumerConnected = (id) => server.onConnect(id);
  transport.onConsumerDisconnected = (id) => server.onDisconnect(id);
  transport.onConsumerMessage = (id, data) => void server.handleMessage(id, data);
  transport.connect();
  return transport;
}

function makeConsumer(): ConsumerClient {
  return new ConsumerClient({
    signalingServerUrl: 'ws://fake',
    room: 'r1',
    nodeId: 'cons1',
    auth: { getToken: () => 't' },
    deadlineMs: 2000,
  });
}

async function twoNormals(): Promise<{ a: WebRTCTransport; b: WebRTCTransport }> {
  const a = setupNormal('A');
  await tick(); // A registers first → server-list = [A, B]
  const b = setupNormal('B');
  await tick();
  return { a, b };
}

describe('Consumer failover (Phase 7)', () => {
  it('a read transparently recovers on another Normal Client when the first one dies', async () => {
    installFakeWebRTC();
    const { a, b } = await twoNormals();
    const consumer = makeConsumer();

    expect(await consumer.invoke('whoami')).toBe('A'); // round-robin picks A

    a.disconnect(); // kill the serving node
    await tick(60); // let the channel close propagate

    expect(await consumer.invoke('whoami')).toBe('B'); // failed over to B

    consumer.close();
    b.disconnect();
  });

  it('a write is NOT replayed across nodes by default (surfaces the error, not double-run)', async () => {
    installFakeWebRTC();
    const { a, b } = await twoNormals();
    const consumer = makeConsumer();
    await consumer.invoke('whoami'); // connect to A

    const p = consumer.invoke('slowWrite', { x: 1 }); // in-flight write to A
    await tick(20);
    a.disconnect(); // A dies before responding

    await expect(p).rejects.toMatchObject({ status: 'UNAVAILABLE' });

    consumer.close();
    b.disconnect();
  });

  it('a write opted into replay (idempotent:true) fails over to another node', async () => {
    installFakeWebRTC();
    const { a, b } = await twoNormals();
    const consumer = makeConsumer();
    await consumer.invoke('whoami'); // connect to A

    const p = consumer.invoke('slowWrite', { x: 1 }, { idempotent: true });
    await tick(20);
    a.disconnect();

    expect(await p).toEqual({ server: 'B' }); // replayed on B

    consumer.close();
    b.disconnect();
  });
});
