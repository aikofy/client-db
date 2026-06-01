import { describe, it, expect, afterEach } from 'vitest';
import { ConsumerClient } from '../consumer.js';
import { WebRTCTransport } from '../sync/webrtc-transport.js';
import { RpcServer } from './server.js';
import { RpcRouter } from './router.js';
import { RpcError } from './errors.js';
import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';
import { installFakeWebRTC, uninstallFakeWebRTC, waitFor } from '../test-utils/fake-webrtc.js';

afterEach(() => uninstallFakeWebRTC());

function setupNormal(router: RpcRouter): WebRTCTransport {
  const transport = new WebRTCTransport({
    signalingServerUrl: 'ws://fake',
    iceServers: [],
    nodeId: 'normalA',
    room: 'r1',
  });
  const server = new RpcServer({
    router,
    adapter: {} as IStorageAdapter,
    hlc: () => 'hlc-1' as HLCTimestamp,
    nodeId: 'normalA',
    send: (id, frame) => transport.sendToConsumer(id, frame),
    sendAsync: (id, frame) => transport.sendToConsumerAsync(id, frame),
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

describe('RPC streaming end-to-end (Phase 6)', () => {
  it('yields chunks in order, then completes', async () => {
    const router = new RpcRouter().stream<undefined, { n: number }>('counter', {
      handler: async function* () {
        for (let i = 0; i < 3; i++) yield { n: i };
      },
    });
    installFakeWebRTC();
    const transport = setupNormal(router);
    const consumer = makeConsumer();

    const got: Array<{ n: number }> = [];
    for await (const chunk of consumer.stream<{ n: number }>('counter')) got.push(chunk);
    expect(got).toEqual([{ n: 0 }, { n: 1 }, { n: 2 }]);

    consumer.close();
    transport.disconnect();
  });

  it('propagates a mid-stream handler error after earlier chunks', async () => {
    const router = new RpcRouter().stream('boom', {
      handler: async function* () {
        yield { n: 0 };
        throw new Error('kaboom');
      },
    });
    installFakeWebRTC();
    const transport = setupNormal(router);
    const consumer = makeConsumer();

    const got: unknown[] = [];
    let caught: unknown;
    try {
      for await (const chunk of consumer.stream('boom')) got.push(chunk);
    } catch (e) {
      caught = e;
    }
    expect(got).toEqual([{ n: 0 }]);
    expect(caught).toMatchObject({ name: 'RpcError', status: 'INTERNAL' });

    consumer.close();
    transport.disconnect();
  });

  it('cancels the producer when the consumer breaks early', async () => {
    let cleanedUp = false;
    const router = new RpcRouter().stream('live', {
      handler: async function* (ctx) {
        try {
          for (let i = 0; i < 1000; i++) {
            yield { n: i };
            await new Promise((r) => setTimeout(r, 5));
            if (ctx.signal.aborted) return;
          }
        } finally {
          cleanedUp = true; // runs when the generator is returned (cancel) or completes
        }
      },
    });
    installFakeWebRTC();
    const transport = setupNormal(router);
    const consumer = makeConsumer();

    for await (const chunk of consumer.stream<{ n: number }>('live')) {
      if (chunk.n >= 1) break; // abandon the stream
    }
    // Breaking sends a cancel; the server aborts ctx.signal and the handler's finally runs.
    await waitFor(() => cleanedUp);
    expect(cleanedUp).toBe(true);

    consumer.close();
    transport.disconnect();
  });

  it('cancels via an AbortSignal', async () => {
    const router = new RpcRouter().stream('live', {
      handler: async function* (ctx) {
        for (let i = 0; i < 1000; i++) {
          yield { n: i };
          await new Promise((r) => setTimeout(r, 5));
          if (ctx.signal.aborted) return;
        }
      },
    });
    installFakeWebRTC();
    const transport = setupNormal(router);
    const consumer = makeConsumer();

    const ac = new AbortController();
    const got: Array<{ n: number }> = [];
    let caught: unknown;
    try {
      for await (const chunk of consumer.stream<{ n: number }>('live', undefined, { signal: ac.signal })) {
        got.push(chunk);
        if (got.length === 1) ac.abort();
      }
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeInstanceOf(RpcError);
    expect((caught as RpcError).status).toBe('CANCELLED');

    consumer.close();
    transport.disconnect();
  });
});
