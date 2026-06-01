import { describe, it, expect, afterEach } from 'vitest';
import { WebRTCTransport } from './webrtc-transport.js';
import type { SyncMessage } from '../core/types.js';
import {
  installFakeWebRTC,
  uninstallFakeWebRTC,
  waitFor,
  tick,
  FakeConsumer,
  type FakeSignalingHub,
} from '../test-utils/fake-webrtc.js';

afterEach(() => uninstallFakeWebRTC());

function makeNormal(nodeId: string, room = 'r1'): WebRTCTransport {
  return new WebRTCTransport({ signalingServerUrl: 'ws://fake', iceServers: [], nodeId, room });
}

describe('WebRTCTransport — consumer isolation (Phase 1)', () => {
  it('a consumer over an rpc channel is never a gossip peer and never broadcast to', async () => {
    const hub: FakeSignalingHub = installFakeWebRTC();
    const peerConnected: string[] = [];
    const consumerConnected: string[] = [];
    const consumerMessages: Array<{ id: string; data: unknown }> = [];

    const t = makeNormal('normalA');
    t.onPeerConnected = (id) => peerConnected.push(id);
    t.onConsumerConnected = (id) => consumerConnected.push(id);
    t.onConsumerMessage = (id, data) => consumerMessages.push({ id, data });
    t.connect();

    const consumer = new FakeConsumer('cons1', 'r1', hub);
    await waitFor(() => consumer.serverList.length > 0);
    expect(consumer.serverList).toContain('normalA');

    await consumer.connectTo('normalA', 'rpc');
    await waitFor(() => consumerConnected.length === 1);

    // Isolation invariants: a consumer must not enter the gossip mesh.
    expect(t.peers()).toEqual([]);
    expect(peerConnected).toEqual([]);
    expect(t.consumers()).toEqual(['cons1']);
    expect(consumerConnected).toEqual(['cons1']);

    // consumer → normal
    consumer.send({ hello: 'world' });
    await waitFor(() => consumerMessages.length === 1);
    expect(consumerMessages[0]).toEqual({ id: 'cons1', data: { hello: 'world' } });

    // normal → consumer
    t.sendToConsumer('cons1', { pong: true });
    await waitFor(() => consumer.received.length === 1);
    expect(consumer.received[0]).toEqual({ pong: true });

    // broadcast() is gossip-only and must NOT reach the consumer.
    t.broadcast({ type: 'peer-hello', nodeId: 'normalA', currentHLC: 'x' } as unknown as SyncMessage);
    await tick();
    expect(consumer.received.length).toBe(1);

    t.disconnect();
  });

  it('refuses a consumer that tries to open a sync (gossip) channel', async () => {
    const hub = installFakeWebRTC();
    const peerConnected: string[] = [];
    const consumerConnected: string[] = [];

    const t = makeNormal('normalA');
    t.onPeerConnected = (id) => peerConnected.push(id);
    t.onConsumerConnected = (id) => consumerConnected.push(id);
    t.connect();

    const consumer = new FakeConsumer('cons1', 'r1', hub);
    await waitFor(() => consumer.serverList.length > 0);

    await consumer.connectTo('normalA', 'sync'); // attempt to grab a gossip channel
    await tick(60);

    expect(t.peers()).toEqual([]);
    expect(t.consumers()).toEqual([]);
    expect(consumerConnected).toEqual([]);
    expect(peerConnected).toEqual([]);
    expect(consumer.connected).toBe(false);

    t.disconnect();
  });

  it('removes a consumer on disconnect()', async () => {
    const hub = installFakeWebRTC();
    const consumerConnected: string[] = [];
    const consumerDisconnected: string[] = [];

    const t = makeNormal('normalA');
    t.onConsumerConnected = (id) => consumerConnected.push(id);
    t.onConsumerDisconnected = (id) => consumerDisconnected.push(id);
    t.connect();

    const consumer = new FakeConsumer('cons1', 'r1', hub);
    await waitFor(() => consumer.serverList.length > 0);
    await consumer.connectTo('normalA', 'rpc');
    await waitFor(() => consumerConnected.length === 1);

    t.disconnect();
    expect(t.consumers()).toEqual([]);
    expect(consumerDisconnected).toEqual(['cons1']);
  });
});

describe('WebRTCTransport — mesh sanity (refactor guard)', () => {
  it('two normal clients connect and exchange a message', async () => {
    installFakeWebRTC();
    const aPeers: string[] = [];
    const bMsgs: Array<{ id: string; msg: SyncMessage }> = [];

    const a = makeNormal('A');
    a.onPeerConnected = (id) => aPeers.push(id);
    a.connect();
    // Let A register first so B receives A in its peer-list (B initiates → no glare).
    await tick();

    const b = makeNormal('B');
    b.onMessage = (id, msg) => bMsgs.push({ id, msg });
    b.connect();

    await waitFor(() => aPeers.includes('B'));
    expect(a.peers()).toContain('B');

    a.send('B', { type: 'peer-hello', nodeId: 'A', currentHLC: 'x' } as unknown as SyncMessage);
    await waitFor(() => bMsgs.length === 1);
    expect(bMsgs[0].id).toBe('A');
    expect(bMsgs[0].msg.type).toBe('peer-hello');

    // A consumer never appears as a gossip peer on either side.
    expect(a.consumers()).toEqual([]);
    expect(b.consumers()).toEqual([]);

    a.disconnect();
    b.disconnect();
  });
});
