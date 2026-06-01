import { describe, it, expect, afterEach } from 'vitest';
import { ConsumerClient } from '../consumer.js';
import { WebRTCTransport } from '../sync/webrtc-transport.js';
import { RpcServer, type TokenVerifier } from './server.js';
import { RpcRouter } from './router.js';
import { createTokenVerifier } from './auth.js';
import { makeEs256Signer } from '../test-utils/jwt.js';
import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';
import { installFakeWebRTC, uninstallFakeWebRTC } from '../test-utils/fake-webrtc.js';

afterEach(() => uninstallFakeWebRTC());

const NOW = () => Math.floor(Date.now() / 1000);
const emptyAdapter = {} as IStorageAdapter;

function setupNormal(router: RpcRouter, verifyToken: TokenVerifier): WebRTCTransport {
  const transport = new WebRTCTransport({
    signalingServerUrl: 'ws://fake',
    iceServers: [],
    nodeId: 'normalA',
    room: 'r1',
  });
  const server = new RpcServer({
    router,
    adapter: emptyAdapter,
    hlc: () => 'hlc-1' as HLCTimestamp,
    nodeId: 'normalA',
    send: (id, frame) => transport.sendToConsumer(id, frame),
    verifyToken,
  });
  transport.onConsumerConnected = (id) => server.onConnect(id);
  transport.onConsumerDisconnected = (id) => server.onDisconnect(id);
  transport.onConsumerMessage = (id, data) => void server.handleMessage(id, data);
  transport.connect();
  return transport;
}

function makeConsumer(getToken: () => string | Promise<string>): ConsumerClient {
  return new ConsumerClient({
    signalingServerUrl: 'ws://fake',
    room: 'r1',
    nodeId: 'cons1',
    auth: { getToken },
    deadlineMs: 2000,
  });
}

describe('Auth & scopes end-to-end (Phase 4)', () => {
  it('a valid token with the required scope succeeds; a missing scope is PERMISSION_DENIED', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const router = new RpcRouter()
      .read('todos.read', { scopes: ['todos:read'], handler: () => 'ok' })
      .read('todos.write', { scopes: ['todos:write'], handler: () => 'ok' });

    installFakeWebRTC();
    const transport = setupNormal(router, verify);
    const token = await signer.sign({ sub: 'patient-1', scope: 'todos:read', exp: NOW() + 60 });
    const consumer = makeConsumer(() => token);

    await expect(consumer.invoke('todos.read')).resolves.toBe('ok');
    await expect(consumer.invoke('todos.write')).rejects.toMatchObject({
      name: 'RpcError',
      status: 'PERMISSION_DENIED',
    });

    consumer.close();
    transport.disconnect();
  });

  it('the authenticated identity comes from the token, not client-asserted fields', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const router = new RpcRouter().read('whoami', {
      scopes: ['me'],
      handler: (ctx) => ctx.consumer.id,
    });

    installFakeWebRTC();
    const transport = setupNormal(router, verify);
    // nodeId is 'cons1' but the token's sub is the authoritative identity.
    const token = await signer.sign({ sub: 'real-user-7', scope: 'me', exp: NOW() + 60 });
    const consumer = makeConsumer(() => token);

    await expect(consumer.invoke('whoami')).resolves.toBe('real-user-7');

    consumer.close();
    transport.disconnect();
  });

  it('an invalid token fails the connection (UNAUTHENTICATED)', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const router = new RpcRouter().read('x', { handler: () => 1 });

    installFakeWebRTC();
    const transport = setupNormal(router, verify);
    const consumer = makeConsumer(() => 'not-a-real-token');

    await expect(consumer.invoke('x')).rejects.toMatchObject({ status: 'UNAUTHENTICATED' });

    consumer.close();
    transport.disconnect();
  });
});
