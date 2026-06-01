import { describe, it, expect } from 'vitest';
import { RpcServer } from './server.js';
import { RpcRouter } from './router.js';
import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';
import type { ServerFrame } from './protocol.js';
import { waitFor } from '../test-utils/fake-webrtc.js';

// HLC strings are lexicographically ordered, so '1' < '2'.
function setup(hlcRef: { value: string }) {
  const router = new RpcRouter().read('r', { handler: () => 'ok' });
  const sent: ServerFrame[] = [];
  const server = new RpcServer({
    router,
    adapter: {} as IStorageAdapter,
    hlc: () => hlcRef.value as HLCTimestamp,
    nodeId: 'A',
    send: (_id, f) => sent.push(f),
    readAfterTimeoutMs: 300,
  });
  server.onConnect('c');
  return { server, sent };
}

async function frameById(sent: ServerFrame[], id: string): Promise<ServerFrame> {
  let found: ServerFrame | undefined;
  await waitFor(() => {
    found = sent.find((f) => (f as { id?: string }).id === id);
    return !!found;
  });
  return found!;
}

describe('readAfter catch-up (Phase 7)', () => {
  it('replies UNAVAILABLE (retryable) when the replica never catches up', async () => {
    const hlcRef = { value: '1' };
    const { server, sent } = setup(hlcRef);
    await server.handleMessage('c', { type: 'auth', token: 't', protocolVersion: 1 });

    await server.handleMessage('c', { type: 'req', id: '1', method: 'r', readAfter: '2' as HLCTimestamp });
    const f = await frameById(sent, '1');
    if (f.type !== 'err') throw new Error('expected err');
    expect(f.status).toBe('UNAVAILABLE');
    expect(f.retryable).toBe(true);
  });

  it('serves the read once the replica catches up to readAfter', async () => {
    const hlcRef = { value: '1' };
    const { server, sent } = setup(hlcRef);
    await server.handleMessage('c', { type: 'auth', token: 't', protocolVersion: 1 });

    setTimeout(() => {
      hlcRef.value = '2'; // gossip catches the replica up
    }, 60);

    await server.handleMessage('c', { type: 'req', id: '1', method: 'r', readAfter: '2' as HLCTimestamp });
    const f = await frameById(sent, '1');
    if (f.type !== 'res') throw new Error('expected res');
    expect(f.body).toBe('ok');
  });

  it('serves immediately when already caught up', async () => {
    const hlcRef = { value: '5' };
    const { server, sent } = setup(hlcRef);
    await server.handleMessage('c', { type: 'auth', token: 't', protocolVersion: 1 });

    await server.handleMessage('c', { type: 'req', id: '1', method: 'r', readAfter: '2' as HLCTimestamp });
    const f = await frameById(sent, '1');
    expect(f.type).toBe('res');
  });
});
