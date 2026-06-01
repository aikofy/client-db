import { describe, it, expect } from 'vitest';
import { RpcServer } from './server.js';
import { RpcRouter } from './router.js';
import { TokenBucket } from './middleware.js';
import type { CallRecord } from './middleware.js';
import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';
import type { ServerFrame } from './protocol.js';
import { waitFor } from '../test-utils/fake-webrtc.js';

function setup(opts?: { perMin?: number; maxPayloadBytes?: number; onCall?: (r: CallRecord) => void }) {
  const router = new RpcRouter()
    .read('echo', { handler: (_ctx, p) => p })
    .write('save', { handler: () => ({ ok: true }) });
  const sent: ServerFrame[] = [];
  const server = new RpcServer({
    router,
    adapter: {} as IStorageAdapter,
    hlc: () => 'hlc-1' as HLCTimestamp,
    nodeId: 'A',
    send: (_id, f) => sent.push(f),
    limits: { rateLimit: { perMin: opts?.perMin ?? 600 }, maxPayloadBytes: opts?.maxPayloadBytes ?? 1024, defaultDeadlineMs: 1000 },
    onCall: opts?.onCall,
  });
  server.onConnect('c');
  return { server, sent };
}

async function authed(server: RpcServer): Promise<void> {
  await server.handleMessage('c', { type: 'auth', token: 't', protocolVersion: 1 });
}

async function frameById(sent: ServerFrame[], id: string): Promise<ServerFrame> {
  let found: ServerFrame | undefined;
  await waitFor(() => {
    found = sent.find((f) => (f as { id?: string }).id === id);
    return !!found;
  });
  return found!;
}

describe('TokenBucket', () => {
  it('allows up to capacity, then refuses until refilled', () => {
    let t = 0;
    const b = new TokenBucket(2, t); // 2/min
    expect(b.tryRemove(t)).toBe(true);
    expect(b.tryRemove(t)).toBe(true);
    expect(b.tryRemove(t)).toBe(false); // empty
    t += 60_000; // a full minute later → refilled
    expect(b.tryRemove(t)).toBe(true);
  });
});

describe('RpcServer hardening (Phase 8)', () => {
  it('trips the rate limit with RESOURCE_EXHAUSTED', async () => {
    const { server, sent } = setup({ perMin: 2 });
    await authed(server);
    await server.handleMessage('c', { type: 'req', id: '1', method: 'echo', params: 1 });
    await server.handleMessage('c', { type: 'req', id: '2', method: 'echo', params: 2 });
    await server.handleMessage('c', { type: 'req', id: '3', method: 'echo', params: 3 }); // over limit
    const f = await frameById(sent, '3');
    if (f.type !== 'err') throw new Error('expected err');
    expect(f.status).toBe('RESOURCE_EXHAUSTED');
    expect(f.retryable).toBe(true);
  });

  it('rejects an oversized payload with INVALID_ARGUMENT', async () => {
    const { server, sent } = setup({ maxPayloadBytes: 50 });
    await authed(server);
    await server.handleMessage('c', { type: 'req', id: '1', method: 'echo', params: { big: 'x'.repeat(200) } });
    const f = await frameById(sent, '1');
    if (f.type !== 'err') throw new Error('expected err');
    expect(f.status).toBe('INVALID_ARGUMENT');
  });

  it('emits an onCall record per call (metrics/audit)', async () => {
    const records: CallRecord[] = [];
    const { server } = setup({ onCall: (r) => records.push(r) });
    await authed(server);
    await server.handleMessage('c', { type: 'req', id: '1', method: 'save', params: { x: 1 } });
    await waitFor(() => records.length > 0);
    const r = records.find((x) => x.method === 'save')!;
    expect(r.kind).toBe('write');
    expect(r.status).toBe('OK');
    expect(typeof r.durationMs).toBe('number');
  });

  it('refuses a newer protocol version at auth (FAILED_PRECONDITION)', async () => {
    const { server, sent } = setup();
    await server.handleMessage('c', { type: 'auth', token: 't', protocolVersion: 999 });
    await waitFor(() => sent.some((f) => f.type === 'auth-err'));
    const f = sent.find((x) => x.type === 'auth-err')!;
    if (f.type !== 'auth-err') throw new Error('expected auth-err');
    expect(f.status).toBe('FAILED_PRECONDITION');
  });
});
