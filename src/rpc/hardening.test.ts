import { describe, it, expect } from 'vitest';
import { RpcServer } from './server.js';
import { RpcRouter } from './router.js';
import { TokenBucket } from './middleware.js';
import type { CallRecord } from './middleware.js';
import type { HLCTimestamp, IStorageAdapter } from '../core/types.js';
import type { ServerFrame } from './protocol.js';
import { waitFor } from '../test-utils/fake-webrtc.js';

function setup(opts?: {
  perMin?: number;
  maxPayloadBytes?: number;
  maxInflight?: number;
  onCall?: (r: CallRecord) => void;
  verifyToken?: (token: string, consumerId: string) => null | { id: string; scopes: string[]; claims: Record<string, unknown> };
  hang?: () => Promise<unknown>;
}) {
  const router = new RpcRouter()
    .read('echo', { handler: (_ctx, p) => p })
    .write('save', { handler: () => ({ ok: true }) });
  if (opts?.hang) router.read('hang', { handler: () => opts.hang!() });
  const sent: ServerFrame[] = [];
  const server = new RpcServer({
    router,
    adapter: {} as IStorageAdapter,
    hlc: () => 'hlc-1' as HLCTimestamp,
    nodeId: 'A',
    send: (_id, f) => sent.push(f),
    verifyToken: opts?.verifyToken,
    limits: {
      rateLimit: { perMin: opts?.perMin ?? 600 },
      maxPayloadBytes: opts?.maxPayloadBytes ?? 1024,
      defaultDeadlineMs: 1000,
      maxInflight: opts?.maxInflight ?? 64,
    },
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

  it('caps concurrent in-flight calls with RESOURCE_EXHAUSTED (retryable)', async () => {
    let release!: () => void;
    const gate = new Promise<unknown>((r) => { release = () => r({ done: true }); });
    const { server, sent } = setup({ maxInflight: 1, hang: () => gate });
    await authed(server);
    await server.handleMessage('c', { type: 'req', id: '1', method: 'hang' }); // occupies the slot
    await server.handleMessage('c', { type: 'req', id: '2', method: 'echo', params: 1 }); // over cap
    const f = await frameById(sent, '2');
    if (f.type !== 'err') throw new Error('expected err');
    expect(f.status).toBe('RESOURCE_EXHAUSTED');
    expect(f.retryable).toBe(true);
    release(); // let call 1 finish; the slot frees up
    await frameById(sent, '1');
    await server.handleMessage('c', { type: 'req', id: '3', method: 'echo', params: 1 });
    const ok = await frameById(sent, '3');
    expect(ok.type).toBe('res');
  });

  it('rejects an oversized auth token without verifying it', async () => {
    let verifierCalls = 0;
    const { server, sent } = setup({ verifyToken: () => { verifierCalls += 1; return null; } });
    await server.handleMessage('c', { type: 'auth', token: 'x'.repeat(10_000), protocolVersion: 1 });
    await waitFor(() => sent.some((f) => f.type === 'auth-err'));
    const f = sent.find((x) => x.type === 'auth-err')!;
    if (f.type !== 'auth-err') throw new Error('expected auth-err');
    expect(f.status).toBe('UNAUTHENTICATED');
    expect(verifierCalls).toBe(0); // bounded before any decode/verify work
  });

  it('locks out repeated failed auth attempts on one connection', async () => {
    let verifierCalls = 0;
    const { server, sent } = setup({ verifyToken: () => { verifierCalls += 1; return null; } });
    for (let i = 0; i < 7; i++) {
      await server.handleMessage('c', { type: 'auth', token: 'bad', protocolVersion: 1 });
    }
    const errs = sent.filter((f) => f.type === 'auth-err');
    expect(errs.length).toBe(7);
    expect(verifierCalls).toBe(5); // attempts 6+ never reach the verifier
    expect(errs.slice(5).every((f) => f.type === 'auth-err' && f.status === 'RESOURCE_EXHAUSTED')).toBe(true);
    // A NEW connection (fresh session) still reaches the verifier.
    server.onConnect('c2');
    await server.handleMessage('c2', { type: 'auth', token: 'bad', protocolVersion: 1 });
    expect(verifierCalls).toBe(6);
  });
});
