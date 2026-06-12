import { describe, it, expect } from 'vitest';
import { RpcClient } from './client.js';
import { RpcError } from './errors.js';
import type { AuthOkFrame, ClientFrame, ReqFrame } from './protocol.js';

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

function authOk(methods: AuthOkFrame['methods'] = [{ name: 'm', kind: 'read', version: 1 }]): AuthOkFrame {
  return {
    type: 'auth-ok',
    server: 'normalA',
    protocolVersion: 1,
    methods,
    serverHlc: 'h' as never,
    limits: { maxPayloadBytes: 1024, defaultDeadlineMs: 1000, rateLimit: { perMin: 100 }, maxInflight: 64 },
  };
}

function makeClient(getToken: () => string | Promise<string>) {
  const sent: ClientFrame[] = [];
  let n = 0;
  const client = new RpcClient({ send: (f) => sent.push(f), getToken, generateId: () => `id${++n}` });
  return { client, sent };
}

const reqs = (sent: ClientFrame[]): ReqFrame[] => sent.filter((f): f is ReqFrame => f.type === 'req');
const auths = (sent: ClientFrame[]) => sent.filter((f) => f.type === 'auth');

describe('RpcClient', () => {
  it('authenticates then resolves a unary call', async () => {
    const { client, sent } = makeClient(() => 'tok');
    const p = client.invoke('m', { x: 1 });
    await flush();
    expect(auths(sent)[0]).toMatchObject({ type: 'auth', token: 'tok' });

    client.handleMessage(authOk());
    await flush();
    const req = reqs(sent)[0];
    expect(req).toMatchObject({ type: 'req', method: 'm', params: { x: 1 } });

    client.handleMessage({ type: 'res', id: req.id, status: 'OK', body: 42 });
    await expect(p).resolves.toBe(42);
  });

  it('maps an err frame to a rejected RpcError', async () => {
    const { client, sent } = makeClient(() => 'tok');
    const p = client.invoke('m');
    await flush();
    client.handleMessage(authOk());
    await flush();
    const req = reqs(sent)[0];
    client.handleMessage({ type: 'err', id: req.id, status: 'NOT_FOUND', message: 'nope', retryable: false });
    await expect(p).rejects.toMatchObject({ name: 'RpcError', status: 'NOT_FOUND' });
  });

  it('a cancelled call rejects CANCELLED and sends a cancel frame', async () => {
    const { client, sent } = makeClient(() => 'tok');
    const ac = new AbortController();
    const p = client.invoke('m', undefined, { signal: ac.signal });
    await flush();
    client.handleMessage(authOk());
    await flush();
    const req = reqs(sent)[0];
    ac.abort();
    await expect(p).rejects.toMatchObject({ name: 'RpcError', status: 'CANCELLED' });
    expect(sent).toContainEqual({ type: 'cancel', id: req.id });
  });

  it('on UNAUTHENTICATED, re-authenticates with a fresh token and retries once', async () => {
    let tokenCalls = 0;
    const { client, sent } = makeClient(() => `tok${++tokenCalls}`);
    const p = client.invoke('m');

    await flush();
    client.handleMessage(authOk()); // first auth (tok1)
    await flush();
    const req1 = reqs(sent)[0];

    // Server says the session expired.
    client.handleMessage({ type: 'err', id: req1.id, status: 'UNAUTHENTICATED', message: 'expired', retryable: false });
    await flush();

    // Client re-authenticated with a freshly fetched token…
    expect(auths(sent).length).toBe(2);
    expect(auths(sent)[1]).toMatchObject({ token: 'tok2' });
    client.handleMessage(authOk());
    await flush();

    // …and retried the request.
    const allReqs = reqs(sent);
    expect(allReqs.length).toBe(2);
    client.handleMessage({ type: 'res', id: allReqs[1].id, status: 'OK', body: 'recovered' });
    await expect(p).resolves.toBe('recovered');
  });

  it('does not retry more than once (second UNAUTHENTICATED propagates)', async () => {
    const { client, sent } = makeClient(() => 'tok');
    const p = client.invoke('m');
    await flush();
    client.handleMessage(authOk());
    await flush();
    client.handleMessage({ type: 'err', id: reqs(sent)[0].id, status: 'UNAUTHENTICATED', message: 'x', retryable: false });
    await flush();
    client.handleMessage(authOk());
    await flush();
    client.handleMessage({ type: 'err', id: reqs(sent)[1].id, status: 'UNAUTHENTICATED', message: 'again', retryable: false });
    await expect(p).rejects.toMatchObject({ status: 'UNAUTHENTICATED' });
  });

  it('keys writes and attaches readAfter (last write hlc) to subsequent reads', async () => {
    const { client, sent } = makeClient(() => 'tok');
    const methods = [
      { name: 'w', kind: 'write' as const, version: 1 },
      { name: 'r', kind: 'read' as const, version: 1 },
    ];

    const pw = client.invoke('w', { v: 1 });
    await flush();
    client.handleMessage(authOk(methods));
    await flush();
    const wreq = reqs(sent).find((r) => r.method === 'w')!;
    expect(wreq.idempotencyKey).toBeTypeOf('string'); // writes auto-keyed
    expect(wreq.readAfter).toBeUndefined();
    client.handleMessage({ type: 'res', id: wreq.id, status: 'OK', body: 1, hlc: 'H1' as never });
    await pw;

    const pr = client.invoke('r');
    await flush();
    const rreq = reqs(sent).find((r) => r.method === 'r')!;
    expect(rreq.readAfter).toBe('H1'); // read carries the write watermark
    expect(rreq.idempotencyKey).toBeUndefined(); // reads aren't keyed
    client.handleMessage({ type: 'res', id: rreq.id, status: 'OK', body: 2 });
    await pr;
  });

  it('fails fast with NOT_FOUND for a method the server did not advertise', async () => {
    const { client, sent } = makeClient(() => 'tok');
    const p = client.invoke('nope'); // not in the advertised catalog (only 'm')
    await flush();
    client.handleMessage(authOk()); // catalog = [{ name: 'm', ... }]
    await expect(p).rejects.toMatchObject({ status: 'NOT_FOUND' });
    // No req frame was ever sent — the client short-circuited.
    expect(reqs(sent).length).toBe(0);
  });

  it('reset() rejects pending calls', async () => {
    const { client, sent } = makeClient(() => 'tok');
    const p = client.invoke('m');
    await flush();
    client.handleMessage(authOk());
    await flush();
    expect(reqs(sent).length).toBe(1);
    client.reset(new RpcError('UNAVAILABLE', 'channel closed'));
    await expect(p).rejects.toMatchObject({ status: 'UNAVAILABLE' });
  });
});
