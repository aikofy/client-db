import { describe, it, expect } from 'vitest';
import { IdempotencyCache } from './idempotency.js';

describe('IdempotencyCache', () => {
  it('runs fn once per key and returns the cached result on replay', async () => {
    const cache = new IdempotencyCache();
    let calls = 0;
    const fn = () => {
      calls += 1;
      return 'result';
    };

    const a = await cache.run('k1', fn);
    const b = await cache.run('k1', fn);
    expect(a).toBe('result');
    expect(b).toBe('result');
    expect(calls).toBe(1);
  });

  it('dedupes concurrent in-flight calls', async () => {
    const cache = new IdempotencyCache();
    let calls = 0;
    const fn = () =>
      new Promise<number>((resolve) => {
        calls += 1;
        setTimeout(() => resolve(calls), 10);
      });

    const [a, b] = await Promise.all([cache.run('k', fn), cache.run('k', fn)]);
    expect(calls).toBe(1);
    expect(a).toBe(1);
    expect(b).toBe(1);
  });

  it('keeps different keys independent', async () => {
    const cache = new IdempotencyCache();
    let calls = 0;
    const fn = () => ++calls;
    await cache.run('a', fn);
    await cache.run('b', fn);
    expect(calls).toBe(2);
  });

  it('does not cache failures (allows retry)', async () => {
    const cache = new IdempotencyCache();
    let calls = 0;
    const fn = () => {
      calls += 1;
      if (calls === 1) throw new Error('boom');
      return 'ok';
    };

    await expect(cache.run('k', fn)).rejects.toThrow('boom');
    await expect(cache.run('k', fn)).resolves.toBe('ok'); // retried, not cached failure
    expect(calls).toBe(2);
  });

  it('re-runs after the TTL expires', async () => {
    let t = 1000;
    const cache = new IdempotencyCache({ ttlMs: 100, now: () => t });
    let calls = 0;
    const fn = () => ++calls;

    await cache.run('k', fn);
    t += 50; // within TTL
    await cache.run('k', fn);
    expect(calls).toBe(1);

    t += 200; // past TTL
    await cache.run('k', fn);
    expect(calls).toBe(2);
  });
});
