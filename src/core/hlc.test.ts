import { describe, it, expect, vi } from 'vitest';
import { HLC, parseHLC } from './hlc.js';

describe('HLC', () => {
  it('tick() produces lexicographically ordered timestamps', () => {
    const hlc = new HLC('node-1');
    const t1 = hlc.tick();
    const t2 = hlc.tick();
    const t3 = hlc.tick();
    expect(t1 < t2).toBe(true);
    expect(t2 < t3).toBe(true);
  });

  it('tick() increments counter within the same millisecond', () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_700_000_000_000);
    const hlc = new HLC('node-1');
    const t1 = hlc.tick();
    const t2 = hlc.tick();
    const p1 = parseHLC(t1);
    const p2 = parseHLC(t2);
    expect(p1.physicalMs).toBe(p2.physicalMs);
    expect(p2.counter).toBe(p1.counter + 1);
    vi.useRealTimers();
  });

  it('tick() resets counter when clock advances', () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_700_000_000_000);
    const hlc = new HLC('node-1');
    hlc.tick();
    hlc.tick();
    vi.setSystemTime(1_700_000_000_001);
    const t = hlc.tick();
    expect(parseHLC(t).counter).toBe(0);
    vi.useRealTimers();
  });

  it('update() advances the clock past a remote timestamp', () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_700_000_000_000);
    const hlc = new HLC('node-1');
    hlc.tick();

    const remote = new HLC('node-2');
    vi.setSystemTime(1_700_000_005_000);
    const remoteTs = remote.tick();

    vi.setSystemTime(1_700_000_000_000);
    hlc.update(remoteTs);
    const after = hlc.tick();

    expect(after > remoteTs).toBe(true);
    vi.useRealTimers();
  });

  it('parseHLC round-trips correctly', () => {
    const hlc = new HLC('my-node-id');
    const ts = hlc.tick();
    const parsed = parseHLC(ts);
    expect(parsed.nodeId).toBe('my-node-id');
    expect(typeof parsed.physicalMs).toBe('number');
    expect(typeof parsed.counter).toBe('number');
  });

  it('timestamps are 16+1+6+1+nodeId length', () => {
    const nodeId = 'abc';
    const hlc = new HLC(nodeId);
    const ts = hlc.tick();
    // format: 0000000000000000-000000-abc
    expect(ts.startsWith('0'.repeat(16 - String(Date.now()).length))).toBeTruthy();
    expect(ts.endsWith(nodeId)).toBe(true);
  });
});
