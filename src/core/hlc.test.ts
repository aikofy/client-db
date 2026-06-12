import { describe, it, expect, vi } from 'vitest';
import { HLC, parseHLC, formatHLC, isValidHLC } from './hlc.js';
import type { HLCTimestamp } from './types.js';

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

  it('counter overflow rolls into physical time, preserving lexicographic order', () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_700_000_000_000);
    const hlc = new HLC('node-1');
    // Force the counter to the 6-digit ceiling via a remote at the same physical ms.
    const remote = formatHLC({ physicalMs: 1_700_000_000_000, counter: 999_999, nodeId: 'evil' });
    hlc.tick(); // physicalMs = now, counter = 0
    const before = hlc.now();
    const rolled = hlc.update(remote); // max(0, 999999) + 1 → would be 1_000_000
    const p = parseHLC(rolled);
    expect(p.counter).toBe(0);
    expect(p.physicalMs).toBe(1_700_000_000_001); // rolled into physical time
    expect(rolled.length).toBe(before.length); // still 6-digit counter field
    expect(rolled > remote).toBe(true); // monotonic vs the remote
    expect(rolled > before).toBe(true); // monotonic vs our own past
    vi.useRealTimers();
  });
});

describe('isValidHLC', () => {
  it('accepts real timestamps', () => {
    const hlc = new HLC('node-1');
    expect(isValidHLC(hlc.tick())).toBe(true);
    expect(isValidHLC(formatHLC({ physicalMs: 0, counter: 0, nodeId: 'x' }))).toBe(true);
  });

  it('rejects malformed input that would corrupt the clock', () => {
    expect(isValidHLC(undefined)).toBe(false);
    expect(isValidHLC(null)).toBe(false);
    expect(isValidHLC(42)).toBe(false);
    expect(isValidHLC('')).toBe(false);
    expect(isValidHLC('garbage')).toBe(false);
    expect(isValidHLC('123-0-node')).toBe(false); // wrong field widths
    expect(isValidHLC('000000000000000a-000000-node')).toBe(false); // non-digit ms
    expect(isValidHLC('0000000000000000-000000-')).toBe(false); // empty nodeId
    expect(isValidHLC(`0000000000000000-000000-${'n'.repeat(200)}`)).toBe(false); // nodeId too long
  });

  it('malformed strings rejected by isValidHLC would otherwise parse to NaN', () => {
    expect(Number.isNaN(parseHLC('garbage' as HLCTimestamp).physicalMs)).toBe(true);
  });
});
