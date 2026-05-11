import { describe, it, expect } from 'vitest';
import { resolveConflict } from './conflict.js';
import type { Doc } from './types.js';
import type { HLCTimestamp } from './types.js';

function makeDoc(rev: string, extra: Record<string, unknown> = {}): Doc {
  return {
    _id: 'doc-1',
    _rev: rev as HLCTimestamp,
    _deleted: false,
    _updatedAt: rev as HLCTimestamp,
    ...extra,
  };
}

describe('resolveConflict', () => {
  it('lww: higher rev wins', () => {
    const local = makeDoc('0000000000000002-000001-node1');
    const remote = makeDoc('0000000000000003-000000-node2');
    const result = resolveConflict(local, remote, 'lww');
    expect(result).toBe(remote);
  });

  it('lww: local wins when rev is higher', () => {
    const local = makeDoc('0000000000000005-000000-node1');
    const remote = makeDoc('0000000000000003-000000-node2');
    const result = resolveConflict(local, remote, 'lww');
    expect(result).toBe(local);
  });

  it('first-write-wins: lower rev wins', () => {
    const local = makeDoc('0000000000000005-000000-node1');
    const remote = makeDoc('0000000000000003-000000-node2');
    const result = resolveConflict(local, remote, 'first-write-wins');
    expect(result).toBe(remote);
  });

  it('custom resolver is called with local and remote', () => {
    const local = makeDoc('0000000000000002-000001-node1', { score: 10 });
    const remote = makeDoc('0000000000000003-000000-node2', { score: 20 });
    const merged = resolveConflict(local, remote, (l, r) => ({
      ...l,
      score: (l['score'] as number) + (r['score'] as number),
    }));
    expect((merged as Record<string, unknown>)['score']).toBe(30);
  });
});
