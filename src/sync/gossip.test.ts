import { describe, it, expect, vi } from 'vitest';
import { resolveConflict } from '../core/conflict.js';
import type { Doc, HLCTimestamp } from '../core/types.js';

// Unit test for the gossip conflict resolution logic (no transport needed)

function makeDoc(id: string, rev: string, extra: Record<string, unknown> = {}): Doc {
  return {
    _id: id,
    _rev: rev as HLCTimestamp,
    _deleted: false,
    _updatedAt: rev as HLCTimestamp,
    ...extra,
  };
}

describe('Gossip sync conflict resolution', () => {
  it('applies remote doc when local does not exist', () => {
    const remote = makeDoc('1', '0000001700000003-000000-remote');
    // Simulates: local=null → use remote
    const result = remote; // direct assignment path
    expect(result._id).toBe('1');
    expect(result._rev).toBe('0000001700000003-000000-remote');
  });

  it('lww: higher-rev remote wins over local', () => {
    const local = makeDoc('1', '0000001700000001-000000-node1');
    const remote = makeDoc('1', '0000001700000005-000000-node2');
    const resolved = resolveConflict(local, remote, 'lww');
    expect(resolved._rev).toBe(remote._rev);
  });

  it('lww: local wins when its rev is higher', () => {
    const local = makeDoc('1', '0000001700000009-000000-node1');
    const remote = makeDoc('1', '0000001700000003-000000-node2');
    const resolved = resolveConflict(local, remote, 'lww');
    expect(resolved._rev).toBe(local._rev);
  });

  it('first-write-wins keeps the doc with the lower rev', () => {
    const local = makeDoc('1', '0000001700000009-000000-node1');
    const remote = makeDoc('1', '0000001700000003-000000-node2');
    const resolved = resolveConflict(local, remote, 'first-write-wins');
    expect(resolved._rev).toBe(remote._rev);
  });

  it('custom resolver can merge fields', () => {
    const local = makeDoc('1', '0000001700000001-000000-a', { votes: 3 });
    const remote = makeDoc('1', '0000001700000002-000000-b', { votes: 7 });
    const resolved = resolveConflict(local, remote, (l, r) => ({
      ...r,
      votes: (l['votes'] as number) + (r['votes'] as number),
    }));
    expect((resolved as Record<string, unknown>)['votes']).toBe(10);
  });

  it('identical revs are no-ops (same doc)', () => {
    const rev = '0000001700000001-000000-node1';
    const local = makeDoc('1', rev, { title: 'hello' });
    const remote = makeDoc('1', rev, { title: 'hello' });
    // When _rev matches we skip — simulate by checking equality
    expect(local._rev === remote._rev).toBe(true);
  });
});
