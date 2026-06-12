import type { Doc, ConflictStrategy, IStorageAdapter } from './types.js';

export function resolveConflict<T extends Record<string, unknown>>(
  local: Doc<T>,
  remote: Doc<T>,
  strategy: ConflictStrategy<T>,
): Doc<T> {
  if (strategy === 'lww') {
    return local._rev >= remote._rev ? local : remote;
  }
  if (strategy === 'first-write-wins') {
    return local._rev <= remote._rev ? local : remote;
  }
  return strategy(local, remote);
}

export async function logConflict(
  adapter: IStorageAdapter,
  collection: string,
  local: Doc,
  remote: Doc,
  resolved: Doc,
): Promise<void> {
  const record = {
    _id: `${collection}:${local._id}:${local._rev}:${remote._rev}`,
    collection,
    localDoc: local,
    remoteDoc: remote,
    resolvedDoc: resolved,
    detectedAt: new Date().toISOString(),
  };
  // Prefer the direct conflict-store write: a plain put() would tick the HLC,
  // append a change-log entry and broadcast the record to every peer — conflict
  // audits are local-only. Fall back to put() for custom adapters.
  const direct = adapter as IStorageAdapter & {
    putConflict?: (r: Record<string, unknown> & { _id: string }) => Promise<void>;
  };
  if (typeof direct.putConflict === 'function') {
    await direct.putConflict(record);
  } else {
    await adapter.put('_conflicts', record);
  }
}
