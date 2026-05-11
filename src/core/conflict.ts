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
  await adapter.put('_conflicts', {
    _id: `${collection}:${local._id}:${local._rev}:${remote._rev}`,
    collection,
    localDoc: local,
    remoteDoc: remote,
    resolvedDoc: resolved,
    detectedAt: new Date().toISOString(),
  });
}
