import type { IDBPTransaction, IDBPDatabase } from 'idb';
import type { ChangeEntry, HLCTimestamp } from './types.js';

export const CHANGES_STORE = '_changes';

export async function appendChange(
  tx: IDBPTransaction<unknown, string[], 'readwrite'>,
  entry: ChangeEntry,
): Promise<void> {
  const store = tx.objectStore(CHANGES_STORE);
  await store.put(entry);
}

export async function queryChanges(
  db: IDBPDatabase,
  since: HLCTimestamp,
): Promise<ChangeEntry[]> {
  const tx = db.transaction(CHANGES_STORE, 'readonly');
  const store = tx.objectStore(CHANGES_STORE);
  const index = store.index('_updatedAt');
  const range = IDBKeyRange.lowerBound(since, true);
  const results = await index.getAll(range);
  await tx.done;
  return results as ChangeEntry[];
}
