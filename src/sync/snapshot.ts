import { v4 as uuidv4 } from 'uuid';
import { isValidHLC, parseHLC } from '../core/hlc.js';
import type {
  IStorageAdapter,
  Snapshot,
  SnapshotRequestMessage,
  SnapshotStreamStartMessage,
  SnapshotStreamBatchMessage,
  SnapshotStreamEndMessage,
} from '../core/types.js';
import type { IndexedDBAdapter } from '../storage/indexeddb.js';
import type { WebRTCTransport } from './webrtc-transport.js';

const SNAPSHOT_BATCH_SIZE = 500;

export async function exportSnapshot(adapter: IStorageAdapter): Promise<Snapshot> {
  return adapter.export();
}

export async function importSnapshot(
  adapter: IStorageAdapter,
  snapshot: Snapshot,
): Promise<void> {
  await adapter.import(snapshot);
}

// ─── Streaming sender ─────────────────────────────────────────────────────────

export async function sendSnapshotTo(
  transport: WebRTCTransport,
  adapter: IStorageAdapter,
  peerId: string,
  requestId: string,
): Promise<void> {
  const concreteAdapter = adapter as IndexedDBAdapter;
  const collections = await adapter.collectionNames();
  const hlc = concreteAdapter.getHLC().now();

  await transport.sendAsync(peerId, {
    type: 'snapshot-stream-start',
    requestId,
    collections,
    hlc,
    version: concreteAdapter.version,
  } as SnapshotStreamStartMessage);

  for (const name of collections) {
    let batchIndex = 0;
    let afterId: string | null = null;
    let done = false;

    while (!done) {
      // Keyset paging by primary key: O(batch) per page, O(n) overall — vs the
      // old growing-offset query which re-scanned all previously-sent records
      // (O(n^2)). Same docs, same _id order, tombstones included.
      const docs = await concreteAdapter._querySnapshotBatch(name, afterId, SNAPSHOT_BATCH_SIZE);
      done = docs.length < SNAPSHOT_BATCH_SIZE;

      await transport.sendAsync(peerId, {
        type: 'snapshot-stream-batch',
        requestId,
        collection: name,
        docs,
        batchIndex,
        isLastBatch: done,
      } as SnapshotStreamBatchMessage);

      if (docs.length > 0) afterId = docs[docs.length - 1]._id;
      batchIndex++;
    }
  }

  await transport.sendAsync(peerId, {
    type: 'snapshot-stream-end',
    requestId,
    hlc: concreteAdapter.getHLC().now(),
  } as SnapshotStreamEndMessage);
}

// ─── Streaming receiver ───────────────────────────────────────────────────────

interface StreamReceiveState {
  adapter: IStorageAdapter;
  resolve: () => void;
  reject: (e: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
  /** Bound on future clock drift for the stream-end HLC (see SyncConfig.maxClockDriftMs). */
  maxClockDriftMs: number;
}

const _activeStreams = new Map<string, StreamReceiveState>();

export function requestSnapshot(
  transport: WebRTCTransport,
  peerId: string,
  adapter: IStorageAdapter,
  maxClockDriftMs: number = Number.POSITIVE_INFINITY,
): Promise<void> {
  const requestId = uuidv4();

  return new Promise<void>((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      _activeStreams.delete(requestId);
      reject(new Error('Snapshot stream timed out'));
    }, 120_000);

    _activeStreams.set(requestId, { adapter, resolve, reject, timeoutId, maxClockDriftMs });

    const req: SnapshotRequestMessage = {
      type: 'snapshot-request',
      fromNodeId: (transport as unknown as { config: { nodeId: string } }).config.nodeId,
      requestId,
    };
    transport.send(peerId, req);
  });
}

export async function handleSnapshotStreamStart(
  _adapter: IStorageAdapter,
  _msg: SnapshotStreamStartMessage,
): Promise<void> {
  // State is tracked in _activeStreams; start message confirms the stream is coming
}

export async function handleSnapshotStreamBatch(
  _adapter: IStorageAdapter,
  msg: SnapshotStreamBatchMessage,
): Promise<void> {
  const state = _activeStreams.get(msg.requestId);
  if (!state) return;
  try {
    await state.adapter.bulkInsert(msg.collection, msg.docs);
  } catch (err) {
    // A failed batch write must reject requestSnapshot (so the caller falls back
    // to delta sync) instead of hanging the full 120s timeout.
    clearTimeout(state.timeoutId);
    _activeStreams.delete(msg.requestId);
    state.reject(err as Error);
  }
}

export async function handleSnapshotStreamEnd(
  _adapter: IStorageAdapter,
  msg: SnapshotStreamEndMessage,
): Promise<void> {
  const state = _activeStreams.get(msg.requestId);
  if (!state) return;

  // Clear the timer + registry entry BEFORE the awaited persist so a throw there
  // can't leave a stale entry or a timer firing against a closed adapter.
  clearTimeout(state.timeoutId);
  _activeStreams.delete(msg.requestId);

  try {
    // A malformed or far-future watermark must not poison the local clock (the
    // imported docs carry their own revs and never touch it). Still resolve —
    // the snapshot data landed.
    const ok =
      isValidHLC(msg.hlc) &&
      parseHLC(msg.hlc).physicalMs <= Date.now() + state.maxClockDriftMs;
    if (ok) {
      const concreteAdapter = state.adapter as IndexedDBAdapter;
      concreteAdapter.getHLC().update(msg.hlc);
      await concreteAdapter.persistHLC();
    }
    state.resolve();
  } catch (err) {
    state.reject(err as Error);
  }
}

/**
 * Abort and reject any in-flight snapshot receives for `adapter`. Called on
 * stop()/close() so a teardown can't leave a 120s timer running or a hanging
 * requestSnapshot promise. Filters by adapter so a concurrently-open DB is safe.
 */
export function cancelActiveStreams(adapter: IStorageAdapter): void {
  for (const [requestId, state] of _activeStreams) {
    if (state.adapter === adapter) {
      clearTimeout(state.timeoutId);
      _activeStreams.delete(requestId);
      state.reject(new Error('Snapshot stream cancelled'));
    }
  }
}
