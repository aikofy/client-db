import { v4 as uuidv4 } from 'uuid';
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
    let offset = 0;
    let done = false;

    while (!done) {
      const docs = await adapter.query(name, {
        limit: SNAPSHOT_BATCH_SIZE,
        offset,
        includeDeleted: true,
      });
      done = docs.length < SNAPSHOT_BATCH_SIZE;

      await transport.sendAsync(peerId, {
        type: 'snapshot-stream-batch',
        requestId,
        collection: name,
        docs,
        batchIndex,
        isLastBatch: done,
      } as SnapshotStreamBatchMessage);

      offset += docs.length;
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
}

const _activeStreams = new Map<string, StreamReceiveState>();

export function requestSnapshot(
  transport: WebRTCTransport,
  peerId: string,
  adapter: IStorageAdapter,
): Promise<void> {
  const requestId = uuidv4();

  return new Promise<void>((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      _activeStreams.delete(requestId);
      reject(new Error('Snapshot stream timed out'));
    }, 120_000);

    _activeStreams.set(requestId, { adapter, resolve, reject, timeoutId });

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
  await state.adapter.bulkInsert(msg.collection, msg.docs);
}

export async function handleSnapshotStreamEnd(
  _adapter: IStorageAdapter,
  msg: SnapshotStreamEndMessage,
): Promise<void> {
  const state = _activeStreams.get(msg.requestId);
  if (!state) return;

  clearTimeout(state.timeoutId);
  _activeStreams.delete(msg.requestId);

  const concreteAdapter = state.adapter as IndexedDBAdapter;
  concreteAdapter.getHLC().update(msg.hlc);
  await concreteAdapter.persistHLC();

  state.resolve();
}
