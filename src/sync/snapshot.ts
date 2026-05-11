import { v4 as uuidv4 } from 'uuid';
import type {
  IStorageAdapter,
  Snapshot,
  SnapshotChunkMessage,
  SnapshotRequestMessage,
  SnapshotResponseMessage,
  SyncMessage,
} from '../core/types.js';
import type { WebRTCTransport } from './webrtc-transport.js';

const CHUNK_SIZE_BYTES = 256 * 1024; // 256 KB

export async function exportSnapshot(adapter: IStorageAdapter): Promise<Snapshot> {
  return adapter.export();
}

export async function importSnapshot(
  adapter: IStorageAdapter,
  snapshot: Snapshot,
): Promise<void> {
  await adapter.import(snapshot);
}

export function serveSnapshot(
  transport: WebRTCTransport,
  adapter: IStorageAdapter,
): void {
  const originalOnMessage = transport.onMessage;
  transport.onMessage = async (peerId, msg) => {
    if (msg.type === 'snapshot-request') {
      await sendSnapshotTo(transport, adapter, peerId, msg.requestId);
    }
    originalOnMessage(peerId, msg);
  };
}

export async function sendSnapshotTo(
  transport: WebRTCTransport,
  adapter: IStorageAdapter,
  peerId: string,
  requestId: string,
): Promise<void> {
  const snapshot = await adapter.export();
  const serialized = JSON.stringify(snapshot);

  if (serialized.length <= CHUNK_SIZE_BYTES) {
    const response: SnapshotResponseMessage = {
      type: 'snapshot-response',
      requestId,
      snapshot,
    };
    transport.send(peerId, response);
    return;
  }

  // Chunk large snapshots
  const chunks: string[] = [];
  for (let i = 0; i < serialized.length; i += CHUNK_SIZE_BYTES) {
    chunks.push(serialized.slice(i, i + CHUNK_SIZE_BYTES));
  }

  for (let i = 0; i < chunks.length; i++) {
    const chunk: SnapshotChunkMessage = {
      type: 'snapshot-chunk',
      requestId,
      chunkIndex: i,
      totalChunks: chunks.length,
      data: chunks[i],
    };
    transport.send(peerId, chunk);
  }
}

export function requestSnapshot(
  transport: WebRTCTransport,
  peerId: string,
): Promise<Snapshot> {
  const requestId = uuidv4();

  return new Promise((resolve, reject) => {
    const chunks = new Map<number, string>();
    let totalChunks: number | null = null;
    const timeoutId = setTimeout(() => reject(new Error('Snapshot request timed out')), 60_000);

    const originalOnMessage = transport.onMessage;
    transport.onMessage = (from, msg: SyncMessage) => {
      if (from !== peerId) {
        originalOnMessage(from, msg);
        return;
      }

      if (msg.type === 'snapshot-response' && msg.requestId === requestId) {
        clearTimeout(timeoutId);
        transport.onMessage = originalOnMessage;
        resolve(msg.snapshot);
        return;
      }

      if (msg.type === 'snapshot-chunk' && msg.requestId === requestId) {
        chunks.set(msg.chunkIndex, msg.data);
        totalChunks = msg.totalChunks;

        if (chunks.size === totalChunks) {
          clearTimeout(timeoutId);
          transport.onMessage = originalOnMessage;
          const assembled = Array.from({ length: totalChunks }, (_, i) => chunks.get(i)!).join('');
          try {
            resolve(JSON.parse(assembled) as Snapshot);
          } catch (e) {
            reject(e);
          }
        }
        return;
      }

      originalOnMessage(from, msg);
    };

    const req: SnapshotRequestMessage = {
      type: 'snapshot-request',
      fromNodeId: transport['config'].nodeId as string,
      requestId,
    };
    transport.send(peerId, req);
  });
}
