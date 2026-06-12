export { createDB } from './db.js';
export type {
  TypedDB,
  DBBase,
  CollectionProxy,
  TxCollectionProxy,
  TransactionProxy,
  SyncStatus,
  RpcConfig,
} from './db.js';

// RPC server: define read/write/stream handlers a Normal Client exposes to Consumers.
export * from './rpc/index.js';

export { HLC, parseHLC, formatHLC, isValidHLC } from './core/hlc.js';
export type { HLCState } from './core/hlc.js';

export type {
  HLCTimestamp,
  SystemFields,
  Doc,
  QueryOptions,
  ScanOptions,
  WhereClause,
  ChangeEntry,
  ChangeOperation,
  ConflictResolver,
  ConflictStrategy,
  IndexDef,
  CollectionSchema,
  IceServer,
  SyncConfig,
  DBConfig,
  Snapshot,
  SnapshotChunk,
  SnapshotHeaderChunk,
  SnapshotBatchChunk,
  IStorageAdapter,
  SyncMessage,
  SyncRequestMessage,
  SyncResponseMessage,
  SnapshotRequestMessage,
  SnapshotChunkMessage,
  SnapshotResponseMessage,
  SnapshotStreamStartMessage,
  SnapshotStreamBatchMessage,
  SnapshotStreamEndMessage,
} from './core/types.js';

export {
  snapshotChunksToNdjsonStream,
  ndjsonStreamToSnapshotChunks,
} from './snapshot-stream.js';

export { resolveConflict, logConflict } from './core/conflict.js';
export { IndexedDBAdapter } from './storage/indexeddb.js';
export type { BatchOp } from './storage/indexeddb.js';
export { WebRTCTransport } from './sync/webrtc-transport.js';
export type { WebRTCTransportConfig } from './sync/webrtc-transport.js';
export { GossipSync, DEFAULT_MAX_CLOCK_DRIFT_MS } from './sync/gossip.js';
export {
  exportSnapshot,
  importSnapshot,
  requestSnapshot,
  sendSnapshotTo,
  handleSnapshotStreamStart,
  handleSnapshotStreamBatch,
  handleSnapshotStreamEnd,
} from './sync/snapshot.js';
