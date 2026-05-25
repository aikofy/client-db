export { createDB } from './db.js';
export type { TypedDB, DBBase, CollectionProxy, SyncStatus } from './db.js';

export { HLC, parseHLC, formatHLC } from './core/hlc.js';
export type { HLCState } from './core/hlc.js';

export type {
  HLCTimestamp,
  SystemFields,
  Doc,
  QueryOptions,
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

export { resolveConflict, logConflict } from './core/conflict.js';
export { IndexedDBAdapter } from './storage/indexeddb.js';
export { WebRTCTransport } from './sync/webrtc-transport.js';
export type { WebRTCTransportConfig } from './sync/webrtc-transport.js';
export { GossipSync } from './sync/gossip.js';
export {
  exportSnapshot,
  importSnapshot,
  requestSnapshot,
  sendSnapshotTo,
  handleSnapshotStreamStart,
  handleSnapshotStreamBatch,
  handleSnapshotStreamEnd,
} from './sync/snapshot.js';
