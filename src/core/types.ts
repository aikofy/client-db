// ─── HLC ─────────────────────────────────────────────────────────────────────

/** Lexicographically sortable HLC string: `<ms_16>-<counter_6>-<nodeId>` */
export type HLCTimestamp = string & { readonly __hlc: unique symbol };

// ─── Document types ───────────────────────────────────────────────────────────

export interface SystemFields {
  _id: string;
  _rev: HLCTimestamp;
  _deleted: boolean;
  _updatedAt: HLCTimestamp;
}

export type Doc<T extends Record<string, unknown> = Record<string, unknown>> =
  T & SystemFields;

// ─── Query ────────────────────────────────────────────────────────────────────

export type WhereClause<T> = Partial<{
  [K in keyof T]: T[K];
}>;

export interface QueryOptions<T extends Record<string, unknown> = Record<string, unknown>> {
  where?: WhereClause<T & SystemFields>;
  orderBy?: keyof (T & SystemFields);
  orderDir?: 'asc' | 'desc';
  limit?: number;
  offset?: number;
  includeDeleted?: boolean;
}

// ─── Change log ───────────────────────────────────────────────────────────────

export type ChangeOperation = 'put' | 'delete';

export interface ChangeEntry {
  id: string;
  collection: string;
  _rev: HLCTimestamp;
  _updatedAt: HLCTimestamp;
  operation: ChangeOperation;
  /** 'local' = written by this node; 'peer' = received from a remote peer */
  origin: 'local' | 'peer';
}

// ─── Conflict resolution ──────────────────────────────────────────────────────

export type ConflictResolver<T extends Record<string, unknown> = Record<string, unknown>> = (
  local: Doc<T>,
  remote: Doc<T>,
) => Doc<T>;

export type ConflictStrategy<T extends Record<string, unknown> = Record<string, unknown>> =
  | 'lww'
  | 'first-write-wins'
  | ConflictResolver<T>;

// ─── Schema ───────────────────────────────────────────────────────────────────

export type IndexDef = string | [string, string, ...string[]];

export interface CollectionSchema<T extends Record<string, unknown> = Record<string, unknown>> {
  indexes?: IndexDef[];
  conflictStrategy?: ConflictStrategy<T>;
}

// ─── Sync config ──────────────────────────────────────────────────────────────

export interface IceServer {
  urls: string | string[];
  username?: string;
  credential?: string;
}

export interface SyncConfig {
  signalingServer: string;
  iceServers: IceServer[];
  nodeId?: string;
  /**
   * Seed the DB before connecting to any peers.
   * Pass a Promise that resolves to a Snapshot (e.g. downloaded from Drive).
   * - Resolves with Snapshot → imported, then peers connect → delta sync (Case 1)
   * - Resolves with null/undefined or rejects → peers connect → full bootstrap from best peer (Case 2)
   * Peer connections are delayed until the promise settles.
   */
  initialSnapshot?: Snapshot | Promise<Snapshot | null | undefined>;
}

// ─── DB config ────────────────────────────────────────────────────────────────

export interface DBConfig {
  name: string;
  version: number;
  collections: Record<string, CollectionSchema>;
  sync?: SyncConfig;
}

// ─── Snapshot ─────────────────────────────────────────────────────────────────

export interface Snapshot {
  version: number;
  hlc: HLCTimestamp;
  collections: Record<string, Doc[]>;
  meta: Record<string, unknown>;
}

// ─── IStorageAdapter ──────────────────────────────────────────────────────────

export interface IStorageAdapter {
  /** Upsert a record. Stamps _rev, _updatedAt, _deleted=false. */
  put(collection: string, doc: Record<string, unknown>): Promise<Doc>;

  /** Fetch a single record by _id. Returns null if not found. */
  get(collection: string, id: string): Promise<Doc | null>;

  /** Filter, sort, limit records in a collection. Excludes deleted by default. */
  query(collection: string, options?: QueryOptions): Promise<Doc[]>;

  /** Soft-delete: sets _deleted=true with a new HLC revision. */
  delete(collection: string, id: string): Promise<void>;

  /** Insert many records in a single transaction (e.g. snapshot restore). */
  bulkInsert(collection: string, docs: Doc[]): Promise<void>;

  /** Return all change log entries with _updatedAt > since. Optionally capped to `limit` entries. */
  changes(since: HLCTimestamp, limit?: number): Promise<ChangeEntry[]>;

  /** Full snapshot export as JSON-serialisable object. */
  export(): Promise<Snapshot>;

  /** Restore from snapshot, then reset HLC watermark to snapshot's HLC. */
  import(snapshot: Snapshot): Promise<void>;

  /** List all user-defined collection names. */
  collectionNames(): Promise<string[]>;

  /** Clean up connections/handles. */
  close(): Promise<void>;
}

// ─── Sync messages ────────────────────────────────────────────────────────────

export interface SyncRequestMessage {
  type: 'sync-request';
  since: HLCTimestamp;
  collections: string[];
  fromNodeId: string;
  requestId: string;
  cursor?: HLCTimestamp;
  pageSize?: number;
}

export interface SyncResponseMessage {
  type: 'sync-response';
  changes: ChangeEntry[];
  docs: Doc[];
  fromNodeId: string;
  requestId: string;
  hasMore?: boolean;
  nextCursor?: HLCTimestamp;
}

export interface SnapshotRequestMessage {
  type: 'snapshot-request';
  fromNodeId: string;
  requestId: string;
}

export interface SnapshotChunkMessage {
  type: 'snapshot-chunk';
  requestId: string;
  chunkIndex: number;
  totalChunks: number;
  data: string;
}

export interface SnapshotResponseMessage {
  type: 'snapshot-response';
  requestId: string;
  snapshot: Snapshot;
}

export interface SnapshotStreamStartMessage {
  type: 'snapshot-stream-start';
  requestId: string;
  collections: string[];
  hlc: HLCTimestamp;
  version: number;
}

export interface SnapshotStreamBatchMessage {
  type: 'snapshot-stream-batch';
  requestId: string;
  collection: string;
  docs: Doc[];
  batchIndex: number;
  isLastBatch: boolean;
}

export interface SnapshotStreamEndMessage {
  type: 'snapshot-stream-end';
  requestId: string;
  hlc: HLCTimestamp;
}

export interface PeerHelloMessage {
  type: 'peer-hello';
  nodeId: string;
  currentHLC: HLCTimestamp;
}

export type SyncMessage =
  | SyncRequestMessage
  | SyncResponseMessage
  | SnapshotRequestMessage
  | SnapshotChunkMessage
  | SnapshotResponseMessage
  | SnapshotStreamStartMessage
  | SnapshotStreamBatchMessage
  | SnapshotStreamEndMessage
  | PeerHelloMessage;
