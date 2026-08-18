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

/**
 * Options for `scan()` — a streaming, bounded-memory iteration over a collection.
 * Unlike `query()`, scan never materializes the whole collection: it pages by
 * primary key (`_id`) and yields docs in `_id` order. There is no `orderBy`
 * (a global sort would require buffering everything); use `query()` for that.
 */
export interface ScanOptions<T extends Record<string, unknown> = Record<string, unknown>> {
  where?: WhereClause<T & SystemFields>;
  includeDeleted?: boolean;
  /** Docs pulled from IndexedDB per page; caps peak memory. Default 1000. */
  batchSize?: number;
}

// ─── Compaction ─────────────────────────────────────────────────────────────

export interface CompactOptions {
  /**
   * A tombstone is eligible for physical removal once its `_rev` HLC is older than
   * `Date.now() - olderThanMs` (i.e. it was deleted before that wall-clock instant).
   * Defaults to `changeLogTtlDays` (30 d) expressed in ms.
   *
   * SYNC SAFETY: the horizon is also applied to the change log — `compact()` prunes
   * change entries older than `olderThanMs` first, which is what forces any peer
   * that has been offline longer than the horizon to re-bootstrap from a full
   * snapshot (`needsFullSync`) instead of receiving a now-doc-less delete delta.
   * Passing a horizon SHORTER than `changeLogTtlDays` therefore tightens the
   * re-bootstrap boundary for the whole replica — only do so deliberately.
   */
  olderThanMs?: number;
  /** Restrict compaction to these collections. Default: all user collections. */
  collections?: string[];
  /** Tombstones scanned per IndexedDB transaction. Caps peak memory. Default 1000. */
  batchSize?: number;
  /**
   * LOCALLY DESTRUCTIVE + REPLICATED. When provided, every LIVE doc matching the
   * predicate is soft-deleted (an ordinary `delete()`, so it ticks the HLC, enters
   * the change log and gossips to peers as a deletion). The matched docs become
   * tombstones now and are only physically reclaimed by a *later* compaction once
   * they age past the horizon. Use to drop live data the consumer no longer wants.
   */
  predicate?: (doc: Doc) => boolean;
}

export interface CompactResult {
  /** Number of tombstone documents physically removed from collection stores. */
  tombstonesPurged: number;
  /** Number of collections scanned. */
  collections: number;
  /** Approximate bytes of tombstone payload reclaimed (sum of UTF-16 JSON length
   *  of each purged record). A lower-bound proxy for on-disk space freed. */
  bytesReclaimed?: number;
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

/** Static URL, or a callback invoked on every connect/reconnect (token rotation). */
export type SignalingServer = string | (() => string | Promise<string>);

export interface SyncConfig {
  signalingServer: SignalingServer;
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
  /** Days to keep change log entries. Older entries are pruned on startup and every 24 h.
   *  Peers with a watermark older than this TTL receive needsFullSync and re-bootstrap.
   *  Also bounds the `_conflicts` audit log. Default: 30. Set to 0 to disable pruning. */
  changeLogTtlDays?: number;
  /** Reject remote docs whose HLC physical time is more than this far ahead of the local
   *  clock. Bounds clock poisoning: without it, one peer with a far-future clock (malicious
   *  or just wrong) permanently drags every replica's HLC forward, so its writes win LWW
   *  against all later edits. Default: 24 h (tolerates realistic device skew). Set to
   *  `Number.POSITIVE_INFINITY` to disable. */
  maxClockDriftMs?: number;
  /** Whether this Normal Client accepts Consumer ('rpc') connections and serves their
   *  RPC calls. Advertised to the signaling server for load-balancer eligibility.
   *  Default: true. (Consumer RPC handling itself lands in Phase 2.) */
  serveConsumers?: boolean;
  /** Physically reclaim tombstone disk space on the existing 24 h maintenance cycle.
   *  When true, tombstones older than `changeLogTtlDays` are purged right after the
   *  change-log prune (which is exactly what makes the purge sync-safe). Off by
   *  default — opt in once you understand the no-resurrection boundary. Requires
   *  `changeLogTtlDays > 0` (the horizon the purge piggybacks on). Default: false. */
  autoCompact?: boolean;
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

// ─── Streaming snapshot chunks ────────────────────────────────────────────────

/** First chunk of a streamed snapshot: metadata + HLC watermark, no docs. */
export interface SnapshotHeaderChunk {
  kind: 'header';
  version: number;
  hlc: HLCTimestamp;
  meta: Record<string, unknown>;
}

/** A bounded page of documents for one collection (tombstones included). */
export interface SnapshotBatchChunk {
  kind: 'batch';
  collection: string;
  docs: Doc[];
}

/**
 * A streamed snapshot is an ordered sequence: one header, then any number of
 * batches. Producing/consuming it never holds the whole dataset in memory.
 */
export type SnapshotChunk = SnapshotHeaderChunk | SnapshotBatchChunk;

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

  /** Delete change log entries older than `olderThanMs` milliseconds and update the cached oldest-entry watermark. */
  pruneChanges(olderThanMs: number): Promise<void>;

  /**
   * Physically remove tombstone (`_deleted=true`) documents whose `_rev` HLC is
   * older than `olderThanMs` ms, reclaiming IndexedDB space in place. Returns the
   * number of tombstones purged.
   *
   * SYNC-SAFE primitive that `compact()` builds on (parallel to `pruneChanges`):
   * a tombstone is only purged when it is BOTH older than the wall-clock horizon
   * AND strictly older than `getOldestChangesHlc()`. The latter guarantees the
   * deletion's change-log entry is already gone (so no peer can be sent a delete
   * delta with no doc to carry) and that any peer lagging behind the deletion is
   * forced to `needsFullSync` → no resurrection. Local-only: never ticks the HLC,
   * appends a change entry, or gossips. Idempotent and crash-safe (one bounded
   * transaction per page; a partial run leaves every store + index consistent).
   */
  pruneTombstones(olderThanMs: number): Promise<number>;

  /** Return the _updatedAt HLC of the oldest surviving change entry, or null if the log is empty. */
  getOldestChangesHlc(): Promise<HLCTimestamp | null>;

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
  /** True when the requester's `since` watermark predates the oldest surviving change entry.
   *  The requester must fall back to full snapshot bootstrap. */
  needsFullSync?: boolean;
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
