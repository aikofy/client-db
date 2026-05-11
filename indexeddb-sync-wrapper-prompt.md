# AI Prompt: IndexedDB Sync Wrapper (Web)

## Context

Build a **storage and sync library** for web browsers. This is the **web implementation only**. The library must be designed with a clean abstraction layer so that in the future, a mobile implementation (SQLite via expo-sqlite or op-sqlite) can be swapped in without changing application logic.

---

## Architecture Requirements

### 1. Storage Abstraction Interface

Define a `IStorageAdapter` TypeScript interface first. All storage logic must go through this interface. The IndexedDB implementation is one concrete implementation of it. This makes future SQLite migration a drop-in swap.

The interface must support:
- `put(collection, doc)` — upsert a record
- `get(collection, id)` — fetch by ID
- `query(collection, options)` — filter, sort, limit
- `delete(collection, id)` — soft delete (tombstone, not hard delete)
- `bulkInsert(collection, docs)` — for snapshot restore
- `changes(since: HLCTimestamp)` — return all changes after a given HLC timestamp
- `export()` — full snapshot export as JSON
- `import(snapshot)` — restore from snapshot, then resume sync from snapshot's HLC
- `collectionNames()` — list all collections
- `close()` — cleanup

---

### 2. IndexedDB Implementation

Use the `idb` npm package (by Jake Archibald) as the IndexedDB wrapper — it is free, promise-based, and lightweight. Do NOT use RxDB or any paywalled library.

Requirements:
- Each collection maps to an IndexedDB object store
- Support schema definition per collection: field types, indexes (single and compound)
- Every record must have system fields:
  - `_id: string` — primary key
  - `_rev: string` — HLC-based revision (see section 3)
  - `_deleted: boolean` — tombstone flag
  - `_updatedAt: string` — HLC timestamp string
- Index the `_updatedAt` field on every store for efficient delta sync queries
- Schema versioning: support `upgrade()` migrations when schema version changes

---

### 3. Hybrid Logical Clock (HLC)

Implement HLC from scratch (no external library needed — it's small).

- HLC format: `<physical_time_ms>-<logical_counter>-<node_id>` as a zero-padded string so lexicographic sort = causal sort
- Every write must call `hlc.tick()` to get the next timestamp
- On receiving remote events, call `hlc.update(remoteTimestamp)` to advance local clock
- Node ID = random UUID generated once per client, persisted in IndexedDB

---

### 4. Change Log

Maintain a `_changes` object store in IndexedDB:
- Every write (put/delete) appends an entry: `{ id, collection, _rev, _updatedAt, operation: 'put'|'delete' }`
- `changes(since)` queries this store by `_updatedAt` index
- This is the source of truth for delta sync

---

### 5. Conflict Resolution

Implement a pluggable conflict resolution system:

- **Default strategy: Last-Write-Wins (LWW)** using HLC timestamp — higher HLC wins
- **Field-level merge**: allow schema to declare per-field strategies: `lww | first-write-wins | custom`
- **Custom resolver hook**: `onConflict(local, remote) => resolved` — user can override per collection
- **Conflict log**: store all conflicts (both versions) in a `_conflicts` collection for audit and manual resolution
- When a conflict is detected and resolved, write the resolved doc with a new HLC tick

---

### 6. WebRTC Transport Layer

Implement a `WebRTCTransport` class:

- Accepts config: `{ signalingServerUrl, iceServers, nodeId }`
- Connects to signaling server via WebSocket to discover peers
- Opens RTCPeerConnection + RTCDataChannel per peer
- Handles: offer/answer/ICE candidate exchange via signaling server
- Auto-reconnect on disconnect with exponential backoff
- Emits events: `onPeerConnected`, `onPeerDisconnected`, `onMessage`
- Serializes sync messages as MessagePack or JSON

**Do not hardcode STUN/TURN** — accept `iceServers` as config so the caller provides their own.

---

### 7. Sync Protocol (Gossip-based)

For 10+ peers, do NOT use full mesh. Use a **gossip protocol**:

- On connect, each peer syncs with **K=3 random peers** (fanout)
- Sync handshake:
  1. Peer A sends: `{ type: 'sync-request', since: myLastSyncHLC, collections: [...] }`
  2. Peer B responds: `{ type: 'sync-response', changes: [...] }` — only records newer than `since`
  3. Peer A applies changes, resolves conflicts, sends back any changes Peer B is missing
- Periodically (every 30s) re-gossip to catch missed peers
- Track `lastSyncedHLC` per peer in IndexedDB

---

### 8. Snapshot Bootstrap

For new clients joining the network:

- Any established peer can serve a snapshot on request
- Snapshot request/response over WebRTC data channel: `{ type: 'snapshot-request' }` → `{ type: 'snapshot-response', snapshot, hlc }`
- New client: calls `import(snapshot)`, sets its `since` HLC to the snapshot's HLC, then starts normal delta sync
- Snapshot includes all collections + current HLC watermark
- Chunk large snapshots (>1MB) into multiple messages to avoid data channel limits

---

### 9. Online/Offline Handling

- Listen to `window.addEventListener('online' / 'offline')`
- On coming online: trigger gossip sync immediately
- Queue writes made while offline — they are already in the change log, so on reconnect delta sync picks them up automatically
- Expose `syncStatus`: `'online' | 'offline' | 'syncing'`

---

### 10. Public API

The public-facing API should be clean and simple:

```typescript
const db = await createDB({
  name: 'myapp',
  version: 1,
  collections: {
    todos: {
      indexes: ['status', ['userId', 'createdAt']],
      conflictStrategy: 'lww', // or custom function
    }
  },
  sync: {
    signalingServer: 'wss://your-signal-server.com',
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
  }
});

await db.todos.put({ _id: '1', title: 'Hello', status: 'open' });
await db.todos.get('1');
await db.todos.query({ where: { status: 'open' }, orderBy: '_updatedAt', limit: 20 });
await db.todos.delete('1');

db.todos.onChange((changes) => { /* reactive updates */ });

await db.export();
await db.import(snapshot);
```

---

## Technical Constraints

- **TypeScript** — strict mode, full types exported
- **No framework dependency** — plain TS, usable in React/Vue/Svelte/vanilla
- **Zero paywalled dependencies** — only free/open-source packages
- **Allowed dependencies**: `idb`, `uuid`, `msgpackr` (optional for serialization)
- **Tree-shakeable** — don't bundle what's not used
- **Target**: ES2020, modern browsers only (no IE)

---

## File Structure to Generate

```
src/
  core/
    hlc.ts              # Hybrid Logical Clock
    types.ts            # IStorageAdapter interface + shared types
    conflict.ts         # Conflict resolution strategies
    change-log.ts       # Change log helpers
  storage/
    indexeddb.ts        # IndexedDB implementation of IStorageAdapter
    schema.ts           # Schema definition + validation
  sync/
    webrtc-transport.ts # WebRTC + signaling connection
    gossip.ts           # Gossip sync protocol
    snapshot.ts         # Snapshot export/import/chunking
  db.ts                 # Main createDB() entry point
  index.ts              # Public exports
```

---

## Out of Scope (Do Not Implement Now)

- Mobile / SQLite implementation (future)
- Encryption at rest
- Full-text search
- Complex joins or aggregations
- Authentication

---

## Notes for Future Mobile Migration

When implementing the SQLite (mobile) version later:
- Implement `IStorageAdapter` using `expo-sqlite` or `op-sqlite`
- HLC, conflict resolution, gossip sync, and snapshot logic are **reused as-is**
- Only `src/storage/indexeddb.ts` gets replaced with `src/storage/sqlite.ts`
- The transport layer may change (WebRTC on mobile requires a different setup) but the protocol stays the same
