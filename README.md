# @aikofy/client-db

A TypeScript-first, offline-ready, peer-to-peer syncing database for web browsers. Built on IndexedDB with a clean storage abstraction, Hybrid Logical Clock (HLC) ordering, pluggable conflict resolution, and WebRTC gossip sync — no server required for data replication.

[![npm version](https://img.shields.io/npm/v/@aikofy/client-db)](https://www.npmjs.com/package/@aikofy/client-db)
[![license](https://img.shields.io/npm/l/@aikofy/client-db)](./LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-aikofy%2Fclient--db-181717?logo=github)](https://github.com/aikofy/client-db)

---

## Features

- **Offline-first** — writes always succeed locally; sync happens automatically on reconnect
- **Real-time peer sync** — every local write is pushed to all connected peers instantly over WebRTC data channels
- **Room isolation** — peers only discover and sync with peers sharing the same DB name; one DB per user is safe
- **Peer-to-peer** — gossip protocol over WebRTC, no central database server needed
- **Hybrid Logical Clock (HLC)** — causal ordering of writes across nodes without relying on wall-clock agreement
- **Smart bootstrap** — new peers auto-detect their state and pick the right sync strategy (snapshot, delta, or full merge)
- **Pluggable conflict resolution** — Last-Write-Wins (default), First-Write-Wins, or a custom resolver per collection
- **Change origin** — `onChange` callbacks receive `origin: 'local' | 'peer'` so you can distinguish your own writes from incoming sync changes
- **Soft deletes** — tombstones preserve sync integrity; deleted records are never lost
- **Delta sync** — only changes since last sync are exchanged, not full datasets
- **Snapshot bootstrap** — new peers receive a full snapshot then switch to delta sync automatically
- **Fully typed** — strict TypeScript with generics; collection access is type-safe
- **Framework-agnostic** — plain TypeScript, works in React, Vue, Svelte, or vanilla JS
- **Tree-shakeable** — ESM + CJS dual build via tsup

---

## Installation

```bash
npm install @aikofy/client-db
# or
bun add @aikofy/client-db
# or
pnpm add @aikofy/client-db
```

**Peer requirements:** Modern browsers only (Chrome 89+, Firefox 86+, Safari 15+). No IE support.

---

## Quick Start

```typescript
import { createDB } from '@aikofy/client-db';

const db = await createDB({
  name: 'myapp',
  version: 1,
  collections: {
    todos: {
      indexes: ['status', ['userId', 'createdAt']],
      conflictStrategy: 'lww', // last-write-wins (default)
    },
  },
});

// Write
await db.todos.put({ _id: 'todo-1', title: 'Buy milk', status: 'open' });

// Read
const todo = await db.todos.get('todo-1');

// Query
const openTodos = await db.todos.query({
  where: { status: 'open' },
  orderBy: '_updatedAt',
  limit: 20,
});

// Delete (soft — tombstone, syncs to peers)
await db.todos.delete('todo-1');

// React to local writes and incoming sync changes
const unsubscribe = db.todos.onChange((changes) => {
  for (const change of changes) {
    console.log(change.origin); // 'local' | 'peer'
    console.log(change.operation); // 'put' | 'delete'
  }
});

// Export / import snapshots
const snapshot = await db.export();
await db.import(snapshot);

// Clean up
await db.close();
```

---

## Sync (WebRTC P2P)

Pass a `sync` config to enable peer-to-peer sync via WebRTC. You need a signaling server (WebSocket) to help peers discover each other — only handshake metadata is exchanged over it, never your data.

The companion package [`@aikofy/client-db-sync`](https://www.npmjs.com/package/@aikofy/client-db-sync) is the ready-made signaling server for this library.

### Room isolation

Peers are automatically grouped by **DB name**. Only clients with the same `name` in their `createDB` config can discover and sync with each other. This makes it safe to use a userID as the DB name — each user's data stays completely isolated.

### Development (no auth)

Start the signaling server with `AUTH_DISABLED=true` and connect without a token:

```typescript
const db = await createDB({
  name: 'myapp',          // peers with name 'myapp' form one group
  version: 1,
  collections: {
    notes: { indexes: ['authorId'] },
  },
  sync: {
    signalingServer: 'ws://localhost:8080/signal',
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
  },
});
```

### Production (JWT auth)

When auth is enabled on the signaling server, your backend issues a token with `subject` set to the DB name (typically the userID). The library appends the room and token automatically:

```typescript
// 1. Your backend issues a token scoped to this user's DB name
const { token } = await fetch('https://signal.example.com/token', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'x-admin-secret': process.env.SIGNAL_ADMIN_SECRET,
  },
  body: JSON.stringify({ ttl: '24h', subject: currentUser.id }),
}).then(r => r.json());

// 2. Use the userID as the DB name — the library ties the room to it automatically
const db = await createDB({
  name: currentUser.id,   // DB name = room = token subject
  version: 1,
  collections: {
    notes: { indexes: ['authorId'] },
  },
  sync: {
    signalingServer: `wss://signal.example.com/signal?token=${token}`,
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      // Add TURN servers here for reliable NAT traversal in production
    ],
  },
});
```

The signaling server validates that the token's `subject` matches the DB name. A token for `user-123` can **only** join room `user-123` — knowing a userID is not enough to access another user's sync group.

```typescript
console.log(db.syncStatus); // 'online' | 'offline' | 'syncing'
```

### What happens after connection

Once connected, the library:

1. Exchanges `peer-hello` messages with each connected peer (advertises current HLC watermark)
2. Picks the right bootstrap strategy based on local state (see below)
3. Pushes every local write to all connected peers **immediately** via RTCDataChannel (~20–100ms)
4. Re-gossips every 30 seconds as a catch-up heartbeat
5. On `window.online`, syncs with **all** connected peers to collect any offline writes

> **Note:** The signaling server is only used for WebRTC handshake (offer/answer/ICE candidates). After connection, all data flows directly peer-to-peer.

---

## Consumer Clients (RPC) — experimental

> **Status:** in active development. Today this supports **authenticated, scope-authorized reads
> and writes** from a Consumer against a Normal Client — JWS tokens are verified locally with
> WebCrypto (ES256/RS256); writes are idempotent (auto-keyed, deduped) and replicate to other
> Normal Clients via the normal gossip pipeline; **server-streaming** is supported (async-generator
> handlers, backpressure-aware, cancellable); a dropped Normal Client triggers **transparent
> failover** to another candidate with **read-your-writes** preserved; and it's hardened with
> **per-consumer rate limiting**, **payload caps**, an **observability/audit hook**, and
> **capability/version negotiation**. The remaining work is docs, examples, and release. Requires
> a **role-aware signaling server** — see
> [`docs/signaling-protocol.md`](./docs/signaling-protocol.md). Full design:
> [`docs/consumer-client-plan.md`](./docs/consumer-client-plan.md),
> [`docs/rpc-protocol.md`](./docs/rpc-protocol.md).

Beyond peer-to-peer replication, the library supports a second client role so you can expose a
controlled API surface instead of replicating the whole database:

- **Normal Client** — the standard `createDB` client (full replica + gossip). It can *additionally*
  act as an **RPC server**, exposing named read/write functions ("handlers") over WebRTC.
- **Consumer Client** — a thin client (`@aikofy/client-db/consumer`, ~10 KB) that holds **no data**
  and never gossips. It authenticates and *calls* a Normal Client's handlers. Think "frontend
  calling a backend," but the wire is a WebRTC data channel instead of HTTP.

A Consumer can only ever receive what a handler returns — it can never pull the database.

### Define handlers on a Normal Client

```ts
import { createDB, RpcRouter, RpcError, createTokenVerifier } from '@aikofy/client-db';

const router = new RpcRouter()
  .read('todos.listMine', {
    scopes: ['todos:read'], // caller's token must carry these
    handler: async (ctx) => {
      // ctx.db is the FULL local replica — the handler decides what leaves the device.
      const all = await ctx.db.query('todos');
      return all.filter((t) => t.ownerId === ctx.consumer.id); // ctx.consumer = authenticated caller
    },
  })
  .read('todos.get', {
    scopes: ['todos:read'],
    input: { parse: (v) => v as { id: string } }, // any zod-compatible `{ parse }`
    handler: async (ctx, { id }) => {
      const todo = await ctx.db.get('todos', id);
      if (!todo) throw new RpcError('NOT_FOUND', 'no such todo');
      return todo;
    },
  })
  .write('todos.create', {
    scopes: ['todos:write'],
    handler: (ctx, params: { title: string }) =>
      // ctx.idempotent dedupes retries (the Consumer auto-attaches a key for write methods).
      ctx.idempotent(ctx.request.idempotencyKey, async () => {
        const doc = await ctx.db.put('todos', { title: params.title, ownerId: ctx.consumer.id });
        return { id: doc._id }; // the write replicates to other Normal Clients via gossip
      }),
  })
  .stream('todos.export', {
    scopes: ['todos:read'],
    // An async generator. Each `yield` is one stream frame; backpressure is handled for you.
    handler: async function* (ctx) {
      const all = await ctx.db.query('todos');
      for (let i = 0; i < all.length; i += 500) {
        yield all.slice(i, i + 500).map((t) => ({ id: t._id }));
      }
    },
  });

const db = await createDB({
  name: currentUser.id,
  version: 1,
  collections: { todos: { indexes: ['ownerId'] } },
  sync: { signalingServer, iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] },
  rpc: {
    router,
    // Verify Consumer tokens locally (no per-request callout). Tokens are minted by your IdP;
    // we only verify. `sub` becomes ctx.consumer.id; `scope`/`scopes` become ctx.consumer.scopes.
    verifyToken: createTokenVerifier({
      jwks: idpPublicJwks,           // your IdP's public keys
      issuer: 'https://idp.example.com',
      audience: 'client-db',
    }),
  },
});
```

The **handler body is the access-control boundary**: it reads the whole replica via `ctx.db` but
returns only what the authenticated `ctx.consumer` may see. Declared `scopes` are enforced before
the handler runs (missing scope → `PERMISSION_DENIED`). Omit `verifyToken` only in dev — without
it every connection is trusted with no scopes.

### Call from a Consumer Client

```ts
import { ConsumerClient, RpcError } from '@aikofy/client-db/consumer';

const consumer = new ConsumerClient({
  signalingServerUrl: 'wss://signal.example.com/signal',
  room: targetUserId,                          // the Normal Clients' DB name
  auth: { getToken: () => fetchAccessToken() }, // your IdP / backend
});

const mine = await consumer.invoke('todos.listMine'); // connects + authenticates on first call

try {
  const one = await consumer.invoke('todos.get', { id: 'abc' });
} catch (e) {
  if (e instanceof RpcError && e.status === 'NOT_FOUND') { /* handle */ }
}

// Writes work the same way; the SDK auto-attaches a stable idempotency key so a retry is
// deduped (applied once) rather than duplicated.
const { id } = await consumer.invoke('todos.create', { title: 'Buy milk' });

// Server-streaming: iterate chunks as they arrive. Breaking the loop (or an AbortSignal)
// cancels the stream and stops the producer on the Normal Client.
for await (const batch of consumer.stream('todos.export')) {
  render(batch);
}

consumer.close();
```

`invoke()` resolves with the handler's return value or rejects with an `RpcError` carrying a
gRPC-style `status` (`UNAUTHENTICATED`, `PERMISSION_DENIED`, `INVALID_ARGUMENT`, `NOT_FOUND`,
`UNAVAILABLE`, …). The Consumer connects to one Normal Client chosen round-robin from the
signaling server's healthy list.

### Failover & read-your-writes

If the serving Normal Client drops (or returns a retryable error), the Consumer transparently
reconnects to the next candidate, re-authenticates, and replays the call:

- **Reads** are replayed automatically. Each read carries the last write's HLC watermark as
  `readAfter`, so a failed-over replica **waits until it has caught up** (read-your-writes) before
  answering — or returns a retryable `UNAVAILABLE` so the client tries elsewhere.
- **Writes are NOT replayed across nodes by default** — replaying a write on a different node can
  duplicate it until the idempotency dedupe store is replicated (a follow-up). A dropped write
  surfaces `UNAVAILABLE`; pass `{ idempotent: true }` to opt a write into failover-replay.

```ts
await consumer.invoke('todos.listMine');                       // auto-replayed on failover
await consumer.invoke('todos.create', { title: 'x' });         // not replayed → surfaces error
await consumer.invoke('todos.create', { title: 'x' }, { idempotent: true }); // opt into replay
```

### Hardening & observability

The RPC server applies per-consumer **rate limiting** (token bucket → `RESOURCE_EXHAUSTED`) and
**payload caps** (`INVALID_ARGUMENT`), advertises its method catalog so the Consumer SDK
**fails fast** (`NOT_FOUND`, no round-trip) on an unsupported method, and rejects a **newer
protocol version** at auth. An `onCall` hook fires once per call for **metrics and audit**:

```ts
const db = await createDB({
  name, version, collections, sync,
  rpc: {
    router,
    verifyToken,
    onCall: (rec) => {
      metrics.timing(`rpc.${rec.method}`, rec.durationMs, { status: rec.status });
      if (rec.kind === 'write') {
        // Audit trail keyed by the authenticated identity. Persist to a (replicated) collection
        // for a durable, gossip-shared log.
        void db.audit.put({ who: rec.identityId, method: rec.method, status: rec.status });
      }
    },
  },
});
```

Limits are configurable via `rpc` (`idempotencyTtlMs`, `readAfterTimeoutMs`) and the server's
`limits` (`maxPayloadBytes`, `defaultDeadlineMs`, `rateLimit.perMin`).

---

## Bootstrap Strategies

When a client connects to peers for the first time in a session, it automatically picks the right strategy:

### Case 1 — Pre-loaded snapshot (`initialSnapshot`)

You passed `initialSnapshot` to `sync`. The library awaits the promise (e.g. a Drive download), imports the snapshot into an empty DB, **then** connects to peers. After connecting it syncs bidirectionally with **all** peers from the snapshot's HLC watermark, catching any writes that happened since the snapshot was taken.

If the promise rejects or resolves to `null`, the library skips the import and falls through to Case 2.

### Case 2 — Completely new client (empty DB)

The library waits briefly (300ms) to collect `peer-hello` responses from all initially-connected peers, then picks the **most up-to-date peer** (highest HLC) and requests their full snapshot. After applying it, it delta-syncs with all remaining peers to catch anything that peer didn't have.

### Case 3 — Returning client (offline data)

The client has existing local data from a previous session. On reconnect, it syncs bidirectionally with **all** connected peers — not just a random subset — so every peer's offline writes are collected without loss.

---

## Cloud Snapshot Seeding

Use `initialSnapshot` to seed a new browser from a cloud-stored snapshot (Google Drive, S3, etc.) before connecting to any peers. This avoids a slow full-snapshot bootstrap from a peer.

```typescript
// Download snapshot from your cloud storage
async function loadSnapshotFromDrive(): Promise<Snapshot | null> {
  try {
    const res = await fetch('https://storage.example.com/snapshots/user-123.json');
    if (!res.ok) return null;
    return res.json() as Promise<Snapshot>;
  } catch {
    return null;
  }
}

const db = await createDB({
  name: currentUser.id,
  version: 1,
  collections: { todos: { indexes: ['status'] } },
  sync: {
    signalingServer: `wss://signal.example.com/signal?token=${token}`,
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
    // Peers are not contacted until this promise settles
    initialSnapshot: loadSnapshotFromDrive(),
  },
});
```

**What happens:**

| Scenario | Result |
|----------|--------|
| Drive download succeeds | Snapshot imported → peers connect → delta sync from snapshot watermark (fast) |
| Drive download fails / returns `null` | No import → peers connect → full snapshot fetched from best peer (Case 2) |
| DB already has local data | `initialSnapshot` is ignored (won't overwrite existing data) |

To keep the cloud snapshot fresh, call `db.export()` and upload the result periodically or on `window.beforeunload`.

```typescript
// Save snapshot to your cloud storage on page unload
window.addEventListener('beforeunload', async () => {
  const snapshot = await db.export();
  navigator.sendBeacon(
    'https://storage.example.com/snapshots/user-123.json',
    JSON.stringify(snapshot),
  );
});
```

---

## API Reference

### `createDB(config)`

```typescript
const db = await createDB(config: DBConfig);
```

Returns a `TypedDB<C>` — an object where each key of `collections` is a `CollectionProxy`, plus `export`, `import`, `syncStatus`, and `close`.

#### `DBConfig`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | IndexedDB database name. Also used as the sync room — peers with the same name sync together. |
| `version` | `number` | Schema version — increment to trigger migrations |
| `collections` | `Record<string, CollectionSchema>` | Collection definitions |
| `sync` | `SyncConfig` (optional) | Enable WebRTC sync |

#### `CollectionSchema`

| Field | Type | Description |
|-------|------|-------------|
| `indexes` | `(string \| string[])[]` (optional) | Single-field or compound indexes |
| `conflictStrategy` | `'lww' \| 'first-write-wins' \| resolver` (optional) | Defaults to `'lww'` |

#### `SyncConfig`

| Field | Type | Description |
|-------|------|-------------|
| `signalingServer` | `string` | WebSocket URL. Append `?token=<jwt>` when auth is enabled. The `room` param is appended automatically from `DBConfig.name`. |
| `iceServers` | `IceServer[]` | STUN/TURN servers for WebRTC |
| `nodeId` | `string` (optional) | Override auto-generated node UUID |
| `initialSnapshot` | `Snapshot \| Promise<Snapshot \| null \| undefined>` (optional) | Seed the DB before connecting to peers. Peer connections are held until this settles. See [Cloud snapshot seeding](#cloud-snapshot-seeding). |

---

### `CollectionProxy<T>`

All collection operations are async and available on `db.<collectionName>`.

#### `put(doc)`

```typescript
const saved = await db.todos.put({ _id: 'abc', title: 'Hello', status: 'open' });
// _id is auto-generated (UUID) if omitted
```

Upserts a record. Stamps `_rev` (HLC), `_updatedAt` (HLC), `_deleted: false`. Returns the full `Doc<T>`. The write is immediately pushed to all connected peers.

#### `get(id)`

```typescript
const todo = await db.todos.get('abc'); // Doc<T> | null
```

#### `query(options?)`

```typescript
const results = await db.todos.query({
  where: { status: 'open' },   // field equality filters
  orderBy: '_updatedAt',        // any field
  orderDir: 'desc',             // 'asc' (default) | 'desc'
  limit: 10,
  offset: 0,
  includeDeleted: false,        // default false — excludes tombstones
});
```

#### `delete(id)`

Soft delete — sets `_deleted: true` with a new HLC revision. The record remains in IndexedDB and is synced as a deletion event to peers immediately.

#### `onChange(callback)`

```typescript
const unsubscribe = db.todos.onChange((changes: ChangeEntry[]) => {
  // fires on local writes AND incoming sync changes from peers
  for (const change of changes) {
    console.log(change.origin);    // 'local' | 'peer'
    console.log(change.operation); // 'put' | 'delete'
    console.log(change.id);        // document _id
  }
});

unsubscribe(); // stop listening
```

---

### System Fields

Every stored document has these read-only system fields:

| Field | Type | Description |
|-------|------|-------------|
| `_id` | `string` | Primary key |
| `_rev` | `HLCTimestamp` | HLC-based revision string |
| `_deleted` | `boolean` | Tombstone flag |
| `_updatedAt` | `HLCTimestamp` | Last write timestamp (HLC) |

`HLCTimestamp` strings are lexicographically sortable — alphabetical order equals causal order.

---

### `ChangeEntry`

Passed to `onChange` callbacks:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Document `_id` |
| `collection` | `string` | Collection name |
| `_rev` | `HLCTimestamp` | Revision at time of change |
| `_updatedAt` | `HLCTimestamp` | Timestamp of change |
| `operation` | `'put' \| 'delete'` | Type of write |
| `origin` | `'local' \| 'peer'` | Who made the change |

---

### Conflict Resolution

#### Built-in strategies

```typescript
// Last-Write-Wins: highest HLC timestamp wins (default)
conflictStrategy: 'lww'

// First-Write-Wins: lowest HLC timestamp wins
conflictStrategy: 'first-write-wins'
```

#### Custom resolver

```typescript
conflictStrategy: (local, remote) => {
  // Merge fields, pick a winner, or return any Doc shape
  return { ...remote, score: local.score + remote.score };
}
```

All conflicts (both versions + resolved) are logged to an internal `_conflicts` collection for audit.

---

### Snapshot API

```typescript
// Export full snapshot (all collections + HLC watermark)
const snapshot = await db.export();

// Import snapshot — use before connecting sync to seed a new client
await db.import(snapshot);
```

Large snapshots (>256 KB) are automatically chunked over the WebRTC data channel.

> **Heads-up:** `db.export()` / `db.import()` materialise the **whole** dataset in memory. For databases beyond a few hundred MB, use the streaming API below — it never holds more than one batch in memory.

---

### Streaming API (large databases)

`export()`, `import()`, and an unbounded `query()` all load their full result set into the JS heap, which will exhaust memory on multi-GB stores. The streaming variants page the data and keep peak memory bounded to a single batch, so they scale to multi-GB databases.

```typescript
// Bounded-memory backup → NDJSON byte stream → upload (no full copy in RAM)
import { snapshotChunksToNdjsonStream, ndjsonStreamToSnapshotChunks } from '@aikofy/client-db';

const body = snapshotChunksToNdjsonStream(db.exportStream(/* batchSize = 1000 */));
await fetch(uploadUrl, { method: 'POST', body, duplex: 'half' });

// Bounded-memory restore from a download stream
const res = await fetch(downloadUrl);
await db.importStream(ndjsonStreamToSnapshotChunks(res.body!));
```

```typescript
// Iterate a large collection without materialising it (one page at a time)
for await (const todo of db.todos.scan({ where: { status: 'open' }, batchSize: 1000 })) {
  process(todo); // pages by _id, yields in _id order
}
```

| Method | Returns | Notes |
|--------|---------|-------|
| `db.exportStream(batchSize?)` | `AsyncGenerator<SnapshotChunk>` | header + keyset-paged batches; tombstones included |
| `db.importStream(chunks)` | `Promise<void>` | consumes chunks; HLC watermark applied **after** all data lands |
| `db.<collection>.scan(opts?)` | `AsyncGenerator<Doc>` | `where` + `includeDeleted`; `_id` order; no `orderBy` (use `query()` for sorted) |
| `snapshotChunksToNdjsonStream(chunks)` | `ReadableStream<Uint8Array>` | encode chunks as NDJSON, backpressure-aware |
| `ndjsonStreamToSnapshotChunks(stream)` | `AsyncGenerator<SnapshotChunk>` | decode NDJSON bytes back to chunks |

Each stage holds only one batch (`batchSize` docs) at a time, so memory stays flat regardless of database size.

---

### Advanced: Direct HLC Access

```typescript
import { HLC, parseHLC } from '@aikofy/client-db';

const hlc = new HLC('my-node-id');
const ts = hlc.tick();          // generate next timestamp
hlc.update(remoteTimestamp);    // advance clock from a remote event
const { physicalMs, counter, nodeId } = parseHLC(ts);
```

---

### Advanced: Custom Storage Adapter

The `IStorageAdapter` interface is the abstraction layer. Implement it to swap in any backend (e.g., SQLite for React Native):

```typescript
import type { IStorageAdapter } from '@aikofy/client-db';

class MySQLiteAdapter implements IStorageAdapter {
  async put(collection, doc) { /* ... */ }
  async get(collection, id) { /* ... */ }
  async query(collection, options) { /* ... */ }
  async delete(collection, id) { /* ... */ }
  async bulkInsert(collection, docs) { /* ... */ }
  async changes(since) { /* ... */ }
  async export() { /* ... */ }
  async import(snapshot) { /* ... */ }
  async collectionNames() { /* ... */ }
  async close() { /* ... */ }
}
```

All sync, HLC, and conflict resolution logic is adapter-agnostic and reusable as-is.

---

## Caveats & Patterns

### `createDB` is idempotent per `name`

Concurrent or repeated `createDB` calls with the same `name` return the **same** in-flight promise. You can safely call it from a React `useEffect` (including under StrictMode, which double-fires effects in dev) without producing two transports.

The registry is cleared on `db.close()`, so close-then-reopen produces a fresh instance.

> **Why this matters:** Without dedup, two `createDB` calls would each spin up a `WebRTCTransport` sharing the same persisted nodeId. The signaling server's "one connection per nodeId per room" rule then alternately kicks each socket with close code `1000 'replaced by new connection'` → reconnect → kick → loop.

### Running multiple tabs of the same browser

The HLC `nodeId` (used for causal ordering) is persisted in IndexedDB and shared across same-origin tabs. The **signaling** `nodeId` should be unique per tab — otherwise the signaling server kicks the older tab when the newer one connects (same room, same nodeId).

The recommended pattern is to override `sync.nodeId` with a per-tab UUID stored in `sessionStorage`:

```typescript
function getSignalNodeId(): string {
  const KEY = 'myapp-signal-node-id';
  let id = sessionStorage.getItem(KEY);
  if (!id) {
    id = crypto.randomUUID();
    sessionStorage.setItem(KEY, id);
  }
  return id;
}

const db = await createDB({
  name: currentUser.id,
  version: 1,
  collections,
  sync: {
    signalingServer: `wss://signal.example.com/signal?token=${token}`,
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
    nodeId: getSignalNodeId(),   // per-tab — distinct from the IDB-persisted HLC nodeId
  },
});
```

`sessionStorage` is per-tab (unlike `localStorage`), so each tab gets a unique UUID. The HLC nodeId stays in IndexedDB unchanged — only signaling identity is per-tab.

### Peer changes and derived caches

If your app caches values derived from synced collections (e.g. account balances aggregated from transactions), **invalidate those caches inside your `onChange` peer handler** — local-write code paths typically invalidate caches inline, but peer writes land via gossip and bypass that code.

```typescript
db.transactions.onChange((changes) => {
  if (!changes.some(c => c.origin === 'peer')) return;
  balanceCache.invalidateAll();   // peer txns shifted balances
  refetchAccounts();              // now returns fresh computedBalance
});
```

---

## React Example

```tsx
import { useEffect, useState } from 'react';
import { createDB } from '@aikofy/client-db';
import type { TypedDB } from '@aikofy/client-db';

const collections = {
  todos: { indexes: ['status'], conflictStrategy: 'lww' as const },
};

// Fetch a short-lived token from your own backend (which calls @aikofy/client-db-sync's POST /token).
// The token's subject must equal the DB name (userId).
// In dev, skip this and use AUTH_DISABLED=true on the signaling server.
async function fetchSignalToken(userId: string): Promise<string> {
  const res = await fetch('/api/signal-token', { method: 'POST' });
  const { token } = await res.json() as { token: string };
  return token;
}

let dbInstance: TypedDB<typeof collections> | null = null;

async function getDB(userId: string) {
  if (!dbInstance) {
    const isDev = import.meta.env.DEV; // Vite / any bundler dev flag
    const signalingServer = isDev
      ? 'ws://localhost:8080/signal'
      : `wss://signal.example.com/signal?token=${await fetchSignalToken(userId)}`;

    dbInstance = await createDB({
      name: userId,   // DB name = room — only this user's peers sync together
      version: 1,
      collections,
      sync: {
        signalingServer,
        iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
      },
    });
  }
  return dbInstance;
}

function useTodos(userId: string) {
  const [todos, setTodos] = useState([]);

  useEffect(() => {
    let unsubscribe: (() => void) | undefined;

    getDB(userId).then((db) => {
      const load = () => db.todos.query({ orderBy: '_updatedAt', orderDir: 'desc' }).then(setTodos);
      load();
      unsubscribe = db.todos.onChange(() => load());
    });

    return () => unsubscribe?.();
  }, [userId]);

  return todos;
}
```

---

## Publishing Checklist (for maintainers)

Before publishing to npm:

1. Bump `version` in `package.json`
2. `bun run build` — verify `dist/` is clean
3. `bun run test` — all tests pass
4. `bun run typecheck` — no type errors
5. `npm publish --access public` (or `bun publish`)

---

## Project Structure

```
src/
  core/
    types.ts          # IStorageAdapter interface + all shared types
    hlc.ts            # Hybrid Logical Clock implementation
    conflict.ts       # LWW, FWW, and custom conflict resolution
    change-log.ts     # Change log helpers
  storage/
    schema.ts         # Collection schema builder
    indexeddb.ts      # IndexedDB implementation of IStorageAdapter
  sync/
    webrtc-transport.ts  # WebRTC + WebSocket signaling (room-aware; isolates consumer 'rpc' channels)
    gossip.ts            # Gossip sync protocol (K=3 fanout, 30s interval, real-time push)
    snapshot.ts          # Snapshot export / import / chunking
  rpc/                # Consumer RPC layer (experimental)
    protocol.ts          # Wire frames + status codes (docs/rpc-protocol.md)
    router.ts            # RpcRouter — register read/write/stream handlers
    server.ts            # RpcServer — dispatch handlers (runs on Normal Clients)
    client.ts            # RpcClient — transport-agnostic caller (used by ConsumerClient)
    context.ts, errors.ts
  db.ts               # createDB() factory + CollectionProxy (+ optional rpc server)
  consumer.ts         # ConsumerClient — slim entry: @aikofy/client-db/consumer
  index.ts            # Public exports
```

---

## Dependencies

| Package | Why |
|---------|-----|
| [`idb`](https://github.com/jakearchibald/idb) | Promise-based IndexedDB wrapper |
| [`uuid`](https://github.com/uuidjs/uuid) | Node ID generation |

No paywalled or proprietary dependencies.

---

## Contributing

Contributions are welcome! The source lives on GitHub at [aikofy/client-db](https://github.com/aikofy/client-db). Please [open an issue](https://github.com/aikofy/client-db/issues) before submitting a PR for large changes.

```bash
git clone https://github.com/aikofy/client-db.git
cd client-db
bun install
bun run test        # run tests
bun run test:watch  # watch mode
bun run typecheck   # type check
bun run build       # build dist/
```

---

## License

MIT © Aikofy
