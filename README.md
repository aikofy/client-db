# @aikofy/client-db

A TypeScript-first, offline-ready, peer-to-peer syncing database for web browsers. Built on IndexedDB with a clean storage abstraction, Hybrid Logical Clock (HLC) ordering, pluggable conflict resolution, and WebRTC gossip sync — no server required for data replication.

[![npm version](https://img.shields.io/npm/v/@aikofy/client-db)](https://www.npmjs.com/package/@aikofy/client-db)
[![license](https://img.shields.io/npm/l/@aikofy/client-db)](./LICENSE)

---

## Features

- **Offline-first** — writes always succeed locally; sync happens automatically on reconnect
- **Peer-to-peer sync** — gossip protocol over WebRTC data channels, no central database server needed
- **Hybrid Logical Clock (HLC)** — causal ordering of writes across nodes without relying on wall-clock agreement
- **Pluggable conflict resolution** — Last-Write-Wins (default), First-Write-Wins, or a custom resolver per collection
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
  console.log('changed:', changes);
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

### Development (no auth)

Start the signaling server with `AUTH_DISABLED=true` and connect without a token:

```typescript
const db = await createDB({
  name: 'myapp',
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

When auth is enabled on the signaling server, your backend issues a token and passes it to the client. The token is appended as a `?token=` query parameter:

```typescript
// 1. Your backend fetches a token from the signaling server
const { token } = await fetch('https://signal.example.com/token', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'x-admin-secret': process.env.SIGNAL_ADMIN_SECRET,
  },
  body: JSON.stringify({ ttl: '24h', subject: currentUser.id }),
}).then(r => r.json());

// 2. Pass the token in the signalingServer URL
const db = await createDB({
  name: 'myapp',
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

```typescript
console.log(db.syncStatus); // 'online' | 'offline' | 'syncing'
```

Once connected, the library:
1. Discovers peers via the signaling server
2. Opens RTCDataChannel connections with up to K=3 random peers (gossip fanout)
3. Exchanges only records changed since the last sync (`HLCTimestamp`)
4. Re-gossips every 30 seconds and immediately on `window.online`
5. Bootstraps new peers with a full snapshot, then switches to delta sync

> **Note:** The signaling server is only used for WebRTC handshake (offer/answer/ICE candidates). After connection, all data flows directly peer-to-peer.

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
| `name` | `string` | IndexedDB database name |
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
| `signalingServer` | `string` | WebSocket URL of the signaling server. Append `?token=<jwt>` when auth is enabled on `@aikofy/client-db-sync`. Use plain `ws://` without a token when `AUTH_DISABLED=true`. |
| `iceServers` | `IceServer[]` | STUN/TURN servers for WebRTC |
| `nodeId` | `string` (optional) | Override auto-generated node UUID |

---

### `CollectionProxy<T>`

All collection operations are async and available on `db.<collectionName>`.

#### `put(doc)`

```typescript
const saved = await db.todos.put({ _id: 'abc', title: 'Hello', status: 'open' });
// _id is auto-generated (UUID) if omitted
```

Upserts a record. Stamps `_rev` (HLC), `_updatedAt` (HLC), `_deleted: false`. Returns the full `Doc<T>`.

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

Soft delete — sets `_deleted: true` with a new HLC revision. The record remains in IndexedDB and is synced as a deletion event to peers.

#### `onChange(callback)`

```typescript
const unsubscribe = db.todos.onChange((changes: ChangeEntry[]) => {
  // fires on local writes AND incoming sync changes
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

// Import snapshot and resume delta sync from its HLC
await db.import(snapshot);
```

Large snapshots (>256 KB) are automatically chunked over the WebRTC data channel.

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

## React Example

```tsx
import { useEffect, useState } from 'react';
import { createDB } from '@aikofy/client-db';
import type { TypedDB } from '@aikofy/client-db';

const collections = {
  todos: { indexes: ['status'], conflictStrategy: 'lww' as const },
};

// Fetch a short-lived token from your own backend (which calls @aikofy/client-db-sync's POST /token).
// In dev, skip this and use AUTH_DISABLED=true on the signaling server.
async function fetchSignalToken(): Promise<string> {
  const res = await fetch('/api/signal-token', { method: 'POST' });
  const { token } = await res.json() as { token: string };
  return token;
}

let dbInstance: TypedDB<typeof collections> | null = null;

async function getDB() {
  if (!dbInstance) {
    const isDev = import.meta.env.DEV; // Vite / any bundler dev flag
    const signalingServer = isDev
      ? 'ws://localhost:8080/signal'
      : `wss://signal.example.com/signal?token=${await fetchSignalToken()}`;

    dbInstance = await createDB({
      name: 'myapp',
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

function useTodos() {
  const [todos, setTodos] = useState([]);

  useEffect(() => {
    let unsubscribe: (() => void) | undefined;

    getDB().then((db) => {
      const load = () => db.todos.query({ orderBy: '_updatedAt', orderDir: 'desc' }).then(setTodos);
      load();
      unsubscribe = db.todos.onChange(() => load());
    });

    return () => unsubscribe?.();
  }, []);

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
    webrtc-transport.ts  # WebRTC + WebSocket signaling
    gossip.ts            # Gossip sync protocol (K=3 fanout, 30s interval)
    snapshot.ts          # Snapshot export / import / chunking
  db.ts               # createDB() factory + CollectionProxy
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

Contributions are welcome! Please open an issue before submitting a PR for large changes.

```bash
bun install
bun run test        # run tests
bun run test:watch  # watch mode
bun run typecheck   # type check
bun run build       # build dist/
```

---

## License

MIT © Aikofy
