# Changelog

All notable changes to `@aikofy/client-db` are documented here. This project adheres to
[Semantic Versioning](https://semver.org/).

## 2.1.0

Performance, security, and large-dataset hardening. No breaking API changes.

### Security

- **Gossip input validation & clock-poisoning defence** — remote docs are validated before they
  touch the replica: malformed `_id`/`_rev`/`_updatedAt` are rejected (a garbage HLC string used
  to parse to `NaN` and could corrupt the local clock), and docs whose HLC is further than
  `sync.maxClockDriftMs` (default 24 h) ahead of the local clock are dropped — one peer with a
  far-future clock can no longer permanently drag every replica's HLC forward and win LWW against
  all later edits. Peer-hello and snapshot watermarks get the same checks.
- **HLC counter overflow guard** — a counter pushed past 6 digits (possible via crafted remote
  timestamps) now rolls into physical time instead of breaking lexicographic `_rev` ordering.
- **RPC auth hardening** — tokens over 8 KB are refused before any decode/verify work; 5 failed
  auth attempts lock the connection out (`RESOURCE_EXHAUSTED`, reconnect to retry), bounding
  online token brute-forcing and verifier CPU burn.
- **RPC concurrency cap** — new `limits.maxInflight` (default 64) caps concurrent in-flight
  calls/streams per consumer; the rate limit alone didn't stop a consumer pinning hundreds of
  long-lived streams.
- **Transport frame caps** — incoming gossip/RPC frames over 64 MB / 4 MB are dropped before
  `JSON.parse`, so a hostile sender can't force huge allocations.

### Performance & large datasets

- **Batched gossip re-broadcast** — applying a sync/snapshot batch now re-broadcasts ONE
  sync-response frame per committed batch instead of one frame per doc (a 500-doc page used to
  fan out 500 messages to every peer). `onChange` listeners likewise receive one callback per
  batch (the existing `ChangeEntry[]` signature now actually carries the batch).
- **Indexed `scan()`** — when a `where` field is backed by a single-field index, `scan()` pages
  over the index (keyset on `_id` via `continuePrimaryKey`), touching O(matches) records instead
  of O(collection). Output and order are unchanged.
- **Conflict log isolation + pruning** — `_conflicts` records no longer tick the HLC, enter the
  change log, or get broadcast to peers; they are pruned on the change-log TTL
  (`changeLogTtlDays`) instead of growing forever.
- **Startup with `initialSnapshot`** — the "has local changes?" check reads 1 change-log entry
  instead of materialising the entire log.
- **Bounded pending-send queues** — messages queued for a peer whose channel never opens are
  capped at 256 (oldest dropped; gossip re-pulls on a later round).
- **Idempotency cache sweep throttled** — expired-entry pruning runs at most once a minute
  instead of on every call (it was O(entries) per request on a busy server).

### Added

- **`db.transaction(fn)` — atomic write batch.** Stage puts/deletes across collections in a
  synchronous callback; everything commits in ONE IndexedDB transaction (docs + change-log
  entries + HLC watermark), all-or-nothing. On failure nothing persists, no events fire,
  nothing is broadcast. Listeners get one `onChange` per collection and peers one frame per
  collection. Note: atomicity is local — cross-peer conflict resolution remains per-doc LWW.
  (`IndexedDBAdapter.applyBatch(ops)` underneath; `BatchOp`, `TxCollectionProxy`,
  `TransactionProxy` types exported.)
- `sync.maxClockDriftMs` (default 24 h) and exported `DEFAULT_MAX_CLOCK_DRIFT_MS`.
- `isValidHLC(ts)` exported for validating externally-sourced timestamps.
- `IndexedDBAdapter.onChangeBatch(cb)`, `putConflict(record)`, `pruneConflicts(olderThanMs)`.
- `GossipSync.broadcastDocs(collection, docs)` (single-frame batch broadcast).
- `RpcLimits.maxInflight` (advertised to consumers in `auth-ok`).

## 2.0.0

### Added — Consumer Client (RPC over WebRTC)

A second client role lets you expose a controlled API surface instead of replicating the whole
database. See the README "Consumer Clients (RPC)" section and `docs/consumer-client-plan.md`.

- **Normal Client as RPC server** — `createDB({ …, rpc: { router } })` turns a full-replica client
  into an RPC server. Define read/write/stream handlers with `RpcRouter`; the handler body is the
  access-control boundary (it reads the full local replica via `ctx.db` but returns only what the
  authenticated `ctx.consumer` may see).
- **Consumer Client** — a thin client at the new `@aikofy/client-db/consumer` entry point (~17 KB,
  no storage/gossip code). Holds no data and never gossips; it authenticates and calls a Normal
  Client's handlers via `invoke()` / `stream()`.
- **Auth & authorization** — `createTokenVerifier` verifies JWS tokens locally with WebCrypto
  (ES256/RS256, rejects `alg:none`, checks `exp`/`nbf`/`iss`/`aud`, revocation hook). Handler
  `scopes` are enforced; expired tokens force re-auth; the client auto-refreshes once.
- **Writes** — idempotent (the SDK auto-attaches a stable key; the server dedupes), replicate to
  other Normal Clients via the existing gossip/CRDT pipeline, and return an HLC watermark.
- **Server-streaming** — async-generator handlers; backpressure-aware (shared with gossip), with
  cancellation on early break or `AbortSignal`.
- **Failover & read-your-writes** — a dropped Normal Client triggers transparent failover to the
  next candidate (round-robin) with re-auth and replay of idempotent calls; reads carry the last
  write watermark so a failed-over replica serves read-your-writes once it has caught up.
- **Hardening** — per-consumer rate limiting (token bucket), payload caps, an `onCall`
  observability/audit hook, capability negotiation (client fails fast on unsupported methods), and
  a protocol-version check.

### Protocol / specs

- New wire contracts documented in `docs/rpc-protocol.md` and `docs/signaling-protocol.md`.

### Compatibility

- **No breaking changes to existing sync APIs.** `createDB`, collections, `query`/`put`/`delete`,
  snapshots, and gossip behave as before; the new `rpc` option and `sync.serveConsumers` are
  optional.
- **Signaling server:** Consumer support requires a **role-aware signaling server** (see
  `docs/signaling-protocol.md`). The client now sends `role`/`serveConsumers` on `register`;
  servers that ignore unknown fields keep working for Normal-Client-only sync. To serve Consumers,
  update the signaling server (e.g. `@aikofy/client-db-sync`) per the spec.

## 1.1.0

- WebRTC gossip sync, HLC ordering, pluggable conflict resolution, snapshot bootstrap, and the
  streaming (bounded-memory) export/import APIs.
