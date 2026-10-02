# AGENTS.md

Guidance for AI coding agents working in `@aikofy/client-db`. Read this before changing code.
User-facing API docs live in [README.md](README.md); this file covers **how the code is built and
the rules that keep it correct**.

## Role

You are a Senior Software Engineer. You love to write clean code and use coding best practices and
Structural Patterns. Prefer small, cohesive modules, explicit interfaces, dependency injection, and
comments that explain *why* (invariants, races, trade-offs) rather than *what*.

## What this project is

An offline-first, peer-to-peer **IndexedDB sync library** for browsers (TypeScript, ESM + CJS):

- **Normal Client** — a full local replica (`createDB`). Writes are stamped with a **Hybrid Logical
  Clock** (HLC), recorded in a change log, and replicated to peers by **gossip over WebRTC data
  channels** (`'sync'` label). Conflicts are resolved per document (LWW / first-write-wins / custom).
- **Consumer Client** — a thin client (`@aikofy/client-db/consumer`) that holds **no database and never
  gossips**. It calls read/write/stream handlers that a Normal Client exposes over an isolated WebRTC
  `'rpc'` data channel.
- The **signaling server** (`@aikofy/client-db-sync`) is a separate repo. Only its protocol spec lives
  here ([docs/signaling-protocol.md](docs/signaling-protocol.md)).

## Commands

Bun is the package runner. Run all three checks before you say a change is done.

```bash
bun install
bun run test         # vitest run (node env, fake-indexeddb, fake WebRTC)
bun run typecheck    # tsc --noEmit (strict)
bun run build        # tsup → dist/ (two entries: index + consumer)
bun run test:watch   # watch mode
bun bench/perf-baseline.mjs   # IDB transaction-count / scaling probe (see also perf-compare.mjs)
```

Run a single file: `bun run vitest run src/sync/gossip.test.ts`.

## Architecture

```
                 ┌──────────── src/index.ts (public API, full bundle) ────────────┐
 createDB() ───► db.ts  (Facade: wires everything, returns typed collection proxies)
                  │
   ┌──────────────┼───────────────────────┬────────────────────────────┐
   ▼              ▼                       ▼                            ▼
 storage/       sync/gossip.ts         sync/webrtc-transport.ts     rpc/server.ts
 indexeddb.ts   (delta sync, bootstrap, (signaling WS, peer conns,   (sessions, auth, limits,
 (IStorage-      snapshot fallback,     'sync' vs 'rpc' isolation,    dispatch to RpcRouter)
  Adapter)       conflict apply)        backpressure)
   │              │
   ▼              ▼
 core/  hlc.ts · conflict.ts · types.ts · change-log.ts   (pure, dependency-free)

 src/consumer.ts (slim entry) ──► rpc/client.ts · rpc/protocol.ts · rpc/errors.ts   ONLY
```

| Layer | Files | Responsibility |
|---|---|---|
| Core | `src/core/` | HLC (`<ms_16>-<counter_6>-<nodeId>`, lexicographically sortable), conflict strategies, all shared types including the `IStorageAdapter` contract and the `SyncMessage` union. No I/O. |
| Storage | `src/storage/` | `IndexedDBAdapter` implements `IStorageAdapter` using `idb`. Object stores per collection plus system stores `_meta`, `_changes`, `_conflicts`. Query/scan, atomic batches, streaming export/import, compaction. `schema.ts` builds store/index definitions. |
| Sync | `src/sync/` | `WebRTCTransport` (signaling, glare handling, reconnect backoff, frame caps, consumer isolation). `GossipSync` (fanout 3 every 30 s, real-time push, paged pull/push, bootstrap, TTL pruning). `snapshot.ts` (streamed snapshot send/receive). `backpressure.ts` (shared `drainIfNeeded`). |
| RPC | `src/rpc/` | `RpcRouter` (handler registry), `RpcServer` (runs on Normal Clients), `RpcClient` (transport-agnostic caller), `createTokenVerifier` (WebCrypto JWS), `IdempotencyCache`, `TokenBucket`, wire frames in `protocol.ts`. |
| Facade | `src/db.ts`, `src/consumer.ts` | `createDB` composes adapter + transport + gossip + optional RPC server. `ConsumerClient` owns signaling, the `'rpc'` channel, round-robin failover and replay. |
| Utilities | `src/snapshot-stream.ts` | NDJSON ⇄ `SnapshotChunk` bridge for bounded-memory backups. |

### Data flow of a local write

`db.todos.put()` → `IndexedDBAdapter.put` ticks the HLC and, in **one** IDB transaction, writes the doc,
the `_changes` entry and the `_meta.hlc` watermark → after commit, `_emitChanges` → `onChangeBatch`
listener in `db.ts` → user `onChange` callbacks + `gossip.broadcastDocs` (one frame per collection batch).

### Data flow of a remote change

Transport `onMessage` → `GossipSync._handleMessage` → `_applyRemoteChanges`: validate → `hlc.update` →
group by collection → dedupe by `_id` → `getMany` prefetch → `resolveConflict` (+ local-only
`logConflict`) → one `bulkInsert` → `persistHLC` if the clock moved.

## Design patterns in use (follow them, don't fight them)

| Pattern | Where | Notes |
|---|---|---|
| **Adapter / Port** | `IStorageAdapter` ← `IndexedDBAdapter` | The interface exists so a SQLite adapter can be swapped in later. Put new storage capabilities on the interface when they are part of the contract. |
| **Strategy** | `ConflictStrategy` (`'lww'` · `'first-write-wins'` · resolver fn), `TokenVerifier`, `Validator` (`{ parse }`, zod-compatible) | Pluggable behaviour is injected as a value, not chosen by `if` chains in callers. |
| **Facade** | `createDB`, `ConsumerClient` | Hide wiring. Users never construct the transport, gossip or server themselves. |
| **Proxy** | `CollectionProxy`, `TxCollectionProxy` | Typed per-collection views over the single adapter. |
| **Unit of Work** | `db.transaction(fn)` → `BatchOp[]` → `adapter.applyBatch` | Staging is synchronous. The IDB transaction opens only at commit. |
| **Observer** | `onChange`, `onChangeEntry`, `onChangeBatch`, transport `on*` hooks | Prefer batch notifications. `GossipSync.start()` *chains* onto existing transport hooks. Never overwrite a hook another layer wired. |
| **Registry / fluent builder** | `RpcRouter.read/.write/.stream` return `this`. `_openDbs` dedupes `createDB` by name. | Duplicate handler ids throw. |
| **Pipeline / guard chain** | `RpcServer._handleReq` | auth → token expiry → rate limit → in-flight cap → payload cap → method lookup → scopes → `readAfter` → input validation → handler. New checks go into this chain in the right order. |
| **Dependency injection** | `RpcServerConfig` (`send`, `sendAsync`, `hlc`, `adapter`, `verifyToken`), `RpcClientConfig`, injectable clocks in `IdempotencyCache` / `TokenBucket` | Keeps RPC transport-agnostic and testable. Inject time and I/O. Don't reach for globals. |
| **Discriminated unions** | `SyncMessage`, `ClientFrame` / `ServerFrame`, `SnapshotChunk`, `BatchOp` | Switch on `type` / `kind`. Add a new variant to the union instead of adding loose fields. |

## Invariants: do not break these

### Storage and HLC
1. **One transaction per local write.** Data + change entry + HLC watermark commit atomically. Emit
   change events only **after** `tx.done`.
2. **Never `await` non-IDB work inside an open IDB transaction.** It auto-commits. That is why
   `db.transaction(fn)` stages synchronously and `applyBatch` opens the transaction afterwards.
3. **Multi-op transactions must abort explicitly on error** (`tx.abort()` + swallow `tx.done`). Otherwise
   the operations already queued auto-commit.
4. **Local-only records never tick the HLC, enter `_changes`, or gossip**: conflict audits
   (`putConflict`), `_meta`, `hardDelete`, `compact`/`pruneTombstones`.
5. **Listener errors must not reject a committed write.** Rethrow them out of band with `queueMicrotask`.
6. **Bounded memory.** Page with keyset cursors (`_id` / `_updatedAt` lower bounds), never growing
   offsets. Respect batch sizes. `getAll(range, 0)` means "unlimited", so clamp limits to ≥ 1.
7. Booleans are invalid IDB keys, so the `_deleted` index is effectively empty. Filter `_deleted` in
   memory. Single-field index fast paths must return exactly the same docs **in the same `_id` order**
   as the full-scan path (see `perf-equivalence.test.ts`).

### Sync safety
8. **Validate everything from the network before use**: `isValidHLC` + `maxClockDriftMs` (`_acceptableHlc`),
   `_id` type/length, known collection, frame-size caps *before* `JSON.parse`. A bad HLC poisons the
   clock for the whole replica.
9. **Strip the transport-only `_collection` tag** before persisting remote docs.
10. **Persist the HLC after remote changes advance it.** `bulkInsert` does not persist it on its own.
11. **Watermarks (`lastSync:<peer>`) advance only on success.** A failed or dropped message is re-pulled
    next round. That is what makes dropping queued messages safe.
12. **Tombstone GC is sync-safe only in this order**: `pruneChanges(horizon)` first, then purge tombstones
    that are older than the horizon **and** strictly older than the oldest surviving change entry. Peers
    behind that boundary get `needsFullSync`. `hardDelete` is intentionally unsafe (it can resurrect).

### Consumer isolation (the RPC safety contract, see [docs/consumer-client-plan.md §2](docs/consumer-client-plan.md))
13. Consumers live in `consumerStates`, **never** `peerStates`. They may only open an `'rpc'` channel. They
    never receive gossip or snapshots. The only data a consumer gets is a handler's return value.
14. **The handler body is the access-control boundary.** `ctx.db` is the full replica.
15. **Identity comes from the verified token** (`ctx.consumer`), never from client-supplied fields.
16. **`src/consumer.ts` must stay slim.** It must not import `storage/`, `sync/gossip|snapshot|webrtc-transport`,
    `db.ts`, `idb` or `uuid`. `src/consumer.imports.test.ts` enforces this. Shared code it needs belongs in
    `rpc/client.ts`, `rpc/protocol.ts` or `rpc/errors.ts`.

## Coding conventions

- **TypeScript strict**, ES2020 target, `isolatedModules`. No `any`. Narrow `unknown` with type guards.
- **ESM import specifiers end in `.js`** even though the source files are `.ts` (`'./core/hlc.js'`). Use
  `import type` for type-only imports.
- Naming: classes `PascalCase`; private members and internal helpers are prefixed with `_`
  (`_syncWithPeer`, `_approxBytes`); module constants are `SCREAMING_SNAKE_CASE` at the top of the file
  with a unit suffix (`GOSSIP_INTERVAL_MS`, `MAX_RPC_MESSAGE_BYTES`). Export a default constant when
  users may need it (`DEFAULT_LIMITS`, `DEFAULT_MAX_CLOCK_DRIFT_MS`).
- Group long files with section rules: `// ─── Section name ───────────`.
- **JSDoc every public API.** Explain semantics, defaults and safety (`SYNC-SAFE`, `⚠️ SYNC-UNSAFE`).
  Inline comments explain *why*: the race, the invariant, the cost. Don't narrate the code.
- Fire-and-forget promises are explicit: `void p.catch(...)`. Never leave a floating promise that can
  reject unhandled.
- Errors: RPC handlers throw `RpcError(status, message)` for a specific status. Anything else maps to
  `INTERNAL`. Retryability defaults come from `defaultRetryable`.
- Timers and listeners must be cleared on `stop()` / `close()` / `disconnect()`. Every test closes its DB.
- No new runtime dependencies without a strong reason. Runtime deps are only `idb` and `uuid`, and the
  consumer bundle uses neither.

## Testing conventions

- Vitest with `globals: true`, node environment. Tests are colocated as `*.test.ts` next to the code.
- IndexedDB: `import 'fake-indexeddb/auto'` and reset `globalThis.indexedDB = new IDBFactory()` in
  `beforeEach`. Use a unique DB name per test (`` `tx-${Math.random()}` ``) because `createDB` dedupes by name.
- WebRTC/signaling: use `src/test-utils/fake-webrtc.ts` (fake peer connections, data channels with
  `bufferedAmount`, `FakeSignalingHub`). JWTs: `src/test-utils/jwt.ts` (`makeEs256Signer`). Neither is
  imported by an entry point, so neither ships.
- Every bug fix gets a regression test. Every performance change that alters an access path gets an
  equivalence test proving identical output (see `storage/perf-equivalence.test.ts`).
- Test names state the behaviour and the guarantee ("rolls back EVERYTHING when one op fails").

## Definition of done for a change

1. `bun run test`, `bun run typecheck` and `bun run build` are green.
2. Public API change: export it from `src/index.ts` (or `src/consumer.ts` for consumer-side code), and
   document it in the README **API Reference**.
3. Add a `CHANGELOG.md` entry and bump `package.json` per semver. A new required method on
   `IStorageAdapter` breaks custom adapters, so call it out.
4. Wire-protocol change (RPC frames, signaling messages, `SyncMessage`): update
   [docs/rpc-protocol.md](docs/rpc-protocol.md) / [docs/signaling-protocol.md](docs/signaling-protocol.md).
   Bump `PROTOCOL_VERSION` if old peers can't interoperate. Signaling changes also need the external
   `client-db-sync` repo.
5. Keep docs current **as you go**. Don't leave README or docs updates for the end.
6. Commit or push only when asked.

## Key files to read first

| Task | Start here |
|---|---|
| Storage / query / compaction | `src/core/types.ts` (`IStorageAdapter` JSDoc), `src/storage/indexeddb.ts` |
| Replication bug | `src/sync/gossip.ts` (`_syncWithPeer`, `_applyRemoteChanges`, `_maybeBootstrap`) |
| Connection / signaling | `src/sync/webrtc-transport.ts`, `docs/signaling-protocol.md` |
| RPC server behaviour | `src/rpc/server.ts`, `docs/rpc-protocol.md` |
| Consumer SDK | `src/consumer.ts`, `src/rpc/client.ts` |
| Design history and rationale | `docs/consumer-client-plan.md`, `CHANGELOG.md`, `indexeddb-sync-wrapper-prompt.md` (original v1 spec) |

## Known tech debt (fix opportunistically, don't spread it)

The phased fix plan, which also covers bugs not listed here, is in
[docs/tech-debt-plan.md](docs/tech-debt-plan.md). Mark phases done there, and remove an item below when
its phase lands.

- `GossipSync` and `sync/snapshot.ts` depend on the concrete `IndexedDBAdapter` (`getMany`,
  `getManyForChanges`, `_querySnapshotBatch`, `getHLC`, `persistHLC`, meta accessors), not on
  `IStorageAdapter`. A second adapter would need these lifted into an extended interface. Don't add
  new concrete-type casts.
- `requestSnapshot` reads the transport's private `config.nodeId` through a cast. Prefer a public getter.
- `_activeStreams` in `snapshot.ts` is module-level state, keyed per adapter for safety.
- `appendChange` / `queryChanges` in `src/core/change-log.ts` are unused. Only `CHANGES_STORE` is.
- Several comments still say "Phase N will…" (`db.ts`, `core/types.ts` `serveConsumers`, `rpc/context.ts`,
  `rpc/router.ts`, `rpc/server.ts`, `webrtc-transport.ts`). All 9 phases are done, so update a comment
  when you touch that code.
- `ctx.log` is a no-op. Observability goes through `RpcConfig.onCall` (`CallRecord`).
- Cross-node idempotency dedupe is not replicated, so writes don't replay on failover unless the caller
  passes `{ idempotent: true }`. Fastest-by-ping server selection is not implemented (round-robin only).
