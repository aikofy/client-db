# Tech-Debt & Hardening Plan

> Status: **proposed** · Baseline: `4467ed8` (v2.3.0), 194 tests green · Owner: maintainer.
> Work through the phases in order. Each phase is independently shippable, with its own tests and release.
> Mark a phase `✅ DONE` here when it lands, as in [consumer-client-plan.md](consumer-client-plan.md).

## How to use this plan

- **One phase = one branch, one PR, one release.** Don't mix phases. Phases 1–3 are low-risk and can
  each be finished in a sitting.
- Every phase follows the *Definition of done* in [AGENTS.md](../AGENTS.md): `bun run test`,
  `bun run typecheck` and `bun run build` are green, `CHANGELOG.md` has an entry, and docs are updated
  as you go.
- **Behaviour-preserving phases (2, 3, 4)** must not change any existing test's expectations. If a test
  needs editing, the change isn't a pure refactor. Stop and reconsider.
- Line numbers are as of `4467ed8`. Re-check them with `grep` before editing.

## Overview

| # | Phase | Kind | Risk | Release | Status |
|---|---|---|---|---|---|
| 0 | Baseline & safety net | prep | none | — | ☐ |
| 1 | Correctness bugs | fix | low | 2.3.1 | ☐ |
| 2 | Dead code, stale comments, docs | cleanup | none | 2.3.2 | ☐ |
| 3 | DRY refactors inside modules | refactor | low | 2.3.3 | ☐ |
| 4 | Decouple sync from the concrete adapter | architecture | medium | 2.4.0 | ☐ |
| 5 | Observability & efficiency | feature | low | 2.5.0 | ☐ |
| 6 | Typed collections (optional) | feature | low | 2.6.0 | ☐ |
| 7 | Deferred RPC features (idempotency store, fastest selection) | feature | high | 2.7.0+ | ☐ |
| 8 | Next major: remove deprecations | breaking | medium | 3.0.0 | ☐ |

---

## Phase 0 — Baseline & safety net

**Goal:** capture numbers to compare against before touching anything. Phases 3 and 4 touch hot paths.

- [ ] Run `bun bench/perf-baseline.mjs` and save the output to `bench/baseline-2.3.0.txt` (or to the PR
      description).
- [ ] Record the test count (194) and bundle sizes from `bun run build` (`dist/index.js`,
      `dist/consumer.js`).

**Done when:** you have numbers to compare against after Phases 3 and 4.

---

## Phase 1 — Correctness bugs

**Goal:** fix real defects. Each fix gets a regression test that fails before the fix.

### 1.1 Floating promise in the maintenance cycle
- `src/sync/gossip.ts:130`: `void this.adapter.pruneConflicts(this.ttlMs);` has no `.catch`. A rejection
  (closed DB, quota) becomes an **unhandled rejection**, unlike the `pruneChanges` chain right above it.
- [ ] Attach the same `.catch((err) => queueMicrotask(() => { throw err; }))`, or the `onError` hook once
      Phase 5 adds it.
- **Test:** stub `pruneConflicts` to reject. Assert that no unhandled rejection occurs and the prune
  interval keeps running.

### 1.2 `GossipSync.stop()` leaves timers and promises behind
- `gossip.ts:113`: the initial `setTimeout(… _gossipRound, 1000)` is not stored, so `stop()` can't clear it.
- `gossip.ts:327`: `_requestPage` holds a 15 s timeout per in-flight request. `stop()` (`gossip.ts:137`)
  never clears `pendingRequests`, so after `db.close()` the timers keep the event loop alive for up to
  15 s, and the promises reject late.
- [ ] Store the initial timer and clear it in `stop()`.
- [ ] Track `{ resolve, reject, timer }` per pending request. In `stop()`, clear every timer, reject every
      pending request with `Error('GossipSync stopped')`, and empty the map.
- **Test:** start a sync, call `db.close()` mid-request, and assert that no timers remain
  (`vi.getTimerCount()` with fake timers) and the request promise rejects promptly.

### 1.3 Streams don't get read-your-writes
- `src/rpc/client.ts:164`: `_stream` sends `{ type: 'req', id, method, params, deadlineMs }` and never
  attaches `readAfter = lastHlc`, unlike `invoke` (`client.ts:98-101`). The server already honours
  `readAfter` for streams (`server.ts:431`), but the client never sends it. After a failover, a stream
  can miss the consumer's own writes.
- [ ] Attach `opts.readAfter ?? this.lastHlc` to stream requests, as `invoke` does.
- [ ] Decide whether streams should also retry once on `UNAUTHENTICATED` (re-auth), as `invoke` does
      (`client.ts:111-118`). Recommended: yes, but only before the first chunk arrives.
- **Test:** in `rpc/client.test.ts`, do a write (server returns `hlc: 'H1'`), then a stream, and assert the
  stream's `req` frame has `readAfter: 'H1'`.

### 1.4 Unary and stream calls check things in a different order
- Unary `_runHandler` (`server.ts:329-348`): scopes → **readAfter wait** → input validation.
- Stream `_runStream` (`server.ts:416-433`): scopes → **input validation** → readAfter wait.
- With the unary order, a malformed request waits up to `readAfterTimeoutMs` (2 s) before it is rejected
  with `INVALID_ARGUMENT`.
- [ ] Use one order everywhere: **scopes → validate → readAfter → handler** (cheap failures first).
- [ ] Update the pipeline order in `AGENTS.md` (Design patterns table) to match.
- **Test:** unary request with invalid params plus an unreachable `readAfter`. Assert `INVALID_ARGUMENT`
  comes back immediately, not after the timeout.

**Release:** 2.3.1 (patch). CHANGELOG under `### Fixed`.

---

## Phase 2 — Dead code, stale comments, docs

**Goal:** no behaviour change. Make the code tell the truth.

### 2.1 Dead code
- [ ] `src/core/change-log.ts`: delete `appendChange` and `queryChanges` (unused, not exported). Keep
      `CHANGES_STORE`.
- [ ] `src/storage/indexeddb.ts:134-136`: delete the `_persistHLC()` wrapper and call `persistHLC()` directly
      (2 call sites: `import`, `importStream`).
- [ ] `src/consumer.ts:83` and `:87`: `invoke` awaits `connect()` twice. Drop the one before the loop.

### 2.2 Stale "Phase N" comments (all 9 consumer-client phases are done)
- [ ] `src/db.ts:233-235`: remove the "wired in Phase 2; until then…" note (it is wired at `db.ts:278`).
- [ ] `src/db.ts:277`, `src/core/types.ts:156` (`serveConsumers` "lands in Phase 2").
- [ ] `src/rpc/context.ts:3, 19, 22-23, 30`; `src/rpc/router.ts:16` ("enforced from Phase 4").
- [ ] `src/rpc/server.ts:24-25, 354`; `src/rpc/client.ts:64, 94`.
- [ ] `src/sync/webrtc-transport.ts:29, 75`.
- [ ] `src/sync/gossip.ts:360, 387`: these are steps of one sync, not project phases. Rename them to
      "Step 1: pull" / "Step 2: push" so they aren't confused with project phases.
- [ ] `src/test-utils/fake-webrtc.ts:10, 286` and `src/test-utils/jwt.ts:3`: "only `src/index.ts` is built" is
      wrong (`consumer.ts` is built too). Say "not imported by any entry point".

### 2.3 Narrow by the message type instead of casting
- [ ] `gossip.ts:420-445`: replace the `if/else` chain and its `msg as SnapshotStream…Message` casts with a
      `switch (msg.type)`. TypeScript narrows the union, so the casts go away.

### 2.4 Deprecate legacy public surface (remove in Phase 8)
- [ ] `SnapshotChunkMessage` and `SnapshotResponseMessage` (`core/types.ts:294-306`) are in the
      `SyncMessage` union and exported, but nothing sends or handles them (replaced by `snapshot-stream-*`).
      Add `/** @deprecated … removed in 3.0 */`.
- [ ] `exportSnapshot` / `importSnapshot` (`sync/snapshot.ts:16-25`) only wrap `adapter.export/import`, and
      `handleSnapshotStreamStart` is a no-op. Mark them `@deprecated`.

### 2.5 Docs
- [ ] README **Project Structure**: add `snapshot-stream.ts`, `test-utils/`, `bench/` and `docs/tech-debt-plan.md`.
- [ ] Note the deprecations in the CHANGELOG.

**Release:** 2.3.2 (patch). Verify: the test count and expectations are unchanged.

---

## Phase 3 — DRY refactors inside modules

**Goal:** remove duplication without changing behaviour. Compare benchmark numbers with Phase 0.

### 3.1 One `appendParams`
- Two copies: `src/consumer.ts:340` and `src/sync/webrtc-transport.ts:544`.
- [ ] Move it to `src/core/url.ts` (pure, no dependencies). The consumer import guard
      (`consumer.imports.test.ts`) allows `core/`. Import it from both places. Add a unit test
      (existing `?`, encoding).

### 3.2 `IndexedDBAdapter` private helpers
- [ ] `_cutoffHlc(olderThanMs)`: the `formatHLC({ physicalMs: Date.now() - x, counter: 0, nodeId: '' })`
      expression is repeated at `:797`, `:838` and `:873`.
- [ ] `_resolveTargets(collections?)`: `collections ? collections.filter(all.includes) : all` is repeated at
      `:885`, `:976` and `:1016`.
- [ ] `_readMeta()`: the meta → record loop is repeated in `export()` (`:638-645`) and `exportStream()`
      (`:678-685`).
- [ ] `_deleteUpTo(storeName, cutoffHlc)`: the cursor-delete loop over the `_updatedAt` index is shared by
      `pruneChanges` (`:799-809`) and `pruneConflicts` (`:840-849`).
- [ ] Optional: split the 1,060-line file. Move query/scan planning (`_pickIndexedWhereField`,
      `_buildWherePairs`, `_matchesPairs`, `_updatedAtLowerBound`) into `storage/query-plan.ts` as pure
      functions that are easy to unit-test.

### 3.3 `RpcServer`: one shared path for setting up and finishing a call
- [ ] `_authorize(def, identity)`: one scope check (now duplicated at `server.ts:329-335` and `:416-420`).
- [ ] `_validate(def, params)`: one input-validation step (`:342-348` and `:422-429`).
- [ ] `_trackCall(consumerId, session, def, frame, start)`: returns `{ ac, finish }` with the deadline timer,
      the in-flight bookkeeping, and `_record`. It replaces the near-identical `finish` closures at
      `:298-308` and `:374-385`, plus the hand-written settle at `:391-399`.
- **Check:** `server.test.ts`, `stream.test.ts`, `hardening.test.ts`, `readafter.test.ts` and
  `write.test.ts` pass unchanged.

### 3.4 `logConflict` duck-typing → an optional interface method
- `core/conflict.ts:34-37` casts the adapter to find a `putConflict` method.
- [ ] Add `putConflict?(record): Promise<void>` to `IStorageAdapter` as an **optional** method (custom
      adapters keep working) and drop the cast.

### 3.5 `GossipSync` options object
- The constructor takes 3 trailing positional numbers/booleans (`gossip.ts:63-70`), which is easy to get
  wrong.
- [ ] Add `GossipSyncOptions { changeLogTtlDays?, maxClockDriftMs?, autoCompact? }`. `GossipSync` is
      **publicly exported**, so accept `ttlDaysOrOptions: number | GossipSyncOptions` for now and deprecate
      the positional form (removed in Phase 8). Update `db.ts:237-244`.

**Release:** 2.3.3 (patch). Verify: no test expectation changed, and benchmark numbers are within noise of
Phase 0.

---

## Phase 4 — Decouple sync from the concrete adapter

**Goal:** make the `IStorageAdapter` contract real, so a SQLite (mobile) adapter can be dropped in. This
was the original design goal in `indexeddb-sync-wrapper-prompt.md`. Today `GossipSync`, `sync/snapshot.ts`
and `db.ts` reach past the interface into `IndexedDBAdapter`.

### 4.1 Split the adapter into small interfaces (interface segregation)
Add these to `src/core/types.ts`:

| Interface | Members (currently concrete-only) | Used by |
|---|---|---|
| `IClockedStore` | `nodeId`, `getHLC()` (or narrower `hlcNow()` / `hlcUpdate(ts)`), `persistHLC()` | gossip, snapshot, rpc |
| `ISyncStorage extends IStorageAdapter, IClockedStore` | `getMany`, `getManyForChanges`, `querySnapshotBatch`, `getMetaValue`, `setMetaValue`, `areStoresEmpty`, `pruneConflicts`, `onChangeBatch` | `GossipSync`, `snapshot.ts` |
| `ILocalStore extends ISyncStorage` | `open`, `scan`, `applyBatch`, `exportStream`, `importStream`, `compact`, `hardDelete` | `db.ts` |

- [ ] `IndexedDBAdapter implements ILocalStore`.
- [ ] Rename `_querySnapshotBatch` → `querySnapshotBatch`. The underscore is wrong for a method other
      modules call. Keep `_querySnapshotBatch` as a `@deprecated` alias until Phase 8.

### 4.2 Depend on interfaces, not on the class
- [ ] `gossip.ts:25, 41, 65, 590`: `IndexedDBAdapter` → `ISyncStorage`.
- [ ] `snapshot.ts:35, 162`: remove both `as IndexedDBAdapter` casts. Type the parameters as `ISyncStorage`.
- [ ] `db.ts:151` (`makeCollectionProxy`) → `ILocalStore`. `db.ts:201` stays the single place that names
      `IndexedDBAdapter` (the composition root).
- [ ] Check with `grep -rn "IndexedDBAdapter" src | grep -v test`: only `db.ts:201`, `index.ts` and
      `storage/` should remain.

### 4.3 Transport seam
- [ ] `snapshot.ts:111` reads the transport's private `config.nodeId` through
      `as unknown as { config: … }`. Add `get nodeId(): string` to `WebRTCTransport`.
- [ ] Define `ISyncTransport` (`peers`, `send`, `sendAsync`, `broadcast`, `nodeId`, and the
      `onMessage` / `onPeerConnected` / `onPeerDisconnected` hooks). Have `GossipSync` and `snapshot.ts`
      depend on it, so gossip can be unit-tested with a ~30-line in-memory fake instead of the full fake
      WebRTC stack.

### 4.4 Snapshot receive state owned per instance
- `_activeStreams` (`snapshot.ts:96`) is module-level state shared by every DB in the page.
- [ ] Add a `SnapshotReceiver` class (`request`, `onBatch`, `onEnd`, `cancelAll`) that each `GossipSync`
      owns. Keep the exported free functions as thin `@deprecated` wrappers over a module default instance
      until Phase 8.

### 4.5 Adapter contract test suite
- [ ] Add `src/storage/adapter.contract.ts`, a `runAdapterContract(name, factory)` suite that covers the
      `ILocalStore` semantics (system fields, HLC monotonicity, tombstones, change-log paging, `applyBatch`
      atomicity, `pruneTombstones` safety boundary, scan/query equivalence). Run it against
      `IndexedDBAdapter` in `indexeddb.contract.test.ts`. A future SQLite adapter reuses it as is.

### 4.6 Docs
- [ ] README **Advanced: Custom Storage Adapter**: document which interface a custom adapter needs for
      local-only use vs with sync.
- [ ] Remove the matching entries from the AGENTS.md *Known tech debt* list.

**Release:** 2.4.0 (minor: new exported interfaces). Verify: no test expectation changed, the contract
suite is green, and benchmark numbers are within noise.

---

## Phase 5 — Observability & efficiency

**Goal:** give users control over background errors and logs, and stop the server from polling.

### 5.1 `onError` hook instead of uncatchable microtask throws
- Four places rethrow background errors with `queueMicrotask(() => { throw err })`: listener errors in
  `indexeddb.ts` (`_emitChanges`), the gossip dispatch catch, and the prune chain. Apps can't route these
  into their own error reporting.
- [ ] Add `SyncConfig.onError?: (err: unknown, ctx: { source: 'gossip' | 'maintenance' | 'listener'; peerId?: string }) => void`.
      Fall back to today's microtask throw when it isn't set, so default behaviour is unchanged.

### 5.2 Implement `ctx.log`
- `server.ts:466`: `log: () => undefined`.
- [ ] Add `RpcConfig.logger?: (entry: { level, message, meta, method, requestId, identityId }) => void` and
      wire `ctx.log` to it, filling in request context automatically. The default stays a no-op.

### 5.3 Event-driven `readAfter` instead of 50 ms polling
- `server.ts:103-112` polls `hlc()` every 50 ms for up to 2 s per waiting request.
- [ ] Have the adapter notify when the HLC advances (e.g. `onHlcAdvance(cb)` on `IClockedStore`, fired
      after `persistHLC` and after local commits). `_awaitReadAfter` resolves on the first notification
      that reaches the watermark and still honours the timeout and abort signal.
- **Test:** `readafter.test.ts` still passes. Add a test that the read is served within one tick of the
  replica catching up, not on the next 50 ms boundary.

**Release:** 2.5.0 (minor).

---

## Phase 6 — Typed collections (optional)

**Goal:** `db.todos.put(...)` is currently typed as `Record<string, unknown>`, because `TypedDB` maps
every collection to an untyped `CollectionProxy` (`db.ts:126`, and `TxCollectionProxy` at `db.ts:78`).

- [ ] Infer the document type from the schema:
      `[K in keyof C]: CollectionProxy<C[K] extends CollectionSchema<infer T> ? T : Record<string, unknown>>`.
- [ ] Add a `defineCollection<T>()(schema)` helper so users can attach a document type without a custom
      conflict resolver, e.g. `todos: defineCollection<Todo>()({ indexes: ['status'] })`.
- [ ] Type `QueryOptions.where`, `ScanOptions` and `TxCollectionProxy.put` with the same `T`.
- **Test:** type-level tests (`expectTypeOf`) for the typed and untyped forms. Existing untyped usage must
  still compile.

**Release:** 2.6.0 (minor; purely additive typing).

---

## Phase 7 — Deferred RPC features

**Goal:** close the two gaps left from the consumer-client work. Each needs a short design note in
`docs/rpc-protocol.md` **before** coding. Treat them as sub-phases 7a and 7b.

### 7a — Replicated (cross-node) idempotency store
- Today `IdempotencyCache` is in-memory per Normal Client, so a write replayed on a different node after
  failover can apply twice. That is why writes default to no replay (`consumer.ts:84`).
- [ ] Extract an `IdempotencyStore` interface (`get(key)`, `putPending`, `putResult`, `delete`) from
      `IdempotencyCache`. The in-memory version becomes the default implementation.
- [ ] Implement a replicated store backed by a system collection (e.g. `_rpc_idem`, keyed by
      `identity:method:key`, holding the result and an expiry), pruned on the maintenance cycle.
- [ ] Decide and document the guarantee. It is still best-effort: two nodes can race before gossip
      converges. Document what replay is safe for.
- [ ] Once shipped, consider making `{ idempotent: true }` the default for writes that carry an
      idempotency key.

### 7b — Fastest-candidate selection
- [ ] Add `ConsumerClientConfig.selection?: 'round-robin' | 'fastest'` (default `'round-robin'`).
- [ ] Design choice (decide first): measure RTT with the existing `ping`/`pong` frames after connecting
      (needs a channel per candidate, which is expensive), **or** have the signaling server include
      latency/load hints in `server-list` (cheap, but needs the external `client-db-sync` repo). The
      second option is recommended.
- [ ] Also wire the client heartbeat (`ping`) to detect half-open channels sooner.

**Release:** 2.7.0 (7a) / 2.8.0 (7b), minor each.

---

## Phase 8 — Next major (3.0.0): remove deprecations

**Goal:** collect every breaking change into one release.

- [ ] Remove `SnapshotChunkMessage`, `SnapshotResponseMessage`, `exportSnapshot`, `importSnapshot`,
      `handleSnapshotStreamStart` and the free snapshot functions (Phases 2.4 and 4.4).
- [ ] Remove the positional `GossipSync` constructor (Phase 3.5) and the `_querySnapshotBatch` alias (Phase 4.1).
- [ ] Consider making `ISyncStorage` the required contract for adapters used with `sync`.
- [ ] Write a **Migration guide 2.x → 3.0** section in the README and CHANGELOG.

**Release:** 3.0.0.

---

## Out of scope / notes

- `createDB` dedupes by `name` and returns the cached promise even if a second call passes a different
  config (`db.ts:190`). That is documented behaviour (README *Caveats*), so it isn't treated as a bug here.
  Revisit only if users report confusion (e.g. a dev-mode warning when configs differ).
- Signaling-server changes (Phase 7b option 2) live in the external `client-db-sync` repo.
