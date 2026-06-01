# Consumer Client over WebRTC RPC — Implementation Plan

> Status: **proposed** (no code yet). This document is the agreed design + a phased
> build plan. Each phase is independently shippable and testable.

## 1. Goal

Add a second client type so the system supports two roles:

- **Normal Client** — today's client. Holds a full replica, gossips with other Normal
  Clients, and **additionally** acts as an RPC *server*: it exposes named read/write
  functions (handlers) that operate on its local replica. Think "backend API server."
- **Consumer Client** — new, thin client. Holds **no data**, never gossips, never receives
  a snapshot. It authenticates and *calls* the Normal Client's functions over WebRTC.
  Think "frontend calling a backend," but the wire is a WebRTC data channel instead of HTTP.

```
  Normal ◀──gossip──▶ Normal ◀──gossip──▶ Normal      (full replicas, share everything)
     ▲                   ▲                   ▲
     │  RPC ('rpc' chan) │                   │
  Consumer            Consumer            Consumer      (no DB — only call functions)
```

## 2. Inviolable rules (the safety contract)

1. A Consumer is **never** added to the gossip peer set and is **never** sent a snapshot.
   The only data that reaches a Consumer is a **handler's return value**.
2. The **handler body is the access-control boundary** — it reads the full local replica
   (`ctx.db`) but returns only what the authenticated caller is allowed to see.
3. A Consumer's identity comes from a **verified token** (`ctx.consumer`), never from
   client-supplied fields.

## 3. Scope: this repo vs. external

| Work | Where |
|---|---|
| RPC protocol, server, router/handler API, consumer SDK, transport changes, slim build | **This repo** |
| Signaling server: role-aware `register`, consumer token check, `server-list` to consumers | **External signaling server repo** — this plan provides the protocol spec only |
| Token issuance / identity provider (minting access tokens) | **External** — we only *verify* tokens here |

## 4. Key decisions

Recommended defaults (locked unless the three open questions below change them):

| Decision | Default |
|---|---|
| Channel separation | WebRTC data-channel **label**: `'sync'` → gossip, `'rpc'` → RPC |
| Token issuance | **Existing IdP / backend** mints short-lived asymmetric-signed JWS (ES256/RS256). We only *verify* here |
| Token verify | **Locally** via WebCrypto on each Normal Client (no per-request callout) + gossiped revocation set |
| Auth timing | Once on channel open (session bound to connection); token self-verifiable so failover needs no shared session store |
| Input validation | **Pluggable** — accept any `{ parse(input) }` validator (zod-compatible); no forced dependency |
| Write ack | "applied locally, will replicate" + HLC watermark; opt-in N-peer durability later |
| Write safety | Idempotency keys, mandatory |
| Session | Sticky by default; HLC-watermark replay for read-your-writes across failover |
| Load balancing | **Round-robin** across healthy candidates (default) or **ping/pong → fastest**; **failover** to next healthy client on mid-call failure |
| Consumer packaging | Separate slim entry `@aikofy/client-db/consumer` (no storage/CRDT/gossip) |

### Resolved decisions (confirmed)
1. **Auth authority:** ✅ Use the **existing IdP / backend** to mint tokens. This repo only
   verifies them (WebCrypto). No token-issuing code here.
2. **Session stickiness:** ✅ **Sticky sessions** — a Consumer pins to one Normal Client per
   session; failover reconnects only on failure. Read-your-writes comes for free; the
   HLC-watermark catch-up (Phase 7) covers the failover edge.
3. **Durability:** ✅ **Applied-locally ack** + HLC watermark ("will replicate eventually").
   No confirmed-propagation mode for now (can be added opt-in later if a critical write needs it).

## 5. New module layout

```
src/
  rpc/
    protocol.ts      # RpcMessage union, status codes, envelope types
    errors.ts        # RpcError + status code enum
    router.ts        # router.read / .write / .stream — the handler-authoring API
    server.ts        # RpcServer: dispatch + middleware pipeline (runs on Normal Client)
    middleware.ts    # authenticate, authorize(scopes), rateLimit, validate
    auth.ts          # WebCrypto token verification + session/identity
    idempotency.ts   # run-once dedupe for writes
    client.ts        # RpcClient stub (invoke / stream / deadlines)
  consumer.ts        # ConsumerClient — slim entry point (NEW build target)
  sync/
    webrtc-transport.ts  # CHANGED: channel-label routing, role tagging, handshake reuse
  db.ts                  # CHANGED: wire RpcServer; branch consumer vs normal connections
```

Build/exports changes:
- `tsup.config.ts`: `entry: ['src/index.ts', 'src/consumer.ts']`.
- `package.json` `exports`: add `"./consumer"` subpath; ensure consumer entry tree-shakes
  out `storage/`, `core/conflict`, `sync/gossip`, `sync/snapshot`.

---

## 6. Phases

### Phase 0 — Protocol spec & decisions (paper only) — ✅ DONE
**Goal:** lock the wire contracts before writing code.
**Deliverables:**
- ✅ [`docs/rpc-protocol.md`](./rpc-protocol.md): the `RpcMessage` envelope
  (auth/req/res/stream/err/cancel/ping-pong), status codes + retryable/failover semantics,
  correlation-id rules, deadline/cancel, idempotency, read-your-writes, versioning, limits.
- ✅ [`docs/signaling-protocol.md`](./signaling-protocol.md): role-aware `register`, consumer
  token verification, rotated `server-list`, consumer-never-gossiped rule, heartbeat/load,
  offer/answer/ICE relay with `fromRole`, backward compat. (For the external server team.)
- ✅ Three open questions in §4 resolved (existing IdP, sticky sessions, applied-locally ack).
**Acceptance:** specs reviewed & approved. No code.

### Phase 1 — Transport isolation + signaling roles — ✅ DONE
**Goal:** a Consumer can open an `'rpc'` data channel to a Normal Client that is **never**
gossiped to. This is the safety foundation; build it first.
**Delivered:**
- ✅ `webrtc-transport.ts`: inbound consumer connections live in a separate `consumerStates`
  map; `ondatachannel` accepts **only** an `'rpc'`-labelled channel from a consumer (a `'sync'`
  channel is refused). `peers()`/`broadcast()`/`onPeerConnected` remain gossip-only. New
  `onConsumer{Connected,Disconnected,Message}`, `sendToConsumer()`, `consumers()`. `_wireICE`
  generalized (shared by gossip + consumer paths). Registers as `role:'normal'` + `serveConsumers`.
- ✅ `core/types.ts` + `db.ts`: `SyncConfig.serveConsumers` plumbed through to the transport.
  (RPC server wiring of the consumer callbacks is deferred to Phase 2, as planned.)
- ✅ `src/test-utils/fake-webrtc.ts`: fake `RTCPeerConnection`/`RTCDataChannel`/`WebSocket` +
  `FakeSignalingHub` (relays offer/answer/ICE, stamps `fromRole`, `peer-list` vs `server-list`)
  + `FakeConsumer` driver + `installFakeWebRTC`/`waitFor`. Not shipped (only `index.ts` is built).
- ✅ `src/sync/webrtc-transport.test.ts`: consumer isolation (not a gossip peer, no broadcast,
  bidirectional rpc messages, onConsumer\* fire), `'sync'`-from-consumer refused, consumer removed
  on `disconnect()`, and a two-normal **mesh sanity** test guarding the `_wireICE` refactor.
- **Note:** the signaling-server changes are external (see `signaling-protocol.md`); the
  client side here is exercised against the fake hub. Live e2e needs the server team's update.
**Result:** 81/81 tests pass (4 new), `tsc --noEmit` clean, `tsup` build succeeds.

### Phase 2 — RPC core + handler/router API (reads first) — ✅ DONE
**Goal:** define and call unary read handlers end-to-end (identity stubbed for now).
**Delivered:**
- ✅ `src/rpc/protocol.ts`: full frame union (auth/req/res/err/stream/cancel/ping-pong), 12
  status codes, `RpcLimits`, `PROTOCOL_VERSION`. `errors.ts`: `RpcError` + per-status
  `retryable` defaults. `context.ts`: `RpcContext` + `ConsumerIdentity`.
- ✅ `router.ts`: `RpcRouter.read/write/stream(id, { scopes, input, version, handler })`,
  pluggable `{ parse }` validator, exact-match dispatch, `catalog()` for `auth-ok`.
- ✅ `server.ts`: per-consumer sessions, `auth`→`auth-ok` handshake (stub verifier; real one in
  Phase 4), unary `req`→`res` dispatch, correlation ids, per-request deadline + `cancel`
  (AbortSignal), validate step, and error→status mapping (RpcError→its status, else INTERNAL).
  Stream dispatch returns `UNAVAILABLE` until Phase 6.
- ✅ `db.ts`: `createDB({ …, rpc: { router } })` mounts an `RpcServer` on the transport's
  isolated `onConsumer*` callbacks. Exported `RpcRouter/RpcServer/RpcError`/types from `index.ts`.
- ✅ `src/rpc/server.test.ts`: auth→catalog, identity-filtered read, req-before-auth→
  UNAUTHENTICATED, unknown→NOT_FOUND, validation→INVALID_ARGUMENT, RpcError→mapped status,
  plain throw→INTERNAL, deadline→DEADLINE_EXCEEDED, ping→pong.
**Result:** 90/90 tests pass (9 new), `tsc --noEmit` clean, `tsup` build succeeds.
**Deferred (as planned):** real token verify/scope enforcement → Phase 4; idempotency +
write watermark → Phase 5; streaming dispatch → Phase 6.

### Phase 3 — Consumer SDK + slim build — ✅ DONE
**Goal:** `ConsumerClient` that connects, calls `invoke()`, and ships small.
**Delivered:**
- ✅ `src/rpc/client.ts`: `RpcClient` — transport-agnostic; auth handshake, `invoke()` with
  correlation ids, deadlines (+grace), `AbortSignal` cancel→`cancel` frame, `reset()` on close.
- ✅ `src/consumer.ts`: `ConsumerClient` — signaling connect (`register role=consumer`+token),
  **round-robin** server pick from `server-list`, RTCPeerConnection offerer with `'rpc'` channel
  + ICE buffering, wires `RpcClient`. `connect()` / `invoke()` (auto-connects) / `close()`.
  Independent of WebRTCTransport/storage/gossip. (`fastest-by-ping` + failover → Phase 7.)
- ✅ `tsup.config.ts` + `package.json`: second entry `src/consumer.ts` → `@aikofy/client-db/consumer`.
- ✅ Tests: `consumer.test.ts` (real `ConsumerClient` ⇄ real `WebRTCTransport`+`RpcServer` via the
  fake hub — connect/auth/invoke read, implicit-connect, RpcError mapping, typed params, cancel)
  and `consumer.imports.test.ts` (static import-graph guard: consumer reaches **only**
  `consumer.ts`, `core/types.ts`, `rpc/{client,errors,protocol}.ts` — no storage/gossip/idb).
- ✅ README: new "Consumer Clients (RPC)" section (handler + ConsumerClient usage, status note)
  and updated Project Structure.
**Result:** 98/98 tests pass (8 new). Consumer bundle **9.75 KB** vs main **63.8 KB**.
`tsc` clean, dual-entry `tsup` build succeeds.

### Phase 4 — Auth & authz — ✅ DONE
**Goal:** real authentication + scope-based authorization.
**Delivered:**
- ✅ `src/rpc/auth.ts`: `createTokenVerifier({ jwks, algorithms, issuer, audience,
  clockToleranceSec, isRevoked, toIdentity })` → verifies JWS via WebCrypto (`crypto.subtle.verify`,
  ES256/RS256), **rejects `alg:none`** + disallowed algs, checks `exp`/`nbf`/`iss`/`aud`,
  revocation hook, maps claims→`ConsumerIdentity` (`sub`, `scope`/`scopes`). `Jwk` type.
- ✅ `server.ts`: **scope authorization** (handler `scopes` ⊆ identity scopes else
  `PERMISSION_DENIED`); **token-expiry** tracking (`session.expiresAt` from `claims.exp`) →
  expired req returns `UNAUTHENTICATED` and forces re-auth (spec §8).
- ✅ `client.ts`: **auto-reauth** — on `UNAUTHENTICATED`, drop the session, re-fetch the token,
  retry once. (`authenticate` already sends the token on channel open.)
- ✅ `db.ts`: `RpcConfig.verifyToken` passed to `RpcServer`. Exported `createTokenVerifier`/types.
- ✅ Tests: `auth.test.ts` (valid / expired / nbf / forged / tampered / `alg:none` / disallowed alg
  / issuer+audience / revoked / malformed), `client.test.ts` (resolve, err-map, cancel, auto-reauth
  ×1 only, reset), `auth.e2e.test.ts` (scope grant + denial, identity-from-token, invalid-token).
  `test-utils/jwt.ts` ES256 signer.
- ✅ README auth/scopes updated. **Note:** signaling-side token check is external
  (`signaling-protocol.md` §6). Revocation set wiring to gossip is left as the `isRevoked` hook.
**Result:** 118/118 tests pass (20 new). `tsc` clean; consumer bundle still **10.35 KB** (the
verifier stays out of it — import guard confirms).

### Phase 5 — Write path: idempotency + watermark — ✅ DONE
**Goal:** safe write handlers that replicate via existing gossip.
**Delivered:**
- ✅ Write handlers write via `ctx.db` (the same adapter `createDB` wired `onChangeEntry` on),
  so a write automatically feeds `gossip.broadcastDoc` → replicates. No special path.
- ✅ `src/rpc/idempotency.ts`: `IdempotencyCache` — run-once with TTL, in-flight dedupe, and
  failed-calls-not-cached (retryable). `ctx.idempotent(key, fn)` keys on
  `${identity.id}:${method}:${key}`; no key → runs directly. (In-memory/per-node; replicated
  cross-node dedupe deferred — cache is injectable.)
- ✅ Write `res` carries the post-write `hlc` watermark; `RpcClient` captures `lastHlc` and
  **auto-attaches a stable idempotency key** for write methods (per `auth-ok` catalog kind) so a
  retry/failover replay is deduped, not double-applied.
- ✅ `db.ts` `RpcConfig.idempotencyTtlMs`. Exported `IdempotencyCache`.
- ✅ Tests: `idempotency.test.ts` (once / concurrent / independent-keys / failed-retry / TTL);
  `write.test.ts` (real `IndexedDBAdapter`): persists, **fires `onChangeEntry` (replication
  trigger)**, **ownerId from token not client input**, res carries `hlc`, idempotent replay runs
  handler once (one doc). README write examples added.
**Result:** 125/125 tests pass (7 new). `tsc` clean; consumer bundle **10.92 KB** (cache is
server-side). **Deferred:** read-your-writes `readAfter` enforcement → Phase 7; replicated dedupe
store → follow-up.

### Phase 6 — Streaming responses — ✅ DONE
**Goal:** large/long results without OOM.
**Delivered:**
- ✅ `src/sync/backpressure.ts`: extracted `drainIfNeeded(channel, timeoutMs?)` + `BACKPRESSURE_*`
  constants (lifted from `sendAsync`, behavior-identical). `webrtc-transport.ts` `sendAsync` now
  uses it, and new `sendToConsumerAsync(consumerId, data)` gives backpressure-aware consumer sends.
- ✅ `server.ts`: `_handleStream`/`_runStream` — iterate an async-generator handler, emit
  `stream-start` → `stream-chunk{seq}` → `stream-end`; authorize + validate; deadline + `cancel`
  via `AbortSignal` (breaking the for-await runs the generator's `finally`); errors → `err` frame;
  chunks sent via `config.sendAsync` (backpressure). `RpcServerConfig.sendAsync`.
- ✅ `client.ts`: `StreamCall` (queue + async iterator; `terminated` distinguishes ended/errored
  from caller-break) + `streams` map; `stream()` async generator (cancel on `AbortSignal` or early
  break, errors propagate); handles `stream-chunk`/`stream-end`/err-for-stream; `reset()` fails
  streams. `consumer.ts` `stream()`. `db.ts` wires `sendAsync`.
- ✅ Tests: `backpressure.test.ts` (immediate / drain-event / close / timeout / closed-race);
  `stream.test.ts` e2e (ordered chunks→complete, mid-stream error after earlier chunks, early-break
  cancels the producer via the handler `finally`, AbortSignal→CANCELLED). README streaming examples.
**Result:** 134/134 tests pass (9 new). `tsc` clean; consumer bundle **13.9 KB**. Gossip
`sendAsync` path unchanged (mesh + gossip tests still green).

### Phase 7 — Failover, sticky sessions, read-your-writes — ✅ DONE
**Goal:** resilience across Normal Client churn.
**Delivered:**
- ✅ `consumer.ts` reworked into a failover-capable connection state machine: persistent signaling,
  round-robin candidate advance on reconnect (`everConnected` guard), reconnect + re-auth, and an
  `invoke` retry loop bounded by candidate count. On a dropped channel (`_onChannelDown`) or a
  server-sent retryable error, idempotent calls replay on the next candidate.
- ✅ Replay policy: **reads replay** by default; **writes do not** (cross-node duplicate risk until
  the dedupe store is replicated) — surface `UNAVAILABLE`; `{ idempotent: true }` opts a write in.
- ✅ Read-your-writes: `RpcClient` auto-attaches `readAfter = lastHlc` to reads. `server.ts`
  `_awaitReadAfter` waits (bounded `readAfterTimeoutMs`) for the local replica's HLC to reach
  `readAfter` before serving, else retryable `UNAVAILABLE` (unary + stream paths). `InvokeOptions`
  gains `idempotent` + `readAfter`.
- ✅ Tests: `failover.test.ts` (read recovers on B after killing A; write surfaces error / not
  double-run; write `{idempotent:true}` replays on B); `readafter.test.ts` (UNAVAILABLE on timeout,
  serves once caught up, immediate when ahead); `client.test.ts` (+1: writes keyed, reads carry
  `readAfter=lastHlc`). README failover/read-your-writes section.
**Result:** 141/141 tests pass (7 new). `tsc` clean; consumer bundle **16.95 KB**. Existing
single-node consumer/auth/stream tests unaffected by the rework.
**Deferred:** fastest-by-ping selection + heartbeat liveness (round-robin is the default);
replicated cross-node idempotency dedupe store.

### Phase 8 — Hardening — ✅ DONE
**Goal:** production posture.
**Delivered:**
- ✅ `src/rpc/middleware.ts`: `TokenBucket` (per-consumer token-bucket rate limiter, injectable
  `now`), `byteSize` (payload sizing), `CallRecord` (observability record).
- ✅ `server.ts`: per-session rate limit → `RESOURCE_EXHAUSTED`; payload cap (`limits.maxPayloadBytes`)
  → `INVALID_ARGUMENT`; `onCall(record)` observability hook fired once per call (unary + stream)
  with `{consumerId, identityId, method, kind, status, durationMs}` — metrics + audit (filter
  `kind==='write'`); protocol-version check at auth → `FAILED_PRECONDITION` for too-new clients.
- ✅ `client.ts`: capability negotiation — fail fast with `NOT_FOUND` (no round-trip) for a method
  not in the `auth-ok` catalog (unary + stream).
- ✅ `db.ts` `RpcConfig.onCall`; exported `TokenBucket`/`byteSize`/`CallRecord`. (Method versioning
  via `name@vN` is already supported by the router; the catalog advertises `{name,kind,version}`.)
- ✅ Tests: `hardening.test.ts` (TokenBucket capacity/refill; rate-limit trips; oversize payload
  rejected; `onCall` record emitted with status/duration; protocol-version mismatch → auth-err);
  `client.test.ts` (+1: unsupported method → client-side `NOT_FOUND`, zero req frames sent).
  README hardening/observability/audit section.
**Result:** 147/147 tests pass (6 new). `tsc` clean; consumer bundle **17.34 KB**.
**Note:** audit persistence is via the `onCall` hook (write to a replicated collection in-app);
the server doesn't hardcode a collection.

### Phase 9 — Docs, examples, release
**Goal:** usable + shipped.
**Changes:**
- README sections: Normal Client handler authoring, Consumer SDK usage, auth setup,
  consistency/failover notes.
- Example app (Normal + Consumer).
- Changelog, version bump, publish (`@aikofy/client-db` + `/consumer` subpath).
**Risk:** low.

---

## 7. Load balancing

All Normal Clients are full replicas, so **any can serve any call** — balancing is pure
capacity/latency distribution with no data-locality or sharding constraints. Selection happens
at **session connect** (sticky sessions) and again **on failover**; once a session is bound,
traffic is direct P2P and is not re-balanced per request.

**Candidate list.** On `register`, the signaling server returns the Consumer a list of
**healthy, opted-in** Normal Clients. To keep round-robin even across independent Consumers, the
director **rotates the head of the list** it hands out — so they don't all start on the same node.

**Selection strategies (configurable on the Consumer):**
1. **Round-robin (default).** The Consumer cycles through the candidate list. Combined with
   director-side rotation, this spreads sessions evenly with **no central load tracking**.
2. **Fastest-by-ping.** The Consumer `ping`s the top N candidates and picks the lowest-RTT
   `pong`. Steers away from slow/overloaded/distant nodes; good for latency-sensitive use. (Any
   mild herd toward one fast node is self-correcting — RTT rises as it loads up.)

**Failover on mid-call failure (the core requirement).** If the serving Normal Client goes
offline or returns `UNAVAILABLE` during a call:
- The Consumer advances to the **next healthy candidate** (round-robin order, or re-pings for
  fastest), reconnects, and re-auths with the same token (self-verifiable → no shared session
  state to migrate).
- In-flight **idempotent** calls are **replayed** safely (idempotency keys prevent
  double-applied writes); non-idempotent, non-retryable calls surface an error instead of
  silently re-running.
- The last **HLC watermark** is replayed so read-your-writes survives the hop to a new replica.

**Health.** The `ping/pong` heartbeat does double duty: (a) the latency probe for
fastest-selection and (b) liveness — a silent peer drops out of the candidate list and triggers
failover for its sessions.

**Admission backstop.** A Normal Client at capacity rejects new sessions with `UNAVAILABLE` at
auth time, so the Consumer simply moves to the next candidate — balancing stays correct even
when the director's load view is stale.

**Edge cases.** No healthy candidate → Consumer retries with backoff and surfaces "no server
available" (no local cache, by design). Single Normal Client → it serves until its admission
limit, then sheds.

**Maps to phases:** candidate list + director rotation → **Phase 1** (signaling); round-robin /
ping-fastest selection → **Phase 3**; failover replay + watermark + admission shedding →
**Phase 7**; opt-in/capacity + health metrics → **Phase 8**. Load balancing is not its own phase
— it threads through these.

---

## 8. Testing strategy

- **Fake signaling + fake `RTCPeerConnection`/`RTCDataChannel`** harness (new) so RPC + transport
  are testable in vitest without a browser, alongside existing `fake-indexeddb`.
- Isolation invariants (Phase 1) are regression-guarded: a consumer connection must never reach
  gossip/snapshot paths.
- Crypto tests use known-answer vectors; reject `alg:none` and algorithm confusion.
- End-to-end "two Normal Clients + one Consumer" scenario reused from Phase 5 onward.

## 9. Dependency notes
- **No new runtime deps required.** Token verify uses WebCrypto (`crypto.subtle`); request IDs
  reuse `uuid`. Validation stays pluggable (zod optional, user-supplied).

## 10. Suggested sequencing
Phases are ordered to ship safety first (1), then the read path (2–3), then auth (4) before any
writes (5), then streaming (6), resilience (7), hardening (8), release (9). Phases 1–4 deliver a
working authenticated read-only Consumer; 5+ add writes and robustness.
