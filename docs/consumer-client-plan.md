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

### Phase 1 — Transport isolation + signaling roles
**Goal:** a Consumer can open an `'rpc'` data channel to a Normal Client that is **never**
gossiped to. This is the safety foundation; build it first.
**Changes:**
- `webrtc-transport.ts`: on `ondatachannel`, branch on `channel.label`. `'sync'` → existing
  gossip wiring; `'rpc'` → new `onConsumerConnected(peerId, channel)` callback. Do **not**
  add `'rpc'` peers to `peerStates` used by `peers()`/gossip, or keep a separate map.
- Extract offer/answer/ICE plumbing into reusable helpers so the Consumer side can connect
  to a single Normal Client without the room/mesh logic.
- `db.ts`: gossip starts only on a `'sync'` channel open; `'rpc'` channels route to `RpcServer`.
- Signaling (external) + client `register`: add `role`. Consumer receives a candidate
  `server-list` instead of a gossip `peer-list`.
**Tests:** fake signaling + fake `RTCPeerConnection`; assert an `'rpc'` peer never appears in
`peers()`, never receives `broadcastDoc`, never triggers a snapshot.
**Risk:** transport refactor touches the battle-tested handshake — keep the mesh path
behavior-identical; cover with existing gossip tests + new isolation tests.

### Phase 2 — RPC core + handler/router API (reads first)
**Goal:** define and call unary read handlers end-to-end (identity stubbed for now).
**Changes:**
- `protocol.ts`, `errors.ts`: envelope + status codes + `RpcError`.
- `router.ts`: `router.read(name, { scopes, input, handler })` (and `.write`, `.stream`
  signatures stubbed). Handler signature `(ctx, params) => result`.
- `server.ts`: dispatch over an `'rpc'` channel, correlation IDs, timeouts, error mapping.
  Middleware pipeline scaffold (validate only this phase; auth stubbed to a trusted identity).
- `ctx` shape: `{ consumer, db, hlc(), idempotent(), signal, log }`. `db` = existing adapter.
- `db.ts`: accept `config.rpc = { router, ... }`; mount `RpcServer` when present.
**Tests:** register a read handler, invoke it via the fake transport, assert filtered result,
validation failure → `INVALID_ARGUMENT`, handler throw → mapped status, timeout fires.
**Risk:** low — additive, gated behind `'rpc'` isolation from Phase 1.

### Phase 3 — Consumer SDK + slim build
**Goal:** `ConsumerClient` that connects, calls `invoke()`, and ships small.
**Changes:**
- `client.ts`: `RpcClient` — correlation, promises, deadlines, cancel.
- `consumer.ts`: `ConsumerClient` — signaling connect (role=consumer), pick a server
  (**round-robin** default, or **fastest-by-ping**), WebRTC connect (offerer, `'rpc'`
  channel), expose `invoke(method, params)`. See §7 for the selection model.
- `tsup.config.ts` + `package.json` exports: second entry, verify bundle excludes heavy modules.
**Tests:** end-to-end over fake transport (consumer ⇄ normal); bundle-size assertion / import
graph check that storage/gossip are not pulled into the consumer entry.
**Risk:** medium — ensuring tree-shaking actually drops the heavy modules; may need import hygiene.

### Phase 4 — Auth & authz
**Goal:** real authentication + scope-based authorization.
**Changes:**
- `auth.ts`: verify JWS via WebCrypto (`crypto.subtle.verify`), check `exp`, populate
  `ctx.consumer = { id, scopes, claims }`. Gossiped revocation set lookup.
- `middleware.ts`: `authenticate` (on connect) + `authorize(scopes)` (per call).
- Consumer side: `auth: { getToken }`, send token in the `auth` control frame on channel open;
  auto-refresh.
- Signaling (external): verify consumer token at `register` (defense in depth / DoS guard).
**Tests:** valid/expired/forged/revoked tokens; missing-scope → `PERMISSION_DENIED`;
unauthenticated channel rejected.
**Risk:** medium — crypto correctness; pin algorithms, reject `alg:none`.

### Phase 5 — Write path: idempotency + watermark
**Goal:** safe write handlers that replicate via existing gossip.
**Changes:**
- `router.write`: handler writes via `ctx.db` (existing adapter `put`/`delete`), which already
  feeds `onChangeEntry` → `gossip.broadcastDoc`. No special replication path.
- `idempotency.ts`: `ctx.idempotent(key, fn)` dedupe; key from request envelope. Dedupe store
  ideally replicated so it survives failover.
- Response carries `at: hlc()` watermark.
**Tests:** write replicates to a second Normal Client; replayed idempotency key runs once;
identity fields (e.g. ownerId) taken from token, not input.
**Risk:** medium — dedupe correctness under failover; define dedupe-store TTL.

### Phase 6 — Streaming responses
**Goal:** large/long results without OOM.
**Changes:**
- `router.stream`: async-generator handler; framework emits `stream` frames per `yield`,
  then `end`. Reuse `sendAsync()` backpressure (16 MB high-water) from `webrtc-transport.ts`.
- Consumer side: `consumer.stream(method, params)` → async iterable; cancel propagates.
**Tests:** stream N batches with backpressure simulated; cancel mid-stream stops the producer;
timeout on stalled stream.
**Risk:** low–medium — mirrors existing `snapshot.ts` streaming.

### Phase 7 — Failover, sticky sessions, read-your-writes
**Goal:** resilience across Normal Client churn.
**Changes:**
- Consumer: warm candidate list; on disconnect/`UNAVAILABLE`, reconnect to the next healthy
  candidate (next in round-robin order, or re-ping for fastest), re-auth, replay **idempotent**
  in-flight calls (non-idempotent → surface error). See §7.
- Sticky session by default. On failover, pass last HLC watermark; new Normal Client waits
  until its replica catches up (or returns retryable `UNAVAILABLE`).
**Tests:** kill serving Normal Client mid-session → transparent recovery; read-your-writes
holds after failover; non-idempotent call not double-run.
**Risk:** medium — replay semantics; lean on idempotency keys + retryable flags.

### Phase 8 — Hardening
**Goal:** production posture.
**Changes:**
- `middleware.ts`: per-consumer token-bucket **rate limiting**; per-request **payload caps**.
- **Audit log** of writes keyed by consumer identity (as a replicated collection).
- **Metrics/logging** per method (latency, errors, caller).
- **Capability negotiation**: server advertises supported methods/versions in `auth-ok`;
  versioned method names (`orders.book@v2`).
**Tests:** rate-limit trips; oversize payload rejected; audit entries written; old consumer
gets a clear error for an unsupported method.
**Risk:** low.

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
