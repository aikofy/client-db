# Changelog

All notable changes to `@aikofy/client-db` are documented here. This project adheres to
[Semantic Versioning](https://semver.org/).

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
