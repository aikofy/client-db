# Examples

Illustrative usage of `@aikofy/client-db` 2.0.0. These are browser-targeted (IndexedDB + WebRTC),
so bundle them with Vite / esbuild / your tool of choice and run in a browser.

| File | What it shows |
|------|---------------|
| [`normal-client.ts`](./normal-client.ts) | A **Normal Client** (full replica) that also serves Consumers: an `RpcRouter` with read/write/stream handlers, token verification, and an audit/metrics hook. |
| [`consumer-client.ts`](./consumer-client.ts) | A **Consumer Client** (`@aikofy/client-db/consumer`) that authenticates and calls the Normal Client's handlers via `invoke()` and `stream()`. |

## How the pieces fit

1. One or more **Normal Clients** open the DB with `sync` (peer-to-peer replication) and
   `rpc: { router }` (so they answer Consumer calls). They are interchangeable — a Consumer can be
   served by any of them, which is what makes failover work.
2. A **Consumer Client** connects via the signaling server, is handed a list of healthy Normal
   Clients, picks one (round-robin), authenticates with a token, and calls handlers.
3. A Consumer can only ever receive what a handler returns — it never replicates the database.

## Prerequisites

- A **role-aware signaling server** (see [`../docs/signaling-protocol.md`](../docs/signaling-protocol.md)).
  The companion `@aikofy/client-db-sync` is the signaling server for this library; it must be
  updated to broker Consumer connections (verify the consumer token, return a `server-list`, never
  gossip consumers).
- An **IdP / backend** that mints short-lived JWS access tokens (ES256/RS256) for Consumers. The
  Normal Client verifies them locally with `createTokenVerifier` — no per-request callout.

## Notes

- Tokens are verified locally on every Normal Client, so any of them can authenticate any Consumer
  offline — this is what preserves failover.
- Writes replicate to other Normal Clients through the normal gossip/CRDT pipeline; there is no
  special path. Reads carry the last write's HLC watermark so a failed-over replica serves
  read-your-writes once it has caught up.
