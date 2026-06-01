# Signaling Protocol Spec (role-aware)

> Phase 0 deliverable, written for the **external signaling-server** team. This server is a
> separate codebase from `@aikofy/client-db`; this document specifies the protocol changes it
> must implement to support Consumer Clients. Status: **proposed**.
>
> The signaling server only brokers the WebRTC handshake and acts as a **load-balancing
> director**. It never sees database contents or RPC payloads — those travel directly P2P over
> the data channel after connection.

## 1. Baseline (what exists today)

Clients connect over WebSocket. The URL carries query params `room` and `nodeId`. The current
message set (see `src/sync/webrtc-transport.ts`):

| Message | Direction | Fields | Purpose |
|---|---|---|---|
| `register` | client→server | `type`, `nodeId` | Announce presence in the room |
| `peer-list` | server→client | `type`, `peers: string[]` | Other members; recipient initiates connections to new ones |
| `offer` | relayed | `type`, `to`, `from`, `sdp` | SDP offer, routed to `to` |
| `answer` | relayed | `type`, `to`, `from`, `sdp` | SDP answer, routed to `to` |
| `ice-candidate` | relayed | `type`, `to`, `from`, `candidate` | ICE candidate, routed to `to` |

The room is the DB name; all members are full-replica **Normal Clients** that gossip with each
other. The server relays `offer`/`answer`/`ice-candidate` by `to`.

## 2. What changes

Add a **role** so the server can keep Consumers out of the gossip mesh and act as a director.

```
Normal Client:   register{ role:"normal", serveConsumers, capacity }   → peer-list (gossip peers)
Consumer Client: register{ role:"consumer", token }                    → server-list (where to call)
```

**Backward compatibility:** a `register` with no `role` is treated as `role:"normal"`, and a
normal with no `serveConsumers` field defaults to `serveConsumers:true`. Existing clients keep
working unchanged. (Consumers can't appear until clients are updated anyway.)

## 3. `register` (extended)

```jsonc
// Normal Client
{ "type":"register", "role":"normal", "nodeId":"…",
  "serveConsumers": true,          // opt-in to serving Consumers (default true)
  "capacity": 50,                  // soft max concurrent Consumer sessions (optional)
  "protocolVersion": 1 }

// Consumer Client
{ "type":"register", "role":"consumer", "nodeId":"…",
  "token": "<JWS>",                // REQUIRED — verified by the server (§6)
  "protocolVersion": 1 }
```

## 4. Consumer registration flow

1. Consumer opens the WS and sends `register{ role:"consumer", token }`.
2. Server **verifies the token** (§6). On failure → send `auth-err` and close. This keeps
   unauthenticated load off the Normal Clients (DoS guard) and is defense-in-depth (the Normal
   Client re-verifies on the data channel too).
3. On success → send `server-list` (§5). The Consumer never receives `peer-list`.
4. The Consumer is **never** added to any Normal Client's `peer-list`. Normal Clients must never
   attempt to gossip with a Consumer. (Authoritative isolation is also enforced client-side via
   the `'rpc'` vs `'sync'` data-channel label, but the server must not advertise consumers as
   gossip peers in the first place.)

```jsonc
// server→consumer
{ "type":"server-list",
  "servers": ["nodeA","nodeC","nodeB"]   // healthy, opted-in Normal Clients — see rotation §7
}
```

## 5. Connection setup (Consumer ⇄ Normal)

- The **Consumer is always the offerer** and opens an `'rpc'`-labelled data channel. (This pairing
  never hits the glare/tie-break path.)
- Consumer picks a target from `servers` and sends `offer{ to:<normalNodeId>, from:<consumerId>,
  sdp, fromRole:"consumer" }`. The server routes it to `to`.
- Normal answers; `answer`/`ice-candidate` relayed both ways as today.
- The server SHOULD stamp relayed offers with `fromRole` so the Normal Client can apply
  consumer-specific handling early (the data-channel label remains the authoritative switch).
- The server **MUST NOT** relay `offer`s where both ends are consumers (consumers never talk to
  consumers) — drop/ignore them.

## 6. Token verification (server side)

The server **verifies but never issues** tokens (issuance is the existing IdP/backend).

- Fetch/configure the IdP public keys (JWKS or static public key).
- Allowed algorithms: `ES256` / `RS256`. **Reject `alg:none`** and algorithm confusion.
- Validate `exp`, `nbf`, `iss`, and `aud` (audience = this service/room as configured).
- On any failure: `{ "type":"auth-err", "status":"UNAUTHENTICATED", "message":"…" }` then close.

Verification at the signaling layer is an admission gate; the Normal Client independently
verifies the same token on the `'rpc'` channel (RPC spec §2).

## 7. Director role: health & load balancing

The server maintains, per room, the set of healthy Normal Clients and uses it to build both
`peer-list` (for normals) and `server-list` (for consumers).

**Heartbeat (Normal→server):**
```jsonc
{ "type":"heartbeat", "load": { "sessions": 12 },
  "serveConsumers": true, "capacity": 50 }
```
- Missed heartbeats / WS close → remove the node from both lists. Removing it from `server-list`
  is what triggers Consumer **failover** (the Consumer re-resolves and reconnects).

**`server-list` construction (round-robin default):**
- Include only nodes with `serveConsumers:true` that are below `capacity` and currently healthy.
- Maintain a per-room rotating pointer. Each `server-list` response is the eligible set **rotated
  by the pointer**, then advance the pointer. This makes independent Consumers start on different
  nodes → even, dependency-light round-robin with no central per-request load tracking.
- The Consumer chooses its strategy over this list: cycle in order (round-robin) or `ping` the
  top N and pick the fastest (RPC spec §3 `ping`/`pong`). The server only needs to provide a fair,
  rotated, healthy list.
- If no eligible node exists → return `server-list` with an empty `servers` array; the Consumer
  backs off and retries.

**Admission backstop:** even with a fresh `server-list`, a Normal Client at capacity rejects the
session at the RPC `auth` step with `UNAVAILABLE`, and the Consumer moves to the next candidate.
So correctness does not depend on the director's load view being perfectly fresh.

## 8. Message reference (additions/changes)

| Message | Direction | New/changed fields |
|---|---|---|
| `register` | client→server | `role`, and for consumer `token`; for normal `serveConsumers`, `capacity` |
| `server-list` | server→consumer | **new** — `servers: string[]` (rotated, healthy, opted-in) |
| `auth-err` | server→consumer | **new** — `status`, `message` (token rejected → close) |
| `heartbeat` | normal→server | **new** — `load`, `serveConsumers`, `capacity` |
| `offer` | relayed | `fromRole` hint added |
| `peer-list` | server→normal | unchanged; **must exclude consumers** |

## 9. Security & abuse notes

- Rate-limit `register` attempts per IP/identity; a flood of consumer registrations shouldn't
  exhaust the server or the Normal Clients.
- Do not expose the full member list to consumers — they receive only the `servers` pool, not
  other consumers.
- Validate `room` access per the token's claims if rooms are tenant-scoped.

## 10. Implementation checklist (signaling team)

- [ ] Parse `role` on `register`; default missing → `normal`.
- [ ] Verify consumer `token` (JWKS, ES256/RS256, reject `alg:none`, check exp/aud/iss).
- [ ] Track normals' `serveConsumers`/`capacity`/heartbeat; maintain healthy set per room.
- [ ] Build `server-list` (rotated round-robin over eligible normals); send to consumers only.
- [ ] Never include consumers in `peer-list`; never relay consumer↔consumer offers.
- [ ] Add `fromRole` to relayed offers.
- [ ] Remove dead/expired nodes from both lists (drives consumer failover).
