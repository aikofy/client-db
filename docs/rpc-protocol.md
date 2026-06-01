# RPC Protocol Spec (Consumer ⇄ Normal Client)

> Phase 0 deliverable. Defines the wire contract carried over the WebRTC **`'rpc'`**
> data channel between a Consumer Client (caller) and a Normal Client (server).
> Status: **proposed**. No implementation yet. See [`consumer-client-plan.md`](./consumer-client-plan.md).

## 1. Scope & framing

- This protocol rides a single ordered `RTCDataChannel` whose **label is `'rpc'`**. A `'sync'`
  channel carries gossip (`SyncMessage`) and is out of scope here; the two never mix.
- Every frame is a UTF-8 JSON object, sent with `channel.send(JSON.stringify(frame))` — the same
  framing the existing `SyncMessage` path uses.
- Frames are discriminated by a `type` field. The full set lives in a new `RpcMessage` union in
  `src/rpc/protocol.ts`; it is **independent** of the existing `SyncMessage` union.
- Unparseable frames are ignored (consistent with `webrtc-transport.ts` today). A frame with a
  known `type` but invalid shape is answered with an `err` of status `INVALID_ARGUMENT` when it
  carries an `id`; otherwise dropped.
- `id` (correlation id) is a UUIDv4 string minted by the **Consumer**, unique per connection.
  Responses echo it. The server never originates an `id`.

## 2. Connection lifecycle

```
channel 'rpc' opens
        │
        ▼
Consumer ──► auth { token }
        ◄── auth-ok { server, methods, serverHlc, limits }   (or auth-err → close)
        │
        ├──► req { id, method, params }        ──► res { id, OK, body }
        ├──► req { id, method, params }        ──► err { id, status, retryable }
        ├──► req (stream method)               ──► stream-chunk* ──► stream-end
        ├──► ping { id }                       ──► pong { id }
        └──► cancel { id }                     (aborts an in-flight req/stream)
```

Rules:
1. The **first** frame a Consumer sends MUST be `auth`. Any `req` before a successful `auth-ok`
   is answered `UNAUTHENTICATED` (and the server MAY close the channel).
2. After `auth-err`, the server closes the channel. The Consumer must re-resolve/re-auth
   (or fail over — see §7).
3. The session identity established by `auth` is bound to the connection for its lifetime.
   No per-`req` token is required (but see token expiry in §8).

## 3. Frame reference

All frames include `type`. Frames tied to a call also include `id`.

### Control

| Frame | Direction | Fields |
|---|---|---|
| `auth` | C→N | `token` (JWS string), `protocolVersion` (int) |
| `auth-ok` | N→C | `server` (nodeId), `protocolVersion`, `methods` (catalog, §9), `serverHlc` (HLCTimestamp), `limits` (§10) |
| `auth-err` | N→C | `status` (`UNAUTHENTICATED`/`PERMISSION_DENIED`), `message` |
| `ping` | C→N | `id`, `ts` (client clock ms, for RTT) |
| `pong` | N→C | `id`, `ts` (echoed) |
| `cancel` | C→N | `id` (the call to abort) |

### Unary call

| Frame | Direction | Fields |
|---|---|---|
| `req` | C→N | `id`, `method` (e.g. `"orders.book@v1"`), `params` (object), `idempotencyKey?` (string), `deadlineMs?` (int), `readAfter?` (HLCTimestamp, §6) |
| `res` | N→C | `id`, `status: "OK"`, `body` (handler return), `hlc?` (watermark for writes, §6) |
| `err` | N→C | `id`, `status` (§4), `message`, `retryable` (bool) |

### Streaming call

| Frame | Direction | Fields |
|---|---|---|
| `stream-start` | N→C | `id` (server acknowledges a streaming response will follow) |
| `stream-chunk` | N→C | `id`, `seq` (0-based int), `chunk` (array/object batch) |
| `stream-end` | N→C | `id`, `status: "OK"`, `hlc?` |
| `err` | N→C | terminates a stream early with a non-OK `status` (same `id`) |

`seq` is strictly increasing and contiguous. The Consumer delivers chunks in `seq` order and
treats `stream-end` (or a terminating `err`) as completion.

## 4. Status codes

Modeled on gRPC. `retryable` is the **default**; a server may override the flag per response.
"Failover" = the Consumer should try another Normal Client (see §7).

| Status | Meaning | Retryable | Failover |
|---|---|---|---|
| `OK` | Success | — | — |
| `UNAUTHENTICATED` | Missing/invalid/expired token | No (re-auth first) | No |
| `PERMISSION_DENIED` | Authenticated but scope/policy forbids | No | No |
| `INVALID_ARGUMENT` | Input failed validation | No | No |
| `NOT_FOUND` | Target does not exist | No | No |
| `FAILED_PRECONDITION` | State conflict (e.g. slot taken) | No | No |
| `ALREADY_EXISTS` | Idempotency/uniqueness conflict | No | No |
| `RESOURCE_EXHAUSTED` | Rate limit / payload cap | Yes (after backoff) | Maybe |
| `UNAVAILABLE` | Server draining / at capacity / catching up | Yes | **Yes** |
| `DEADLINE_EXCEEDED` | Handler exceeded `deadlineMs` | Only if idempotent | Maybe |
| `INTERNAL` | Unhandled server error | Only if idempotent | No |
| `CANCELLED` | Client cancelled or channel closed | No | No |

## 5. Deadlines & cancellation

- `req.deadlineMs` is a client-relative timeout. If absent, the server applies
  `limits.defaultDeadlineMs` from `auth-ok`.
- The server exposes the deadline to handlers via `ctx.signal` (an `AbortSignal`) and SHOULD stop
  work when it fires, replying `DEADLINE_EXCEEDED` (or nothing if the client already gave up).
- The Consumer may send `cancel { id }` at any time (e.g. caller walked away). The server aborts
  `ctx.signal` and stops emitting frames for that `id`.
- A `cancel` for an unknown/finished `id` is a no-op.

## 6. Idempotency & read-your-writes

**Idempotency (writes).** `req.idempotencyKey` is an opaque client-supplied string. The server
dedupes on `(consumerId, method, idempotencyKey)` within a TTL window:
- First occurrence → run the handler, cache `{status, body, hlc}`.
- Replay within TTL → return the cached result **without** re-running the handler.
This makes the failover replay in §7 safe (a write is never double-applied). The dedupe store
SHOULD be replicated so the guarantee survives a hop to another Normal Client. TTL is a
deployment knob (default proposal: 10 min).

**Read-your-writes.** A write `res`/`stream-end` carries `hlc` — the HLC watermark of the applied
write. The Consumer retains the latest watermark and MAY attach it as `req.readAfter` on a later
read. A Normal Client that has **not** yet merged up to `readAfter` either waits (bounded) for
gossip to catch it up, or replies `UNAVAILABLE` (retryable) so the Consumer retries / fails over.
With sticky sessions this is a no-op (same replica); it only matters across failover.

## 7. Failover (client behavior)

On `UNAVAILABLE`, a channel close, or a `DEADLINE_EXCEEDED`/`INTERNAL` on an **idempotent** call,
the Consumer:
1. Advances to the next healthy candidate (round-robin order, or re-pings for fastest — see
   signaling spec §load-balancing and plan §7).
2. Reconnects, re-sends `auth` (same token — self-verifiable, no shared session state).
3. Replays in-flight calls that are **idempotent** (carry an `idempotencyKey`) or are pure reads.
   Non-idempotent, non-retryable calls surface their error to the caller instead.
4. Re-attaches the last `hlc` watermark as `readAfter` so read-your-writes survives the hop.

## 8. Token expiry mid-session

Tokens are short-lived. The server MAY reject a `req` with `UNAUTHENTICATED` once the session's
token has expired. The Consumer SHOULD proactively refresh (from the IdP) before expiry and send
a fresh `auth` frame to re-establish the session on the same channel (no reconnect needed). A
`reauth` is just another `auth` frame; the server replaces the bound identity on `auth-ok`.

## 9. Method catalog & versioning

- `auth-ok.methods` advertises what this server supports:
  `{ name, kind: "read"|"write"|"stream", version: int }[]`.
- Method identifiers are `name@vN` (e.g. `orders.book@v2`). Multiple versions may coexist.
- The Consumer SDK fails fast (client-side `NOT_FOUND`) if it needs a method/version the server
  did not advertise, rather than sending a doomed `req`.
- `protocolVersion` gates envelope-level changes; mismatch → `auth-err` with guidance.

## 10. Limits & backpressure

- `auth-ok.limits`: `{ maxPayloadBytes, defaultDeadlineMs, rateLimit: { perMin } }`.
- A frame exceeding `maxPayloadBytes` → `INVALID_ARGUMENT` (request) or the server splits the
  response into a stream.
- Large/streaming responses ride the existing `sendAsync()` backpressure in
  `webrtc-transport.ts` (16 MB high-water / 1 MB low-water), so a fast producer can't overflow
  the channel. Rate-limit trips → `RESOURCE_EXHAUSTED`.

## 11. Worked sequences

**Unary write with failover + idempotent replay**
```
C → auth{token}                         N → auth-ok{server:A, serverHlc}
C → req{id:1, "orders.book@v1", params, idempotencyKey:"k1", deadlineMs:5000}
                                        (Normal A applies write, begins reply…)
✗ Normal A goes offline (channel close)
C picks Normal B (next round-robin) → auth{token} → auth-ok{server:B}
C → req{id:1', "orders.book@v1", params, idempotencyKey:"k1", readAfter:<lastHlc>}
                                        N(B): dedupe miss → applies once → res{id:1', OK, hlc:H}
```

**Server-streaming read**
```
C → req{id:7, "records.export@v1", params, deadlineMs:120000}
N → stream-start{id:7}
N → stream-chunk{id:7, seq:0, chunk:[…]}   (backpressure-gated)
N → stream-chunk{id:7, seq:1, chunk:[…]}
C → cancel{id:7}                            (caller navigated away)
N stops; (no stream-end required after cancel)
```
