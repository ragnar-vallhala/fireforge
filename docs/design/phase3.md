# Phase 3 — Peer Networking: Direct Node-to-Node Connectivity

Status: design, v0.1 (2026-11-05). Builds on Phases 1–2 (phase1.md, phase2.md).

Goal: every node keeps an authenticated, direct connection to every other
online node, using the control-plane peer directory to find them. Paths
are LAN first, then Tailscale. Each node measures liveness and RTT and reports
reachability to the control plane. No jobs or artifacts cross these links
yet (Phase 4 and 5 add those message types on top).

Decisions (2026-11-05): peer auth = **mTLS with the Phase 2 CA**; scope =
**connectivity + liveness**; topology = persistent full mesh.

---

## 1. Components

```
 tdrd
 ├─ NodeAgent (Phase 2) ── peer directory ──┐
 └─ PeerManager (runtime/peer) ◄────────────┘ update_directory()
      │  per peer: state machine, path choice, backoff, ping/RTT, dedup
      ▼
    ITransport ── TlsTransport (runtime/peer)
      │  listener on [network] listen_address:listen_port (7400)
      │  dialer: TCP connect + TLS 1.3 handshake + name check
      ▼
    IFrameLink: one TLS stream carrying 48-byte envelopes + proto payloads
```

| Module | Path | Target | Owns |
|---|---|---|---|
| peer | `runtime/peer` | `tdr_peer` | TlsTransport, IFrameLink, PeerManager |
| tls (change) | `runtime/tls` | `tdr_tls` | node certs get serverAuth+clientAuth; `make_peer_context` |

## 2. Authentication

- Every peer link is TLS 1.3 with mutual certificates from the FireForge CA.
  Only **node** certificates are accepted on either side.
- Node certificates now carry EKU `serverAuth, clientAuth`, because a node is a TLS
  server for inbound peers and a client for outbound ones. **Node certs issued in Phase 2
  must be re-issued** (`tdrctl pki issue ... --role node NAME`).
- There is no hostname/IP check, because peers dial by IP and node certs have no SANs.
  Instead the dialer checks that the server cert's CN equals the directory
  name of the node it meant to reach. A mismatch closes the link with error `identity mismatch`.
- The acceptor checks that the client cert CN equals `PeerHello.name`, and that
  `PeerHello.node_id` matches the directory entry for that name (if the directory
  knows the name; otherwise the hello is accepted and a directory refresh requested).
- Every frame's `sender_id` must equal `node_id_from_string(peer node_id)`.
  Frames that fail this are dropped and the link is closed.

## 3. Wire protocol (inside TLS)

Same 48-byte envelope as Phase 1 (big-endian header + proto3 payload), with
payloads from `proto/fireforge/tdr/v1/peer.proto`:

| Type | Payload | Direction | Notes |
|---|---|---|---|
| `NODE_HELLO` | `PeerHello` | dialer → acceptor | first frame, within `hello_timeout` (5 s) |
| `NODE_HELLO` | `PeerHello` | acceptor → dialer | reply (`correlation_id` = request id) |
| `NODE_HEARTBEAT` | `PeerPing` | either | ping; peer echoes it back as a reply with the same type |
| `PEER_ADVERTISE` | `PeerGoodbye` | either | graceful close (shutdown, duplicate); then close |
| `ERROR` | `Error` | either | rejection (UNAUTHORIZED, BAD_REQUEST); then close |

`PeerHello.protocol_version` = 1. Different major → `ERROR UNSUPPORTED` and close.
Any frame before the hello completes, other than HELLO/ERROR, is a protocol error.
Unknown message types after the hello go to the Phase 4 frame handler. If there is none,
they're ignored and logged at debug.

## 4. Connection management

**Directory → peers.** The NodeAgent refreshes `GET /nodes` (every 10 s in
Phase 3, previously 30 s) and calls `PeerManager::update_directory`. Each entry
other than self becomes a peer:
- ONLINE or STALE → dial target.
- OFFLINE → not dialed. An existing link is kept while it stays healthy, and inbound links are accepted.
- Removed from the directory → link closed, peer dropped.

**Who dials.** Both sides dial. When two links to the same peer exist, the link
**dialed by the node with the lower node_id** (string compare) is kept. Both
sides apply the same rule, so they agree without coordination. The other link
gets `PeerGoodbye{reason: "duplicate"}` and is closed.

**Path order.** The peer's advertised addresses sorted: `lan` before
`tailscale` before anything else, keeping the advertised order within a type.
A dial round tries each path in turn (`connect_timeout` 3 s each, including
TLS). The first success is the link's path.

**Backoff.** After a failed round: 1 s, 2 s, 4 s … up to 60 s, ±20% jitter.
After `unavailable_after_rounds` (3) consecutive failed rounds, the peer becomes
**UNAVAILABLE** and dialing continues at the max interval. Any success resets this.

**Liveness.** Every `ping_interval` (5 s) on each connected link, send a PeerPing.
RTT is the time to the echo, kept as an EWMA (α = 0.3) plus the last sample.
With no frame received for `ping_timeout` (15 s), the link is closed as `ping timeout` and
the peer goes back to dialing (backoff starts at min).

**Peer states:**

| State | Meaning |
|---|---|
| CONNECTING | no link; a dial round in progress or scheduled |
| CONNECTED | authenticated link up, hello done |
| UNAVAILABLE | ≥ N failed rounds; still retrying slowly |
| IDLE | not a dial target (OFFLINE in the directory) and no link |

**Shutdown.** `PeerManager::stop()` sends PeerGoodbye{"shutdown"} on every link,
closes them, and stops dialing and the listener. The peer then goes to CONNECTING
and backs off. Its next directory refresh shows the node OFFLINE, and it goes IDLE.

## 5. Reporting

- `Heartbeat.peers` (control.proto) is a repeated `PeerReachability{node_id, name, state,
  path_type, address, rtt_ms, connected_since_unix_ms, last_error}`.
  It is stored with the heartbeat (JSONB), so no schema change is needed. Each node's
  view of the mesh appears under `tdrctl cp node ID`.
- `tdrctl peers` (local) shows the directory merged with live link state: state,
  path, RTT, since, last error. PeerList gains `repeated PeerConnection connections`.

## 6. Configuration

`[node] advertise_address` also accepts an explicit list
`type=addr[:port], ...` (port defaults to `listen_port`), for NAT/port-forwarding
setups and for tests that need several paths.

`[network] listen_address` (0.0.0.0) and `listen_port` (7400) already exist.
The peer listener starts only when `[control] address` is set: there's no directory
without a control plane, and it reuses the `[tls]` node credentials.
Tuning for tests (`[network]` keys): `peer_ping_interval` (5 s), `peer_ping_timeout` (15 s).

## 7. Testing

- `tdr_peer_tests`, transport: real TLS links on 127.0.0.1 with test CAs. Covers hello
  framing, identity mismatch, non-node certs rejected, foreign CA rejected,
  connect timeout, frame size limits, close semantics, stop().
- `tdr_peer_tests`, manager: fake transport. Covers path order, fallback to the second
  path, backoff and UNAVAILABLE, dedup from both sides, ping RTT, ping timeout,
  directory add/remove/OFFLINE, sender_id check, hello validation, goodbye.
- E2E (`tests/e2e/test_peers.py`): tdrcp + 3 tdrd nodes on localhost, each with a
  fake "lan" address that is unreachable (127.0.0.2:<closed port>) and a reachable
  "tailscale" address:
  - full mesh forms over the fallback path; RTT reported
  - stopping a node (graceful) → peers see it go; restart → reconnects
  - SIGSTOP a node → ping timeout → others mark it down; SIGCONT → back
  - wrong-CA node cannot join
  - reachability visible through `tdrctl cp node`

Exit criteria: a 3-node mesh on localhost forms within 2 directory refreshes,
recovers from node restart and pause, and shows path and RTT both locally and in the
control plane.

## 8. Status (2026-11-05)

Implemented as designed.

| Check | Result |
|---|---|
| C++ suites (13, `-Werror`) | all pass; `tdr_peer`: 11 transport (real TLS) + 16 manager (fake network, incl. both-sides dedup races) |
| ASan/UBSan (peer, tls, agent) | clean |
| e2e | 33 pass (4 peer + 10 control-plane + 19 Phase 1), stable over 3 runs |
| Mesh formation, 3 nodes on localhost | ~2 s |

Behaviour notes:
- A link is reported with the dialer's path (`lan`/`tailscale`). On the accepting
  side the path type is inferred from the remote IP and is often `inbound`.
- The lower-node-id rule only applies when both sides dialed at once. A node
  that connects inbound first keeps that single link, and the directory refresh it
  triggers doesn't start a second dial.
- Node certs issued before Phase 3 (clientAuth only) can't accept peer links.
  Re-issue them.

Known limitations:
- RTT is measured only after the first ping (`peer_ping_interval`), so it is 0 until then.
- A node whose id changes (control-plane database reset) keeps its old id on peer
  links until tdrd restarts. This is logged.
- Dead links close between `ping_timeout` and `ping_timeout + ping_interval`.
