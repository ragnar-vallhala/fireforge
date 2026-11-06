# Phase 5 — Artifact Infrastructure: Direct Peer Transfer, Placement, Retention

Status: implemented, v1.0 (2026-11-06). Builds on Phases 1–4.

Goal: a node that needs an artifact it doesn't hold fetches it **directly from a
peer** that does (Description.md §13.1). Transfers are chunked, resumable,
optionally rate-limited, and checksum-verified, and the control plane never
relays payloads. Jobs with inputs no longer need to land on a holder (Phase 4's
strict locality). Holders are still preferred. Each node keeps its store under a
disk cap, evicting least-recently-used copies that exist elsewhere and never the last copy.

Decisions (2026-11-06):
- transfers use a **separate mTLS data connection** to the peer port, not the peer link
- placement: **prefer holders, then anyone** after `locality_wait` (15 s)
- retention: **disk cap + LRU, keep the last copy**; admins can pin
  (§21 open item "artifact retention" → decided)

---

## 1. Components

```
 RemoteExecutor (Phase 4) ── lease with missing inputs ──► IArtifactFetcher::fetch
                                                             │
 ArtifactService (runtime/transfer)                          ▼
   fetcher: GET /artifacts/sha256/{hex} (holders) ─► PeerManager::dial_data(holder)
            ─► ARTIFACT_REQUEST{offset} ◄─ OFFER, CHUNK... (window, acks) ─► verify ─► store
   server:  PeerManager data acceptor ─► serve from local store (rate limit, max_serving)
 StoreGc (runtime/transfer): disk cap, LRU by mtime, POST /artifacts/sha256/{hex}/release
 tdrcp: placement with locality_wait, release/pin endpoints
```

| Module | Path | Owns |
|---|---|---|
| peer (extend) | `runtime/peer` | data links: inbound routing by first frame, `dial_data` |
| transfer (new) | `runtime/transfer` | ArtifactService (serve + fetch), StoreGc |
| remote (extend) | `runtime/remote` | fetch inputs before local submit; capacity held while fetching |
| cpdb/cpapi (extend) | `control/*` | `queued_at` + locality_wait placement; release; pin |

## 2. Data connections

- A data link is a normal TlsTransport link to the peer's listen port, with the same
  node certificates. The dialer checks the CN as for peer links.
- **Routing.** The first frame on an inbound link decides its kind:
  `NODE_HELLO` makes it a peer link (Phase 3), and `ARTIFACT_REQUEST` makes it a data link,
  handed to the data acceptor. The acceptor side requires the cert CN to be a
  directory node and `sender_id` to match that node's id. Anything else gets the Phase 3 protocol error.
- `PeerManager::dial_data(node_id, ...)` tries the node's addresses in `dial_order`
  (LAN first) and returns an outbound link whose frames go to the caller's handlers.
- Data links don't count as peer links (no ping, no dedup, not in snapshots).
  One link per transfer; it closes when the transfer ends.

## 3. Transfer protocol (on a data link)

Payloads in `proto/fireforge/tdr/v1/artifact.proto`:

```
fetcher                                   server
ARTIFACT_REQUEST{artifact_id, offset, window}  ─►
                                          ◄─  ARTIFACT_OFFER{artifact_id, size, sha256, media_type, offset}
                                              (or ERROR NOT_FOUND / UNAVAILABLE; then close)
                                          ◄─  ARTIFACT_TRANSFER{offset, data (≤ chunk_bytes), last}  × ≤ window in flight
ARTIFACT_CHUNK_ACK{next_offset}  ─►            (server sends more as acks arrive)
… last chunk → fetcher closes
```

- `chunk_bytes` 1 MiB, `window` 8 chunks (8 MiB in flight). This flow control
  keeps the transport's send queue bounded.
- **Resume.** The fetcher writes to `<store>/partial/<hex>.part`. A retry (same or
  another holder) requests `offset = current .part size`, and the server streams from there.
  A `.part` larger than the offered size, or one that fails verification, is discarded and the fetch restarts from 0.
- **Verify.** At `last`, the fetcher fsyncs, runs `sha256_file(.part)`, and requires it to equal the id. It then
  `store.ingest(.part, media_type, producer_job="", move_source=true)`, records the journal artifact row,
  and announces (incremental, through the RemoteExecutor).
- **Sources.** Holders come from `GET /artifacts/sha256/{hex}` (excluding self), tried in
  this order: CONNECTED peers by RTT, then other ONLINE nodes. On a link error or idle timeout
  (30 s with no chunk), move to the next holder and keep the `.part`. After every holder has been tried
  (one round), fail with the last error.
- **Dedup.** Concurrent fetches of the same id share one transfer, so every waiter gets the result.
  Limits: `max_fetching` 4 transfers at once (others queue), `max_serving` 8 (beyond that the
  server replies `ERROR UNAVAILABLE "busy"`, and the fetcher moves to the next holder).
- **Rate limit.** `[network] max_transfer_mbps` (0 = unlimited) caps each node's total
  serving rate (token bucket).

## 4. Placement (control plane)

- `jobs.queued_at` is set whenever a job enters QUEUED (submit or requeue).
- A lease is eligible for node N if the Phase 4 rules hold except locality, and **either**
  N holds every input, **or** `now − queued_at ≥ locality_wait` and every input has a
  location on at least one ONLINE node.
  An input with no ONLINE holder keeps the job QUEUED.
- `[jobs] locality_wait = 15` (seconds; 0 = anyone at once).

## 5. Node side: fetch before run

- RemoteExecutor gets a lease whose spec lists inputs missing locally. It persists the mapping,
  then calls `IArtifactFetcher::fetch` for each missing input (in parallel), and calls
  `JobManager::submit` only after all have arrived.
- **Capacity.** While fetching, the lease counts as occupying a job slot plus its cpu and memory
  in the pull loop's `free`, so the node doesn't over-lease. Heartbeats renew it (it is in `active_leases()`).
- If a fetch fails, the job is reported FAILED, `retryable`, with error `fetch <id>: <reason>`. If no fetcher is set yet (peer layer not
  up), it is FAILED retryable with "artifact transfer unavailable".
- A fetch interrupted by a tdrd restart resumes from its `.part`, because the reconcile pass refetches
  inputs for leases that hadn't started.

## 6. Retention (StoreGc)

- `max_disk_mb` (existing `[resources]` key, 0 = unlimited) caps the **artifact store**.
  GC runs every `gc_interval` (60 s) and after each new artifact. When the store is above `gc_high`% (90),
  it evicts toward `gc_low`% (80).
- **Last used** = file mtime. The store files are immutable, so mtime is free to serve as an LRU clock.
  It is touched when an artifact is used as a job input, served, or fetched.
- **Candidates**, oldest first, skipping:
  - inputs of non-terminal local jobs and of fetching leases
  - artifacts being served or fetched
  - artifacts younger than 10 minutes
- **Release (atomic, control plane).** `POST /artifacts/sha256/{hex}/release {node_id}` deletes this
  node's location row only if the artifact is not pinned **and** another node that is not OFFLINE holds a copy.
  The reply is `{released, reason}`. Only a released artifact's file is deleted, so two nodes can never
  both delete the last two copies. A refused artifact (last copy, pinned) is skipped.
- Deleting removes the store file. Its journal row stays as history, and `contains()` is
  false afterwards.
- **Pins** (admin): `POST /artifacts/sha256/{hex}/pin` and `/unpin`. A pinned artifact is never released.
- **Manual delete:** `tdrctl artifact rm ID` goes through release. `--force` deletes even the
  last copy (release with `force`).
- **No space at all:** if a fetch would exceed `max_disk_mb` even after GC, the fetch fails with
  `"store full"` (retryable for the job).

## 7. CLI

- `tdrctl artifact put FILE [--media-type T]` ingests a local file into this node's store and
  announces it. Prints the id.
- `tdrctl artifact get ID [--out PATH]` fetches from peers if missing, then copies the file
  out. tdrctl does the copy, so it needs read access to the store; it prints the store path otherwise.
- `tdrctl artifact ls`, `tdrctl artifact rm ID [--force]`, `tdrctl artifact gc` (runs GC now and prints what it freed).
- `tdrctl cp artifact ID` shows the holders plus `pinned`; `tdrctl cp artifact-pin|artifact-unpin ID`.

## 8. Configuration

- tdrd `[network] max_transfer_mbps = 0`.
- tdrd `[artifacts] gc_high = 90`, `gc_low = 80`, `gc_interval = 60`, `gc_min_age = 600` (seconds).
- tdrcp `[jobs] locality_wait = 15`.

## 9. Testing

- peer: data-link routing (inbound first frame, sender check, unknown CN rejected),
  `dial_data` path fallback.
- transfer: real TLS between two services on 127.0.0.1:
  - small, empty and multi-chunk (32 MiB) artifacts
  - resume after the source link is cut mid-transfer, from a second holder at the right offset
  - corrupted source → verification fails, `.part` discarded
  - NOT_FOUND → next holder
  - busy → next holder
  - concurrent fetches of the same id share one transfer
  - rate limit roughly honoured
  - stop/destroy with transfers in flight is ASan-clean
- gc: watermarks, LRU order, protected sets, release refused → kept, young artifacts kept.
- cpdb/cpapi: queued_at + locality_wait eligibility (holder first, others after the wait,
  no-holder input stays queued), release atomicity (two nodes releasing the last two copies:
  exactly one succeeds), pin/unpin, force.
- remote: lease with missing inputs → fetch → run; fetch failure → FAILED retryable; capacity
  held while fetching.
- E2E (`tests/e2e/test_artifacts.py`), 3 nodes:
  - `artifact put` on A, a job with that input runs on B (A has no slots), and B fetched it
  - `artifact get` on C
  - interrupted transfer (rate-limited source killed mid-way) resumes from the other holder
  - GC on a small cap evicts replicated copies but keeps the last copy; pinned is kept
  - two nodes evicting the same last two copies: one copy survives

Exit criteria: Description.md §20 "interrupted artifact transfer" and "artifact
transfer streaming and resumable" demonstrated on localhost.

## 10. Status (2026-11-06)

Implemented as designed.

| Check | Result |
|---|---|
| C++ suites (15, `-Werror`) | all pass. New tests: peer 7 data-link cases, transfer 13 service (real TLS) + 14 GC, remote +8, cpdb +11, cpapi +4 |
| ASan/UBSan (peer, transfer, remote, agent, cpdb, cpapi) | clean |
| e2e | 47 pass (5 artifact + 9 remote + 4 peer + 10 control-plane + 19 Phase 1), 3 runs in a row |
| Concurrent release of the last two copies | 50 rounds in cpdb + e2e: exactly one copy released each time |

Behaviour notes:
- Error code `UNAVAILABLE = 7` was added to `ErrorCode` (local.proto). Servers use it for "busy" and "stopped".
- `PeerManager::self_node_id()` was added.
- When a lease arrives, the RemoteExecutor passes every input to the fetcher, which returns at once for inputs already held. tdrd starts pulling leases only after the transfer service is up (on the first directory), so leases are not pulled before a fetcher exists.
- The state file (`remote.json`) is version 2: each lease records `submitted`. A lease that was still fetching when tdrd restarted is fetched again (resuming from its `.part`) and then run.
- A release counts only holders that are not OFFLINE, so an OFFLINE node's copy never lets the last reachable one go. `--force` overrides both the pin and the last-copy check.
- GC re-checks protection after a successful release. If the artifact became protected meanwhile, the file is kept and the next full sync re-announces it.
- `tdrctl artifact gc` evicts down to `gc_low` immediately (`force_to_low`). The periodic pass starts only above `gc_high`.

Known limitations:
- An incremental announcement still queued when GC releases the same artifact can re-add the location after the file is deleted. The control plane then lists a holder without the file until that node's next full sync. `gc_min_age` (10 min) keeps this from happening with defaults.
- Holders are tried CONNECTED-first by RTT, but only the order among offline-directory nodes is unit-tested.
- Empty `sha256/<2 hex>` directories stay after eviction.
- A resumed lease that needs inputs waits until a fetcher exists. If the peer layer never starts, the lease is renewed forever.
