# Phase 2 — Control Plane: Multi-node Registration, Health and Campaigns

Status: design, v0.1 (2026-11-05). Builds on Phase 1 (docs/design/phase1.md).

Goal: a lightweight control plane, `tdrcp`, that nodes register with over
mutually-authenticated HTTPS. It tracks node health from heartbeats, serves the
peer directory, holds per-node resource policies that `tdrd` applies live,
and owns campaign state in PostgreSQL. Running jobs never depend on it: when
it's down, nodes keep running work and reconnect when it comes back.

Decisions (2026-11-05): `tdrcp` in C++ (Boost.Beast + libpq); node auth via
mTLS with a private FireForge CA; scope = nodes + campaigns (jobs over the
network are Phase 4); dev/test Postgres = throwaway clusters (`initdb`, no sudo).

---

## 1. Components

```
 tdrctl (admin cert) ──HTTPS/mTLS──┐
                                   ▼
 tdrd (node cert) ──HTTPS/mTLS──► tdrcp ──libpq──► PostgreSQL
   └─ NodeAgent: register, heartbeat,   ├─ HttpsServer (runtime/http)
      apply policy, peer cache,         ├─ ControlApi  (control/api)
      offline on shutdown               ├─ Repository  (control/db)
                                        └─ health sweeper (timer)
```

| Module | Path | Target | Owns |
|---|---|---|---|
| tls | `runtime/tls` | `tdr_tls` | CA + cert issuing, identity from certs, Asio SSL contexts |
| http | `runtime/http` | `tdr_http` | async HTTPS server + router, async/sync HTTPS client |
| agent | `runtime/agent` | `tdr_agent` | tdrd side: registration, heartbeats, policy, peers |
| cpdb | `control/db` | `tdr_cpdb` | libpq wrapper, migrations, repository, test clusters |
| cpapi | `control/api` | `tdr_cpapi` | endpoint handlers, auth rules, health sweep |
| tdrcp | `control/server` | `tdrcp` | main: config, wiring |

Payload schemas: `proto/fireforge/control/v1/control.proto`, sent as **proto3
JSON** (`preserve_proto_field_names`, unknown fields ignored on parse, so
newer clients can talk to older servers). One schema serves C++, Python and the wire.

**Threading.** `tdrcp` runs one io_context thread. Handlers are synchronous
and call libpq on that thread (one connection). That's fine for tens of nodes
heartbeating every 10 s (each query takes about a millisecond). If it becomes a
bottleneck, a connection pool plus worker threads goes behind the same
`IRepository` interface.

## 2. PKI and identity (mTLS)

- `tdrctl pki init DIR` creates a CA (`ca.key` 0600, `ca.crt`), using ECDSA P-256, valid 10 years.
- `tdrctl pki issue DIR --role node|admin|server NAME [--dns H]... [--ip A]...`
  writes `NAME.key` (0600) + `NAME.crt`, signed by the CA and valid 1 year.
- **Subject:** `CN=<name>, OU=<role>, O=FireForge`. EKU is clientAuth for node
  and admin, serverAuth for server. Server certs carry DNS/IP SANs and clients
  check them (RFC 2818 host check).
- **TLS:** 1.3 only. The server *requires* a client cert chaining to `ca.crt`,
  and clients verify the server against the same CA.
- **Identity** = (role, CN, SHA-256 fingerprint of the cert DER). Handlers get
  it from the TLS session; there are no passwords and no bearer tokens.
- Node files (Description.md §5.2): `/etc/fireforge/node.key`,
  `/etc/fireforge/certificates/{ca.crt,node.crt}`.
- Revocation: not in Phase 2. Rotate by issuing new certs; a CRL check is a later addition.

## 3. HTTP API (v1)

All bodies are JSON (`application/json`), and errors are
`{"code": "...", "message": "..."}` with a matching HTTP status.

| Endpoint | Method | Role | Body → Reply |
|---|---|---|---|
| `/api/v1/nodes/register` | POST | node (`name` must equal CN) | RegisterRequest → RegisterReply |
| `/api/v1/nodes/heartbeat` | POST | node (owns `node_id`) | Heartbeat → HeartbeatReply |
| `/api/v1/nodes` | GET | node, admin | → NodeList (`?health=ONLINE` optional) |
| `/api/v1/nodes/{id}` | GET | node, admin | → NodeInfo |
| `/api/v1/nodes/{id}/limits` | PATCH | admin | UpdateLimitsRequest → ResourcePolicy |
| `/api/v1/nodes/{id}/offline` | POST | node (self), admin | OfflineRequest → NodeInfo |
| `/api/v1/nodes/{id}/events` | GET | admin | → NodeEventList (`?limit=`) |
| `/api/v1/campaigns` | POST | admin | CreateCampaignRequest → Campaign (201) |
| `/api/v1/campaigns` | GET | node, admin | → CampaignList (`?state=ACTIVE`) |
| `/api/v1/campaigns/{id}` | GET | node, admin | → Campaign |
| `/api/v1/campaigns/{id}` | PATCH | admin | UpdateCampaignRequest → Campaign |
| `/api/v1/audit` | GET | admin | → AuditList (`?limit=`) |
| `/healthz` | GET | any authenticated | `{"status":"ok"}` |

Status codes: 400 bad JSON/field, 401 no/invalid client cert (normally
rejected in the handshake), 403 wrong role or identity, 404 unknown id,
409 invalid transition, 413 body > 1 MiB, 500 internal.

The table in Description.md §6.2 lists `/api/v1/jobs` and `/api/v1/artifacts`.
Those are Phase 4 and 5.

## 4. Nodes

**Registration** (idempotent, keyed by name). If the name is new, the
control plane creates a `node_id` (`ff-node-<16 hex>`) and a default policy
(version 1, all limits 0 = "no central limit"). If the node is already known,
it keeps its node_id and policy, updates addresses, capabilities, resources,
version and cert fingerprint, and sets health to ONLINE. The reply carries
`heartbeat_interval_seconds`.

**Heartbeat** every `heartbeat_interval_seconds` (default 10). The fields come
from Description.md §8.4. The reply includes the full policy **only** when
`policy.version` ≠ the node's applied `policy_version`. An unknown node_id
returns 404, and the node registers again.

**Health** is computed by a sweeper in tdrcp every `heartbeat_interval`:

| From | To | When |
|---|---|---|
| any | ONLINE | register or heartbeat received |
| ONLINE | STALE | no heartbeat for `stale_after` (default 30 s = 3 intervals) |
| ONLINE/STALE | OFFLINE | no heartbeat for `offline_after` (default 90 s), or explicit `/offline` |

Each change is stored in `node_events` (time, from, to, reason).

**Policy** (Description.md §8 "central policies, local enforcement"):
`ResourcePolicy{version, profile, max_cpu_cores, max_memory_bytes, max_jobs,
max_disk_bytes, max_gpus}`, where 0 means no central limit. PATCH applies a
profile preset first (§8.3: small 4/8 GB/2, medium 12/24 GB/4, compute
32/96 GB/8, gpu 16/48 GB/4) and then any explicit fields, and bumps the version.
`tdrd` enforces `effective = min(local tdr.conf cap, policy limit when non-zero)`.
A smaller cap only affects admission; running jobs are never killed to meet it.

## 5. Campaigns

`Campaign{campaign_id ("camp-<16 hex>"), name (unique), description, domain
(e.g. "thermoelectric"), state, parameters_json, created_by, created/updated}`.

```
DRAFT ──► ACTIVE ◄──► PAUSED
  │          │           │
  │          └─► COMPLETED ◄┘
  └──────────────► ARCHIVED ◄── COMPLETED
```
Allowed: DRAFT→ACTIVE, DRAFT→ARCHIVED, ACTIVE↔PAUSED, ACTIVE→COMPLETED,
PAUSED→COMPLETED, COMPLETED→ARCHIVED. Any other transition returns 409. Updates are a
compare-and-set on the current state, so two admins cannot race a transition.
`parameters_json` must parse as JSON. Phase 4 adds jobs referencing a campaign.

## 6. Database (PostgreSQL 16)

Migrations run at tdrcp start (`schema_migrations(version, applied_at)`), each
in one transaction. Tables:

- `nodes(node_id PK, name UNIQUE, health, addresses JSONB, capabilities JSONB,
  daemon_version, physical JSONB, configured JSONB, cert_fingerprint,
  registered_at, last_seen, offline_reason, last_heartbeat JSONB)`
- `node_policies(node_id PK FK, version, profile, max_cpu_cores, max_memory_bytes,
  max_jobs, max_disk_bytes, max_gpus, updated_at, updated_by)`
- `node_events(id BIGSERIAL, node_id FK, at, from_health, to_health, reason)`
- `campaigns(campaign_id PK, name UNIQUE, description, domain, state,
  parameters JSONB, created_by, created_at, updated_at)`
- `audit_log(id BIGSERIAL, at, actor, action, target, details JSONB)`, written
  for every admin mutation (policy, offline, campaign create/update), as
  Description.md §19 requires.

Times are `timestamptz` in SQL, and the API exposes `*_unix_ms`. Every
statement uses parameters (`PQexecParams`); SQL text never contains values.

## 7. tdrd changes

`[control] address` empty means standalone (Phase 1 behaviour, unchanged). When set,
`tdrd` creates a `NodeAgent`:

1. Detects physical resources (CPUs, RAM, `statvfs(state_dir)`, GPUs via
   `/dev/nvidia[0-9]*`) and addresses (`getifaddrs`, non-loopback IPv4/IPv6;
   100.64.0.0/10, fd7a:115c:a1e0::/48 or a `tailscale*` interface counts as
   `tailscale`; with `[network] tailscale = off` those addresses are not
   advertised at all; port = `listen_port`). `advertise_address` ≠ auto
   overrides this with one `lan` entry.
2. Registers. On failure it retries with exponential backoff (1 s → 60 s, ±20% jitter),
   and local work keeps running the whole time.
3. Applies the policy (`JobManager::set_capacity`) and caches it in
   `state/policy.json`, so a restart while the control plane is down uses the last known policy.
4. Heartbeats on the interval. On a transport error or 5xx it goes to
   `disconnected`, backs off, and keeps the current policy. On 404 it registers again.
   The first success after a failure is logged as reconnected.
5. Refreshes the peer directory (`GET /nodes`) every 30 s and caches it in memory (Phase 3 uses it).
6. On SIGTERM it POSTs `/offline` with a 2 s timeout and then exits.

`tdrctl node` shows control-plane state, node_id, policy version and last
heartbeat. `tdrctl peers` lists the cached peer directory.

New config (`tdr.conf`):
```ini
[tls]
ca_file   = /etc/fireforge/certificates/ca.crt
cert_file = /etc/fireforge/certificates/node.crt
key_file  = /etc/fireforge/node.key
```

## 8. tdrcp

```
tdrcp --config /etc/fireforge/tdrcp.conf
```
```ini
[server]
listen_address = 0.0.0.0
port = 8080
[tls]
ca_file = /etc/fireforge/certificates/ca.crt
cert_file = /etc/fireforge/certificates/server.crt
key_file = /etc/fireforge/server.key
[database]
conninfo = postgresql:///fireforge
[health]
heartbeat_interval = 10
stale_after = 30
offline_after = 90
[log]
level = info
```

Admin CLI (admin cert): `tdrctl cp ...` with `--url https://host:8080
--ca --cert --key` (or `TDR_CP_URL`, `TDR_CP_CA`, `TDR_CP_CERT`, `TDR_CP_KEY`):
`nodes`, `node ID`, `limits ID [--profile P] [--cpus N] [--memory-mb N] [--jobs N]`,
`offline ID`, `events ID`, `campaigns [--state S]`, `campaign ID`,
`campaign-create NAME [--domain D] [--description T] [--params JSON]`,
`campaign-set ID --state S`, `audit`.

## 9. Testing

- `tools/pg.sh start|stop|env|psql` runs a private dev cluster in `build/pg`
  (Unix socket only, `listen_addresses=''`, small `shared_buffers`), under the cap.
- `tdr::cpdb::testing::TempCluster` gives each test binary its own throwaway cluster in /tmp.
- Unit tests per module. Each API handler is tested against a fake repository
  and against the real Postgres repository.
- E2E (`tests/e2e/test_control_plane.py`): Postgres + PKI + tdrcp + two tdrd
  nodes on localhost. Covers registration, heartbeats, peers, a live limits
  change that tdrd applies, the stale → offline sweep (SIGSTOP a node), graceful
  offline, a control-plane outage with a job still running, reconnection,
  campaign lifecycle and audit, and wrong-role / no-cert rejection.

Phase 2 exit criteria: the two-machine First Demonstration (Description.md
§20) registration, health and peer exchange all work on localhost with two
nodes, and every e2e scenario above passes.

## 10. Status (2026-11-05)

Implemented as designed. Verified:

| Check | Result |
|---|---|
| C++ suites (12, `-Werror`) | all pass: tls, http, cpdb (real Postgres), cpapi (fake + real repo), agent (fake + real HTTPS), plus Phase 1 |
| ASan/UBSan (tls, http, cpdb, cpapi, agent, jobs) | clean |
| e2e | 29 pass (10 control-plane + 19 Phase 1) |
| SDK | 33 pass |

Behaviour notes from implementation:
- Server certs can't act as API clients. Their EKU fails the TLS handshake, and the API would also answer 403.
- `created_by` on a campaign is the admin CN. Audit actors are `admin:<cn>` / `node:<cn>`.
- Event reasons: `registered`, `heartbeat` (revived), `heartbeat stale`, `heartbeat timeout`, or the reason given in the offline request (`shutdown` from tdrd).
- tdrd checks at startup that its node cert CN equals `node.name`. A mismatch is fatal.
- On SIGTERM, tdrd waits up to 2 s for `/offline`. A second signal exits at once.

Known limitations (later phases):
- No certificate revocation (CRL). Rotate certs instead.
- tdrcp uses one Postgres connection on the io thread. A query is retried once after a reconnect, and a failed COMMIT is not retried.
- Heartbeats re-send the addresses detected at startup. Address changes need a tdrd restart.
- Over `max_connections` the HTTPS server just closes the connection (no 503).
