"""Phase 2 end-to-end: Postgres + private CA + tdrcp + tdrd nodes on localhost.

Run:  tools/e2e.sh -k control_plane
Covers docs/design/phase2.md §9: registration, heartbeats, peers, live limits,
stale -> offline sweep, graceful offline, control-plane outage with a running
job, reconnection, policy cache, campaigns + audit, and mTLS/role rejection.
"""
import http.client
import json
import os
import shutil
import signal
import ssl
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BUILD = Path(os.environ.get("TDR_BUILD", ROOT / "build/dev"))
TDRD = BUILD / "runtime/daemon/tdrd"
TDRCTL = BUILD / "runtime/ctl/tdrctl"
TDRCP = BUILD / "control/server/tdrcp"


def pg_bin() -> Path:
    if os.environ.get("PG_BIN"):
        return Path(os.environ["PG_BIN"])
    p = Path("/usr/lib/postgresql/16/bin")
    if (p / "initdb").exists():
        return p
    return Path(subprocess.run(["pg_config", "--bindir"], capture_output=True, text=True, check=True).stdout.strip())


def wait_for(pred, timeout=15.0, interval=0.1, what="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = pred()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what} (last={last!r})")


def ctl(*args, check=True, env=None):
    r = subprocess.run([str(TDRCTL), *map(str, args)], capture_output=True, text=True, timeout=60, env=env)
    if check and r.returncode != 0:
        raise AssertionError(f"tdrctl {' '.join(map(str, args))} -> {r.returncode}\n{r.stdout}\n{r.stderr}")
    return r


# ---- shared infrastructure (one Postgres cluster + CA per test module) -------------

class Postgres:
    def __init__(self, base: Path):
        self.dir = base / "pg"
        self.bin = pg_bin()
        subprocess.run([self.bin / "initdb", "-D", self.dir, "-U", os.environ.get("USER", "fireforge"),
                        "--auth=trust", "-E", "UTF8", "--no-sync"], check=True, capture_output=True)
        with open(self.dir / "postgresql.conf", "a") as f:
            f.write(f"listen_addresses = ''\nunix_socket_directories = '{self.dir}'\nport = 5432\n"
                    "shared_buffers = 16MB\nfsync = off\nsynchronous_commit = off\nmax_connections = 40\n")
        subprocess.run([self.bin / "pg_ctl", "-D", self.dir, "-l", self.dir / "server.log", "-w", "start"],
                       check=True, capture_output=True)
        self.n = 0

    def new_db(self) -> str:
        self.n += 1
        name = f"cp{self.n}"
        subprocess.run([self.bin / "createdb", "-h", self.dir, "-p", "5432", name], check=True, capture_output=True)
        return f"host={self.dir} port=5432 dbname={name}"

    def stop(self):
        subprocess.run([self.bin / "pg_ctl", "-D", self.dir, "-m", "immediate", "-w", "stop"], capture_output=True)


class Pki:
    def __init__(self, base: Path):
        self.dir = base / "pki"
        ctl("pki", "init", self.dir)
        ctl("pki", "issue", self.dir, "--role", "server", "tdrcp", "--ip", "127.0.0.1", "--dns", "localhost")
        for n in ("node-a", "node-b", "node-c"):
            ctl("pki", "issue", self.dir, "--role", "node", n)
        ctl("pki", "issue", self.dir, "--role", "admin", "alice")
        # A second, unrelated CA for rejection tests.
        self.other = base / "pki-other"
        ctl("pki", "init", self.other)
        ctl("pki", "issue", self.other, "--role", "admin", "mallory")
        ctl("pki", "issue", self.other, "--role", "node", "node-a")

    def creds(self, name, other=False):
        d = self.other if other else self.dir
        return d / "ca.crt", d / f"{name}.crt", d / f"{name}.key"


@pytest.fixture(scope="module")
def infra():
    for b in (TDRD, TDRCTL, TDRCP):
        if not b.exists():
            pytest.skip(f"{b} not built (run tools/e2e.sh)")
    base = Path(tempfile.mkdtemp(prefix="ffcp-", dir="/tmp"))
    pg = Postgres(base)
    try:
        yield base, pg, Pki(base)
    finally:
        pg.stop()
        if os.environ.get("FF_KEEP"):  # keep logs for debugging
            print(f"kept {base}")
        else:
            shutil.rmtree(base, ignore_errors=True)


# ---- per-test processes --------------------------------------------------------------

class ControlPlane:
    def __init__(self, base: Path, pki: Pki, conninfo: str):
        self.base = base
        self.pki = pki
        self.conninfo = conninfo
        self.port = 0
        self.proc = None
        self.conf = base / "tdrcp.conf"
        ca, crt, key = pki.creds("tdrcp")
        self.conf.write_text(
            "[server]\nlisten_address = 127.0.0.1\n"
            f"[tls]\nca_file = {ca}\ncert_file = {crt}\nkey_file = {key}\n"
            f"[database]\nconninfo = {conninfo}\n"
            "[health]\nheartbeat_interval = 1\nstale_after = 3\noffline_after = 6\n")

    def start(self):
        pf = self.base / "tdrcp.port"
        pf.unlink(missing_ok=True)
        self.log = open(self.base / "tdrcp.log", "ab")
        self.proc = subprocess.Popen([TDRCP, "--config", self.conf, "--port", str(self.port), "--port-file", pf,
                                      "--log-level", "debug"], stdout=self.log, stderr=self.log)

        def ready():
            if self.proc.poll() is not None:
                raise RuntimeError(f"tdrcp exited {self.proc.returncode}: {(self.base / 'tdrcp.log').read_text()[-3000:]}")
            return pf.exists() and pf.read_text().strip()

        self.port = int(wait_for(ready, what="tdrcp port file"))
        return self

    def stop(self, sig=signal.SIGTERM):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(10)

    @property
    def url(self):
        return f"https://127.0.0.1:{self.port}"

    def admin(self, *args, check=True, who="alice", other=False):
        ca, crt, key = self.pki.creds(who, other)
        if other:
            ca = self.pki.dir / "ca.crt"  # trust the real server; present a foreign client cert
        return ctl("cp", "--url", self.url, "--ca", ca, "--cert", crt, "--key", key, *args, check=check)

    def admin_json(self, *args):
        return json.loads(self.admin(*args, "--json").stdout)

    def nodes(self):
        return {n["name"]: n for n in self.admin_json("nodes").get("nodes", [])}

    def raw(self, method, path, body=None, cert=None, other_ca=False):
        """Direct HTTPS request; cert = (crt, key) or None for no client certificate."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(self.pki.dir / "ca.crt")
        if cert:
            ctx.load_cert_chain(cert[0], cert[1])
        conn = http.client.HTTPSConnection("127.0.0.1", self.port, context=ctx, timeout=10)
        try:
            conn.request(method, path, body=json.dumps(body) if body is not None else None,
                         headers={"Content-Type": "application/json"})
            r = conn.getresponse()
            return r.status, r.read().decode()
        finally:
            conn.close()


class Node:
    def __init__(self, base: Path, pki: Pki, name: str, cp: ControlPlane, listen_port: int):
        self.name = name
        self.dir = base / name
        self.state = self.dir / "st"
        self.state.mkdir(parents=True, exist_ok=True)
        self.sock = self.state / "run/ctl.sock"
        self.conf = self.dir / "tdr.conf"
        self.cp = cp
        self.listen_port = listen_port
        self.pki = pki
        self.proc = None
        self.write_conf()

    def write_conf(self, cpus=4, jobs=4):
        ca, crt, key = self.pki.creds(self.name)
        self.conf.write_text(
            f"[control]\naddress = 127.0.0.1\nport = {self.cp.port}\n"
            f"[node]\nname = {self.name}\nadvertise_address = 127.0.0.1\n"
            f"[network]\nlisten_port = {self.listen_port}\ntailscale = off\n"
            f"[resources]\nmax_cpu_cores = {cpus}\nmax_jobs = {jobs}\nmax_memory_mb = 4096\ncgroups = off\n"
            f"[tls]\nca_file = {ca}\ncert_file = {crt}\nkey_file = {key}\n")

    def start(self, wait_connected=True):
        self.write_conf()
        self.log = open(self.dir / "tdrd.log", "ab")
        self.proc = subprocess.Popen([TDRD, "--config", self.conf, "--state-dir", self.state, "--log-level", "debug"],
                                     stdout=self.log, stderr=self.log)

        def up():
            if self.proc.poll() is not None:
                raise RuntimeError(f"tdrd exited {self.proc.returncode}: {(self.dir / 'tdrd.log').read_text()[-3000:]}")
            return self.sock.exists() and self.ctl("node", check=False).returncode == 0

        wait_for(up, what=f"{self.name} socket")
        if wait_connected:
            wait_for(lambda: self.status().get("control_state") == "connected", what=f"{self.name} connected")
        return self

    def stop(self, sig=signal.SIGTERM, timeout=10):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(timeout)

    def ctl(self, *args, check=True):
        return ctl("--socket", self.sock, *args, check=check)

    def status(self):
        return json.loads(self.ctl("node", "--json").stdout)

    def peers(self):
        return json.loads(self.ctl("peers", "--json").stdout)


@pytest.fixture
def env(infra):
    base, pg, pki = infra
    tdir = Path(tempfile.mkdtemp(prefix="t-", dir=base))
    cp = ControlPlane(tdir, pki, pg.new_db()).start()
    nodes = []
    ports = iter(range(17401, 17500))

    def node(name, start=True, **kw):
        n = Node(tdir, pki, name, cp, next(ports))
        nodes.append(n)
        return n.start(**kw) if start else n

    try:
        yield cp, node, pki
    finally:
        for n in nodes:
            if n.proc and n.proc.poll() is None:
                try:
                    os.kill(n.proc.pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            n.stop(signal.SIGKILL)
        cp.stop(signal.SIGKILL)


# ---- tests ---------------------------------------------------------------------------------

def test_registration_heartbeats_and_peers(env):
    cp, node, _ = env
    a = node("node-a")
    b = node("node-b")
    sa, sb = a.status(), b.status()
    assert sa["node_id"].startswith("ff-node-") and sa["node_id"] != sb["node_id"]
    assert sa["policy_version"] == "1"

    nodes = wait_for(lambda: (n := cp.nodes()) and all("last_heartbeat" in n[k] for k in ("node-a", "node-b")) and n,
                     what="heartbeats stored")
    na = nodes["node-a"]
    assert na["health"] == "ONLINE"
    assert na["node_id"] == sa["node_id"]
    assert {"type": "lan", "address": "127.0.0.1", "port": a.listen_port} in na["addresses"]
    assert "cpu" in na["capabilities"]
    assert na["configured"]["cpu_cores"] == 4
    assert na["last_heartbeat"]["capacity"]["cpu_cores"] == 4
    assert na["cert_fingerprint"] and len(na["cert_fingerprint"]) == 64

    # node-b registered second, so its first directory fetch already lists both.
    peers = wait_for(lambda: {p["name"] for p in b.peers().get("peers", [])} >= {"node-a", "node-b"} and b.peers(),
                     what="peer directory")
    assert int(peers["refreshed_unix_ms"]) > 0

    # Re-registration (restart) keeps the node id.
    a.stop()
    a.start()
    assert a.status()["node_id"] == sa["node_id"]
    assert len(cp.nodes()) == 2


def test_limits_applied_live(env):
    cp, node, _ = env
    a = node("node-a")
    nid = a.status()["node_id"]

    pol = json.loads(cp.admin("limits", nid, "--cpus", "1", "--jobs", "1", "--json").stdout)
    assert pol["version"] == "2" and pol["max_cpu_cores"] == 1 and pol["updated_by"] == "admin:alice"
    st = wait_for(lambda: (s := a.status())["policy_version"] == "2" and s, what="policy applied")
    assert st["capacity"]["cpu_cores"] == 1 and st["capacity"]["jobs"] == 1

    # Admission follows the new cap: one job runs, the next waits.
    j1 = a.ctl("submit", "--", "sleep", "30").stdout.strip()
    j2 = a.ctl("submit", "--", "true").stdout.strip()
    wait_for(lambda: json.loads(a.ctl("status", j1, "--json").stdout)["state"] == "RUNNING", what="j1 running")
    time.sleep(0.5)
    assert json.loads(a.ctl("status", j2, "--json").stdout)["state"] == "QUEUED"

    # Profile preset larger than the local cap: local tdr.conf caps still win (min).
    cp.admin("limits", nid, "--profile", "compute")
    st = wait_for(lambda: (s := a.status())["policy_version"] == "3" and s, what="profile applied")
    assert st["capacity"]["cpu_cores"] == 4 and st["capacity"]["jobs"] == 4
    assert a.ctl("wait", j2, "--timeout", "15", check=False).returncode == 0  # raised cap starts queued work
    a.ctl("cancel", j1)

    # The node record reflects the policy; lowering never kills running work.
    info = cp.admin_json("node", nid)
    assert info["policy"]["profile"] == "compute"
    audit = cp.admin_json("audit")["entries"]
    assert [e["action"] for e in audit[:2]] == ["node.limits", "node.limits"]


def test_stale_then_offline_then_back(env):
    cp, node, _ = env
    a = node("node-a")
    nid = a.status()["node_id"]
    os.kill(a.proc.pid, signal.SIGSTOP)
    try:
        wait_for(lambda: cp.nodes()["node-a"]["health"] == "STALE", timeout=10, what="STALE")
        wait_for(lambda: cp.nodes()["node-a"]["health"] == "OFFLINE", timeout=10, what="OFFLINE")
        assert cp.nodes()["node-a"]["offline_reason"] == "heartbeat timeout"
    finally:
        os.kill(a.proc.pid, signal.SIGCONT)
    wait_for(lambda: cp.nodes()["node-a"]["health"] == "ONLINE", timeout=15, what="back ONLINE")
    events = cp.admin_json("events", nid)["events"]
    seq = [(e.get("from", "NODE_HEALTH_UNSPECIFIED"), e["to"]) for e in reversed(events)]
    assert ("ONLINE", "STALE") in seq and ("STALE", "OFFLINE") in seq and ("OFFLINE", "ONLINE") in seq


def test_graceful_offline_on_shutdown(env):
    cp, node, _ = env
    a = node("node-a")
    t0 = time.monotonic()
    a.stop(signal.SIGTERM)
    assert time.monotonic() - t0 < 5
    n = cp.nodes()["node-a"]
    assert n["health"] == "OFFLINE" and n["offline_reason"] == "shutdown"


def test_admin_offline(env):
    cp, node, _ = env
    a = node("node-a")
    nid = a.status()["node_id"]
    out = cp.admin_json("offline", nid, "--reason", "maintenance")
    assert out["health"] == "OFFLINE" and out["offline_reason"] == "maintenance"
    # The node is still alive and heartbeating, so it comes back.
    wait_for(lambda: cp.nodes()["node-a"]["health"] == "ONLINE", what="heartbeat revives node")


def test_control_plane_outage_does_not_affect_jobs(env):
    cp, node, _ = env
    a = node("node-a")
    nid = a.status()["node_id"]
    job = a.ctl("submit", "--", "sh", "-c", "sleep 4; echo done > $FIREFORGE_OUTPUT_DIR/out").stdout.strip()

    cp.stop(signal.SIGKILL)
    wait_for(lambda: a.status()["control_state"] == "disconnected", timeout=10, what="disconnected")
    assert a.ctl("wait", job, "--timeout", "20", check=False).returncode == 0  # finished while CP was down
    # Local submissions still work.
    assert a.ctl("wait", a.ctl("submit", "--", "true").stdout.strip(), "--timeout", "10", check=False).returncode == 0

    cp.start()  # same port and database
    wait_for(lambda: a.status()["control_state"] == "connected", timeout=70, what="reconnected")
    assert a.status()["node_id"] == nid
    assert cp.nodes()["node-a"]["health"] == "ONLINE"


def test_policy_cache_used_when_control_plane_down(env):
    cp, node, _ = env
    a = node("node-a")
    cp.admin("limits", a.status()["node_id"], "--cpus", "2", "--jobs", "2")
    wait_for(lambda: a.status()["policy_version"] == "2", what="policy v2")
    a.stop()
    cp.stop()
    a.start(wait_connected=False)
    st = a.status()
    assert st["control_state"] in ("registering", "disconnected")
    assert st["capacity"]["cpu_cores"] == 2 and st["capacity"]["jobs"] == 2


def test_campaign_lifecycle_and_audit(env):
    cp, _, _ = env
    c = cp.admin_json("campaign-create", "thermo-1", "--domain", "thermoelectric",
                      "--description", "first screen", "--params", '{"max_atoms": 40}')
    cid = c["campaign_id"]
    assert cid.startswith("camp-") and c["state"] == "DRAFT" and c["created_by"] == "alice"
    assert json.loads(c["parameters_json"]) == {"max_atoms": 40}

    assert "conflict" in cp.admin("campaign-create", "thermo-1", check=False).stderr
    assert cp.admin("campaign-set", cid, "--state", "COMPLETED", check=False).returncode != 0  # DRAFT -> COMPLETED

    for s in ("ACTIVE", "PAUSED", "ACTIVE", "COMPLETED", "ARCHIVED"):
        assert cp.admin_json("campaign-set", cid, "--state", s)["state"] == s
    assert cp.admin("campaign-set", cid, "--state", "ACTIVE", check=False).returncode != 0

    cp.admin_json("campaign-create", "thermo-2")
    assert [x["name"] for x in cp.admin_json("campaigns", "--state", "DRAFT")["campaigns"]] == ["thermo-2"]
    assert len(cp.admin_json("campaigns")["campaigns"]) == 2
    assert cp.admin_json("campaign", cid)["description"] == "first screen"

    actions = [e["action"] for e in cp.admin_json("audit")["entries"]]
    assert actions.count("campaign.update") == 5 and actions.count("campaign.create") == 2


def test_auth_rules(env):
    cp, node, pki = env
    a = node("node-a")
    nid = a.status()["node_id"]
    _, a_crt, a_key = pki.creds("node-a")
    _, b_crt, b_key = pki.creds("node-b")
    _, s_crt, s_key = pki.creds("tdrcp")

    # No client certificate: rejected in the TLS handshake.
    with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
        cp.raw("GET", "/api/v1/nodes")
    # Client cert from a foreign CA: rejected.
    _, m_crt, m_key = pki.creds("mallory", other=True)
    with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
        cp.raw("GET", "/api/v1/nodes", cert=(m_crt, m_key))
    assert cp.admin("nodes", who="mallory", other=True, check=False).returncode != 0

    # Node certs may read the directory but not administer.
    assert cp.raw("GET", "/api/v1/nodes", cert=(a_crt, a_key))[0] == 200
    assert cp.raw("PATCH", f"/api/v1/nodes/{nid}/limits", {"max_jobs": 1}, cert=(a_crt, a_key))[0] == 403
    assert cp.raw("POST", "/api/v1/campaigns", {"name": "x"}, cert=(a_crt, a_key))[0] == 403
    # A node can't register under another name or heartbeat for another node.
    assert cp.raw("POST", "/api/v1/nodes/register", {"name": "node-a"}, cert=(b_crt, b_key))[0] == 403
    assert cp.raw("POST", "/api/v1/nodes/heartbeat", {"node_id": nid}, cert=(b_crt, b_key))[0] == 403
    assert cp.raw("POST", f"/api/v1/nodes/{nid}/offline", {}, cert=(b_crt, b_key))[0] == 403
    # Server certificates are not API clients: their EKU (serverAuth only) fails
    # the handshake before any handler runs (the API would also answer 403).
    with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
        cp.raw("GET", "/api/v1/nodes", cert=(s_crt, s_key))
    # Errors are JSON.
    st, body = cp.raw("GET", "/api/v1/nodes/ff-node-nope", cert=(a_crt, a_key))
    assert st == 404 and json.loads(body)["code"] == "not_found"
    assert cp.nodes()["node-a"]["health"] == "ONLINE"


def test_tdrd_refuses_mismatched_certificate(env, infra):
    _, node, pki = env
    n = node("node-c", start=False)
    n.write_conf()
    # Point node-c at node-a's certificate.
    text = n.conf.read_text()
    ca, crt, key = pki.creds("node-a")
    n.conf.write_text(text.replace(str(pki.dir / "node-c.crt"), str(crt)).replace(str(pki.dir / "node-c.key"), str(key)))
    r = subprocess.run([TDRD, "--config", n.conf, "--state-dir", n.state], capture_output=True, text=True, timeout=20)
    assert r.returncode != 0 and "node certificate is for 'node-a'" in r.stderr
