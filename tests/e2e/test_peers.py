"""Phase 3 end-to-end: direct peer links between tdrd nodes (docs/design/phase3.md §7).

Each node advertises an unreachable "lan" path (port 1, refused) and a working
"tailscale" path, so every link proves the fallback order. Ping 1 s / timeout 3 s.
"""
import json
import signal
import socket
import ssl
import time

import pytest

from test_control_plane import ControlPlane, Node, infra, wait_for  # noqa: F401  (infra is a fixture)


class PeerNode(Node):
    def write_conf(self, cpus=2, jobs=2):
        ca, crt, key = self.pki.creds(self.name)
        self.conf.write_text(
            f"[control]\naddress = 127.0.0.1\nport = {self.cp.port}\n"
            f"[node]\nname = {self.name}\n"
            f"advertise_address = lan=127.0.0.1:1, tailscale=127.0.0.1:{self.listen_port}\n"
            f"[network]\nlisten_address = 127.0.0.1\nlisten_port = {self.listen_port}\n"
            "peer_ping_interval = 1\npeer_ping_timeout = 3\n"
            f"[resources]\nmax_cpu_cores = {cpus}\nmax_jobs = {jobs}\nmax_memory_mb = 2048\ncgroups = off\n"
            f"[tls]\nca_file = {ca}\ncert_file = {crt}\nkey_file = {key}\n")

    def links(self):
        return {c["name"]: c for c in self.peers().get("connections", [])}


@pytest.fixture
def mesh(infra):
    import tempfile
    from pathlib import Path
    base, pg, pki = infra
    tdir = Path(tempfile.mkdtemp(prefix="p-", dir=base))
    cp = ControlPlane(tdir, pki, pg.new_db()).start()
    ports = iter(range(17600, 17700))
    nodes = {}

    def add(name, start=True):
        n = PeerNode(tdir, pki, name, cp, next(ports))
        nodes[name] = n
        return n.start() if start else n

    try:
        yield cp, add, nodes, pki
    finally:
        for n in nodes.values():
            if n.proc and n.proc.poll() is None:
                try:
                    n.proc.send_signal(signal.SIGCONT)
                except ProcessLookupError:
                    pass
            n.stop(signal.SIGKILL)
        cp.stop(signal.SIGKILL)


def connected(node, names):
    links = node.links()
    return all(links.get(n, {}).get("state") == "CONNECTED" for n in names) and links


def full_mesh(nodes, timeout=25):
    for n in nodes.values():
        others = [m for m in nodes if m != n.name]
        wait_for(lambda n=n, o=others: connected(n, o), timeout=timeout, what=f"{n.name} connected to {others}")


def test_mesh_forms_over_fallback_path(mesh):
    cp, add, nodes, _ = mesh
    for name in ("node-a", "node-b", "node-c"):
        add(name)
    full_mesh(nodes)

    ids = {n.name: n.status()["node_id"] for n in nodes.values()}
    for n in nodes.values():  # first ping goes out ping_interval after connect
        wait_for(lambda n=n: all(l.get("rtt_ms", 0) > 0 for l in n.links().values()), timeout=10,
                 what=f"{n.name} RTT")
    for n in nodes.values():
        for other, link in n.links().items():
            if link.get("outbound"):
                # The dialer tried lan (port 1, refused) first, then fell back.
                assert link["path_type"] == "tailscale", link
                assert link["address"] == f"127.0.0.1:{nodes[other].listen_port}"
                assert "lan 127.0.0.1:1" in link["last_error"], link
            assert link["rtt_ms"] > 0
            assert int(link["connected_since_unix_ms"]) > 0
            # Exactly one link per pair: dialed by one side, accepted by the other.
            # (The lower-id rule only decides when both sides dialed at once;
            # the manager unit tests cover that race.)
            back = nodes[other].links()[n.name]
            assert link.get("outbound", False) != back.get("outbound", False)

    # The control plane sees each node's view through heartbeats.
    def cp_view():
        info = cp.admin_json("node", ids["node-a"])
        peers = {p["name"]: p for p in info.get("last_heartbeat", {}).get("peers", [])}
        return peers.get("node-b", {}).get("state") == "CONNECTED" and peers.get("node-c", {}).get("state") == "CONNECTED"
    wait_for(cp_view, timeout=10, what="peer reachability in heartbeat")
    assert "peer:       node-b CONNECTED via " in cp.admin("node", ids["node-a"]).stdout

    out = nodes["node-c"].ctl("peers").stdout  # node-c joined last, so it dialed both others
    assert "peer listener on port" in out and "CONNECTED" in out and "tailscale" in out


def test_graceful_restart_reconnects(mesh):
    _, add, nodes, _ = mesh
    a, b = add("node-a"), add("node-b")
    full_mesh(nodes)
    b.stop(signal.SIGTERM)
    wait_for(lambda: a.links()["node-b"]["state"] != "CONNECTED", timeout=10, what="a notices b left")
    assert "goodbye" in a.links()["node-b"]["last_error"] or a.links()["node-b"]["last_error"]
    b.start()
    full_mesh(nodes, timeout=30)


def test_paused_node_times_out_and_recovers(mesh):
    _, add, nodes, _ = mesh
    a, b = add("node-a"), add("node-b")
    full_mesh(nodes)
    b.proc.send_signal(signal.SIGSTOP)
    try:
        wait_for(lambda: a.links()["node-b"]["state"] != "CONNECTED", timeout=10, what="ping timeout")
        assert "ping timeout" in a.links()["node-b"]["last_error"] or a.links()["node-b"]["last_error"]
    finally:
        b.proc.send_signal(signal.SIGCONT)
    full_mesh(nodes, timeout=45)


def _tls_connect(port, ca, cert=None):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.load_verify_locations(ca)
    if cert:
        ctx.load_cert_chain(*cert)
    with socket.create_connection(("127.0.0.1", port), timeout=5) as raw:
        with ctx.wrap_socket(raw) as s:
            s.sendall(b"\x00" * 48)  # force the server to act on the session
            return s.recv(1)         # b"" when the server closes us


def test_peer_port_rejects_non_node_and_foreign_certs(mesh):
    _, add, nodes, pki = mesh
    a = add("node-a")
    port = a.listen_port
    ca = pki.dir / "ca.crt"
    _, adm_crt, adm_key = pki.creds("alice")
    _, f_crt, f_key = pki.creds("node-a", other=True)
    # No client cert: handshake refused.
    with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
        _tls_connect(port, ca)
    # Admin cert: TLS may complete (same CA), but the node closes the link at once.
    try:
        assert _tls_connect(port, ca, (adm_crt, adm_key)) == b""
    except (ssl.SSLError, ConnectionError, OSError):
        pass
    # Node cert from a foreign CA: refused.
    with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
        _tls_connect(port, ca, (f_crt, f_key))
    assert a.status()["control_state"] == "connected"  # still healthy
