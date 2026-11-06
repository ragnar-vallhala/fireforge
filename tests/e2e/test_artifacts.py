"""Phase 5 end-to-end: direct peer artifact transfer, placement after
locality_wait, resumable fetches, store GC (docs/design/phase5.md §9).
"""
import hashlib
import json
import os
import signal
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import pytest

from test_control_plane import TDRCTL, infra, wait_for  # noqa: F401  (infra is a fixture)
from test_remote_jobs import TERMINAL, JobCP, JobNode

MiB = 1024 * 1024


class ArtCP(JobCP):
    def __init__(self, base, pki, conninfo, locality_wait=1):
        super().__init__(base, pki, conninfo)
        with open(self.conf, "a") as f:
            f.write(f"locality_wait = {locality_wait}\n")  # appended to the [jobs] section JobCP wrote

    def holders(self, art):
        r = self.admin("artifact", art, "--json", check=False)
        return json.loads(r.stdout).get("node_ids", []) if r.returncode == 0 else []


class ArtNode(JobNode):
    def __init__(self, *a, max_disk_mb=0, mbps=0, gc_interval=3600, **kw):
        self.max_disk_mb, self.mbps, self.gc_interval = max_disk_mb, mbps, gc_interval
        super().__init__(*a, **kw)

    def write_conf(self, cpus=None, jobs=None):
        super().write_conf()
        text = self.conf.read_text()
        text = text.replace("tailscale = off\n", f"tailscale = off\nmax_transfer_mbps = {self.mbps}\n")
        text = text.replace("cgroups = off\n", f"cgroups = off\nmax_disk_mb = {self.max_disk_mb}\n")
        text += f"[artifacts]\ngc_interval = {self.gc_interval}\ngc_min_age = 0\n"
        self.conf.write_text(text)

    def put(self, path):
        return self.ctl("artifact", "put", str(path)).stdout.strip()

    def ls(self):
        return {a["ref"]["artifact_id"]: a for a in json.loads(self.ctl("artifact", "ls", "--json").stdout).get("artifacts", [])}

    def part(self, art):
        return self.state / "artifacts" / "partial" / (art.rsplit("/", 1)[1] + ".part")


@pytest.fixture
def art_cluster(infra):
    base, pg, pki = infra
    tdir = Path(tempfile.mkdtemp(prefix="a-", dir=base))
    cps, nodes = [], []
    ports = iter(range(17800, 17900))

    def make_cp(**kw):
        cp = ArtCP(tdir, pki, pg.new_db(), **kw).start()
        cps.append(cp)
        return cp

    def node(cp, name, start=True, **kw):
        n = ArtNode(tdir, pki, name, cp, next(ports), **kw)
        nodes.append(n)
        return n.start() if start else n

    try:
        yield tdir, make_cp, node
    finally:
        for n in nodes:
            n.stop(signal.SIGKILL)
        for cp in cps:
            cp.stop(signal.SIGKILL)


def blob(dirpath, name, size, seed):
    p = Path(dirpath) / name
    chunk = hashlib.sha256(seed.encode()).digest() * (64 * 1024 // 32)  # 64 KiB pattern
    with open(p, "wb") as f:
        left = size
        i = 0
        while left > 0:
            piece = bytes([i % 251]) + chunk[1:]
            f.write(piece[:left])
            left -= len(piece[:left])
            i += 1
    return p


def art_id(path):
    return "artifact://sha256/" + hashlib.sha256(Path(path).read_bytes()).hexdigest()


def meshed(*nodes, timeout=30):
    for n in nodes:
        others = [m.name for m in nodes if m is not n]
        wait_for(lambda n=n, o=others: all(
            {c["name"]: c for c in n.peers().get("connections", [])}.get(x, {}).get("state") == "CONNECTED" for x in o),
            timeout=timeout, what=f"{n.name} meshed")


def announced(cp, art, node_ids, timeout=20):
    wait_for(lambda: set(node_ids) <= set(cp.holders(art)), timeout=timeout, what=f"{art} on {node_ids}")


# ---- tests --------------------------------------------------------------------------------

def test_job_input_is_fetched_from_a_peer(art_cluster):
    tdir, make_cp, node = art_cluster
    cp = make_cp(locality_wait=1)
    a = node(cp, "node-a", remote_jobs="off")   # holds the input but never runs jobs
    b = node(cp, "node-b")
    meshed(a, b)
    src = blob(tdir, "in.bin", 3 * MiB + 17, "input")
    art = a.put(src)
    assert art == art_id(src)
    announced(cp, art, [a.node_id])

    job = cp.submit("sh", "-c", "sha256sum $FIREFORGE_INPUT_DIR/* | cut -c1-64 > $FIREFORGE_OUTPUT_DIR/sum", input=art)
    done = cp.wait_state(job, TERMINAL, timeout=40)
    assert done["state"] == "COMPLETED", done
    assert done["node_id"] == b.node_id
    assert art in b.ls()                                   # fetched into b's store
    announced(cp, art, [a.node_id, b.node_id])

    # The job's output lives on b; `artifact get` on a pulls it over a data link.
    out = done["outputs"][0]["artifact_id"]
    dest = tdir / "sum.out"
    r = a.ctl("artifact", "get", out, "--out", str(dest))
    assert r.stdout.startswith("fetched ")
    assert dest.read_text().strip() == art.rsplit("/", 1)[1]


def test_artifact_get_on_third_node_and_empty_artifact(art_cluster):
    tdir, make_cp, node = art_cluster
    cp = make_cp()
    a, c = node(cp, "node-a"), node(cp, "node-c")
    meshed(a, c)
    big = blob(tdir, "big.bin", 20 * MiB + 3, "big")
    empty = tdir / "empty.bin"
    empty.write_bytes(b"")
    ids = [a.put(big), a.put(empty)]
    for art in ids:
        announced(cp, art, [a.node_id])
    for art, src in zip(ids, [big, empty]):
        dest = tdir / ("got-" + src.name)
        assert c.ctl("artifact", "get", art, "--out", str(dest)).stdout.startswith("fetched ")
        assert dest.read_bytes() == src.read_bytes()
        # Second get is local.
        assert c.ctl("artifact", "get", art).stdout.startswith("present ")
    assert not any(p.suffix == ".part" for p in (c.state / "artifacts" / "partial").glob("*"))
    # Unknown artifact: clean error.
    r = c.ctl("artifact", "get", "artifact://sha256/" + "ab" * 32, check=False)
    assert r.returncode != 0 and "no holder" in r.stderr


def test_interrupted_transfer_resumes_from_another_holder(art_cluster):
    tdir, make_cp, node = art_cluster
    cp = make_cp()
    # Both sources serve at 32 Mbit/s (4 MB/s): 32 MiB takes ~8 s from one source.
    a = node(cp, "node-a", mbps=32)
    b = node(cp, "node-b", mbps=32)
    c = node(cp, "node-c")
    meshed(a, b, c)
    src = blob(tdir, "resume.bin", 32 * MiB, "resume")
    art = a.put(src)
    announced(cp, art, [a.node_id])

    get = subprocess.Popen([TDRCTL, "--socket", str(c.sock), "artifact", "get", art],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    part = c.part(art)
    wait_for(lambda: part.exists() and part.stat().st_size >= 12 * MiB, timeout=30, what="half the transfer")
    a.stop(signal.SIGKILL)                       # the only holder dies mid-transfer
    _, err = get.communicate(timeout=60)
    assert get.returncode != 0, err
    held = part.stat().st_size
    assert 12 * MiB <= held < 32 * MiB

    assert b.put(src) == art                     # a second holder appears
    announced(cp, art, [b.node_id])
    t0 = time.monotonic()
    r = c.ctl("artifact", "get", art, "--out", str(tdir / "resumed.bin"))
    took = time.monotonic() - t0
    assert r.stdout.startswith("fetched ")
    assert (tdir / "resumed.bin").read_bytes() == src.read_bytes()
    # Only the rest was sent: (32 - held) MiB at 4 MB/s, plus slack; a restart from 0 needs >= 8 s.
    assert took < (32 * MiB - held) / 4e6 + 3.5, (took, held)
    assert not part.exists()


def test_gc_evicts_replicated_copies_but_keeps_last_copy_and_pins(art_cluster):
    tdir, make_cp, node = art_cluster
    cp = make_cp()
    a = node(cp, "node-a", max_disk_mb=4)
    b = node(cp, "node-b")
    size = 1280 * 1024   # 1.25 MiB; three of them = 3.75 MiB > 90% of 4 MiB
    only_a = blob(tdir, "only-a.bin", size, "only-a")
    pinned = blob(tdir, "pinned.bin", size, "pinned")
    shared = blob(tdir, "shared.bin", size, "shared")

    x_only = a.put(only_a)
    time.sleep(1.1)
    x_pin = a.put(pinned)
    b.put(pinned)
    cp.admin("artifact-pin", x_pin)
    time.sleep(1.1)
    x_shared = a.put(shared)                     # newest, but the only evictable one
    b.put(shared)
    announced(cp, x_only, [a.node_id])
    announced(cp, x_pin, [a.node_id, b.node_id])
    announced(cp, x_shared, [a.node_id, b.node_id])
    assert cp.admin_json("artifact", x_pin)["pinned"] is True

    rep = json.loads(a.ctl("artifact", "gc", "--json").stdout)
    assert rep.get("evicted") == [x_shared], rep
    assert rep.get("kept_last_copy") == 1 and rep.get("kept_pinned") == 1, rep
    held = a.ls()
    assert x_shared not in held and x_only in held and x_pin in held
    assert cp.holders(x_shared) == [b.node_id]
    assert sorted(cp.holders(x_only)) == [a.node_id]

    # Still over the low mark, but nothing more may go: the last copy and the pin stay.
    rep = json.loads(a.ctl("artifact", "gc", "--json").stdout)
    assert not rep.get("evicted"), rep

    # Manual rm of the last copy needs --force.
    r = a.ctl("artifact", "rm", x_only, check=False)
    assert r.returncode != 0 and "last copy" in r.stderr
    a.ctl("artifact", "rm", x_only, "--force")
    assert x_only not in a.ls()
    assert cp.holders(x_only) == []


def test_two_nodes_evicting_the_last_two_copies_keep_one(art_cluster):
    tdir, make_cp, node = art_cluster
    cp = make_cp()
    a = node(cp, "node-a", max_disk_mb=4)
    b = node(cp, "node-b", max_disk_mb=4)
    src = blob(tdir, "both.bin", 3800 * 1024, "both")   # > 90% of 4 MiB on each node
    art = a.put(src)
    b.put(src)
    announced(cp, art, [a.node_id, b.node_id])

    reports = {}

    def gc(n):
        reports[n.name] = json.loads(n.ctl("artifact", "gc", "--json").stdout)

    threads = [threading.Thread(target=gc, args=(n,)) for n in (a, b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    evicted = [name for name, r in reports.items() if r.get("evicted")]
    assert len(evicted) == 1, reports
    survivors = [n for n in (a, b) if art in n.ls()]
    assert len(survivors) == 1
    assert cp.holders(art) == [survivors[0].node_id]
