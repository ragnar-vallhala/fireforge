"""Phase 7 end-to-end: multi-node MPI gangs with real Open MPI over loopback
(docs/design/phase7.md §10).
"""
import json
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from test_artifacts import ArtCP, ArtNode, meshed
from test_control_plane import infra, wait_for  # noqa: F401  (infra is a fixture)
from test_tdrd import Daemon

pytestmark = pytest.mark.skipif(shutil.which("mpicc") is None or shutil.which("mpirun") is None,
                                reason="Open MPI (mpicc/mpirun) not installed")

PROGRAM = r"""
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, size, sum = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    double secs = argc > 1 ? atof(argv[1]) : 0;
    for (double t = 0; t < secs; t += 0.2) { usleep(200000); MPI_Barrier(MPI_COMM_WORLD); }
    MPI_Allreduce(&rank, &sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    char cwd[1024] = "";
    if (!getcwd(cwd, sizeof cwd)) strcpy(cwd, "?");
    char line[1100];
    snprintf(line, sizeof line, "%s", cwd);
    char* all = rank == 0 ? malloc((size_t)size * sizeof line) : NULL;
    MPI_Gather(line, sizeof line, MPI_CHAR, all, sizeof line, MPI_CHAR, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        const char* out = getenv("FIREFORGE_OUTPUT_DIR");
        char path[1200];
        snprintf(path, sizeof path, "%s/ranks.json", out ? out : "outputs");
        FILE* f = fopen(path, "w");
        fprintf(f, "{\"size\": %d, \"sum\": %d, \"cwd\": [", size, sum);
        for (int i = 0; i < size; ++i) fprintf(f, "%s\"%s\"", i ? ", " : "", all + (size_t)i * sizeof line);
        fprintf(f, "]}\n");
        fclose(f);
    }
    MPI_Finalize();
    return 0;
}
"""

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "EXPIRED", "VALIDATION_FAILED"}


@pytest.fixture(scope="module")
def mpi_program():
    d = Path(tempfile.mkdtemp(prefix="ffmpi-"))
    (d / "ranks.c").write_text(PROGRAM)
    subprocess.run(["mpicc", "-O1", "-o", str(d / "ranks"), str(d / "ranks.c")], check=True)
    yield str(d / "ranks")
    shutil.rmtree(d, ignore_errors=True)


class MpiNode(ArtNode):
    def write_conf(self, cpus=None, jobs=None):
        super().write_conf()
        with open(self.conf, "a") as f:
            # All "nodes" share one host: Open MPI shared memory (vader) between them crashes.
            f.write("[mpi]\nif_include = lo\nmca = btl=^vader\nlaunch_timeout = 30\n")

    def processes(self):
        """Pids of processes whose cwd is inside this node's state dir (job processes)."""
        out = []
        for p in Path("/proc").iterdir():
            if not p.name.isdigit():
                continue
            try:
                if str(Path(p / "cwd").resolve()).startswith(str(self.state)):
                    out.append(int(p.name))
            except OSError:
                pass
        return out


class MpiCP(ArtCP):
    def __init__(self, base, pki, conninfo, **kw):
        super().__init__(base, pki, conninfo, locality_wait=0, **kw)
        with open(self.conf, "a") as f:
            f.write("[placement]\ninterval = 1\n[gangs]\ndrain_timeout = 120\nready_window = 5\n")


@pytest.fixture
def mpi_cluster(infra):
    base, pg, pki = infra
    tdir = Path(tempfile.mkdtemp(prefix="m-", dir=base))
    cps, nodes = [], []
    ports = iter(range(18000, 18100))

    def make_cp(**kw):
        cp = MpiCP(tdir, pki, pg.new_db(), **kw).start()
        cps.append(cp)
        return cp

    def node(cp, name, start=True, **kw):
        n = MpiNode(tdir, pki, name, cp, next(ports), **kw)
        nodes.append(n)
        return n.start() if start else n

    try:
        yield tdir, make_cp, node
    finally:
        for n in nodes:
            n.stop(signal.SIGKILL)
        for cp in cps:
            cp.stop(signal.SIGKILL)


def ranks_output(cp, node, job, tdir):
    art = next(o["artifact_id"] for o in job["outputs"] if o.get("name", "").endswith("ranks.json"))
    dest = tdir / f"{job['job_id']}-ranks.json"
    node.ctl("artifact", "get", art, "--out", str(dest))
    return json.loads(dest.read_text())


def gang_nodes(job):
    return {m["node_id"]: m.get("ranks", 0) for m in job.get("gang", [])}


# ---- tests --------------------------------------------------------------------------------

def test_six_ranks_across_two_nodes(mpi_cluster, mpi_program):
    tdir, make_cp, node = mpi_cluster
    cp = make_cp()
    a, b = node(cp, "node-a", cpus=4, jobs=4), node(cp, "node-b", cpus=2, jobs=4)
    meshed(a, b)
    job = cp.submit(mpi_program, ranks=6)
    done = cp.wait_state(job, TERMINAL, timeout=90)
    assert done["state"] == "COMPLETED", json.dumps(cp.job(job), indent=1)
    assert gang_nodes(done) == {a.node_id: 4, b.node_id: 2}
    assert done["node_id"] == a.node_id                      # head: the bigger node
    res = ranks_output(cp, a, done, tdir)
    assert res["size"] == 6 and res["sum"] == 15
    # Every rank ran in its own node's job sandbox.
    on_a = [c for c in res["cwd"] if c.startswith(str(a.state))]
    on_b = [c for c in res["cwd"] if c.startswith(str(b.state))]
    assert len(on_a) == 4 and len(on_b) == 2, res
    out = cp.admin("job", job).stdout
    assert "gang 1:" in out and "member" in out


def test_gang_waits_for_capacity_and_holds_it(mpi_cluster, mpi_program):
    tdir, make_cp, node = mpi_cluster
    cp = make_cp()
    a, b = node(cp, "node-a", cpus=2, jobs=4), node(cp, "node-b", cpus=2, jobs=4)
    meshed(a, b)
    busy = cp.submit("sleep", "8", cpus=2)
    wait_for(lambda: cp.job(busy)["state"] == "RUNNING", timeout=30, what="busy job running")
    busy_node = cp.job(busy)["node_id"]
    gang = cp.submit(mpi_program, ranks=4)
    wait_for(lambda: "draining" in cp.job(gang).get("pending_reason", ""), timeout=20, what="gang draining")
    # Held capacity is not handed to later small jobs.
    small = cp.submit("true", cpus=1)
    time.sleep(3)
    s = cp.job(small)
    assert s["state"] == "QUEUED" or s.get("node_id") != busy_node, s
    done = cp.wait_state(gang, TERMINAL, timeout=90)
    assert done["state"] == "COMPLETED", json.dumps(cp.job(gang), indent=1)
    assert cp.job(busy)["state"] == "COMPLETED"
    assert ranks_output(cp, a if done["node_id"] == a.node_id else b, done, tdir)["sum"] == 6
    cp.wait_state(small, TERMINAL, timeout=60)


def test_member_node_death_retries_the_gang(mpi_cluster, mpi_program):
    tdir, make_cp, node = mpi_cluster
    cp = make_cp()
    nodes = [node(cp, f"node-{x}", cpus=2, jobs=4) for x in "abc"]
    meshed(*nodes)
    by_id = {n.node_id: n for n in nodes}
    job = cp.submit(mpi_program, "6", ranks=4, attempts=3)
    first = wait_for(lambda: (j := cp.job(job))["state"] == "RUNNING" and len(gang_nodes(j)) == 2 and j,
                     timeout=60, what="gang running")
    member = next(n for n in gang_nodes(first) if n != first["node_id"])
    time.sleep(1.5)
    by_id[member].stop(signal.SIGKILL)
    done = cp.wait_state(job, TERMINAL, timeout=120)
    assert done["state"] == "COMPLETED", json.dumps(cp.job(job), indent=1)
    assert done["attempts_used"] >= 2
    assert member not in gang_nodes(done)
    head = by_id[done["node_id"]]
    assert ranks_output(cp, head, done, tdir)["sum"] == 6


def test_cancel_kills_every_member(mpi_cluster, mpi_program):
    _, make_cp, node = mpi_cluster
    cp = make_cp()
    a, b = node(cp, "node-a", cpus=2, jobs=4), node(cp, "node-b", cpus=2, jobs=4)
    meshed(a, b)
    job = cp.submit(mpi_program, "60", ranks=4)
    wait_for(lambda: cp.job(job)["state"] == "RUNNING" and a.processes() and b.processes(),
             timeout=60, what="ranks running on both nodes")
    cp.admin("job-cancel", job)
    done = cp.wait_state(job, TERMINAL, timeout=60)
    assert done["state"] == "CANCELLED"
    wait_for(lambda: not a.processes() and not b.processes(), timeout=30, what="no job processes left")


def test_not_enough_capacity_stays_queued(mpi_cluster, mpi_program):
    _, make_cp, node = mpi_cluster
    cp = make_cp()
    a, b = node(cp, "node-a", cpus=2, jobs=4), node(cp, "node-b", cpus=2, jobs=4)
    meshed(a, b)
    job = cp.submit(mpi_program, ranks=9)
    # Before the nodes' first lease polls the reason may still count 0 eligible nodes.
    j = wait_for(lambda: "not enough capacity: 4 of 9" in (j := cp.job(job)).get("pending_reason", "") and j,
                 timeout=30, what="pending reason")
    assert j["state"] == "QUEUED"
    assert "2 eligible nodes" in j["pending_reason"]


def test_local_single_node_mpi(tmp_path, mpi_program):
    d = Daemon(tmp_path / "state")
    d.start()
    try:
        job = d.submit("--ranks", "2", "--", mpi_program)
        st = d.wait(job)
        assert st["state"] == "COMPLETED", st
        (art,) = st["output_artifacts"]
        digest = art.removeprefix("artifact://sha256/")
        res = json.loads((d.state / "artifacts/sha256" / digest[:2] / digest).read_text())
        assert res["size"] == 2 and res["sum"] == 1
    finally:
        d.stop()
