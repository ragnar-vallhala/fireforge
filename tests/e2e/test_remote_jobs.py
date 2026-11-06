"""Phase 4 end-to-end: central job queue, leases, remote execution, recovery
(docs/design/phase4.md §9).

tdrcp runs with a short lease TTL (4 s) and 1 s heartbeats; nodes poll for
leases every second.
"""
import json
import signal
import tempfile
import time
from pathlib import Path

import pytest

from test_control_plane import ControlPlane, Node, infra, wait_for  # noqa: F401  (infra is a fixture)

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "EXPIRED", "VALIDATION_FAILED"}


class JobCP(ControlPlane):
    def __init__(self, base, pki, conninfo, lease_ttl=4):
        super().__init__(base, pki, conninfo)
        with open(self.conf, "a") as f:
            f.write(f"[jobs]\nlease_ttl = {lease_ttl}\nmax_attempts_default = 3\n")
            if type(self) is JobCP:  # holders only (Phase 4 locality); Phase 5 tests set their own
                f.write("locality_wait = 3600\n")

    def job(self, job_id):
        return self.admin_json("job", job_id)

    def jobs(self, **q):
        args = ["jobs", "--limit", "1000"]
        for k, v in q.items():
            args += [f"--{k}", v]
        return self.admin_json(*args).get("jobs", [])

    def submit(self, *argv, **opts):
        args = ["job-submit"]
        for k, v in opts.items():
            for item in (v if isinstance(v, list) else [v]):
                args += [f"--{k}", str(item)]
        return self.admin(*args, "--", *argv).stdout.strip()

    def submit_raw(self, argv):
        """POST /api/v1/jobs directly (fast, for bulk submission)."""
        _, crt, key = self.pki.creds("alice")
        status, body = self.raw("POST", "/api/v1/jobs", {"spec": {"command": {"argv": argv}}}, cert=(crt, key))
        assert status == 201, body
        return json.loads(body)["job_id"]

    def wait_state(self, job_id, states, timeout=30):
        states = {states} if isinstance(states, str) else set(states)
        return wait_for(lambda: (j := self.job(job_id))["state"] in states and j, timeout=timeout,
                        interval=0.25, what=f"job {job_id} in {states}")


class JobNode(Node):
    def __init__(self, *a, cpus=2, jobs=2, remote_jobs="on", **kw):
        self.cpus, self.jobs, self.remote_jobs = cpus, jobs, remote_jobs
        super().__init__(*a, **kw)

    def write_conf(self, cpus=None, jobs=None):
        ca, crt, key = self.pki.creds(self.name)
        self.conf.write_text(
            f"[control]\naddress = 127.0.0.1\nport = {self.cp.port}\n"
            f"remote_jobs = {self.remote_jobs}\nlease_poll = 1\nlease_poll_idle = 1\n"
            f"[node]\nname = {self.name}\nadvertise_address = 127.0.0.1\n"
            f"[network]\nlisten_address = 127.0.0.1\nlisten_port = {self.listen_port}\ntailscale = off\n"
            f"[resources]\nmax_cpu_cores = {self.cpus}\nmax_jobs = {self.jobs}\nmax_memory_mb = 2048\n"
            "default_job_memory_mb = 64\ncgroups = off\n"
            f"[tls]\nca_file = {ca}\ncert_file = {crt}\nkey_file = {key}\n")

    @property
    def node_id(self):
        return self.status()["node_id"]


@pytest.fixture
def cluster(infra):
    base, pg, pki = infra
    tdir = Path(tempfile.mkdtemp(prefix="j-", dir=base))
    cps, nodes = [], []
    ports = iter(range(17700, 17800))

    def make_cp(lease_ttl=4):
        cp = JobCP(tdir, pki, pg.new_db(), lease_ttl=lease_ttl).start()
        cps.append(cp)
        return cp

    def node(cp, name, start=True, **kw):
        n = JobNode(tdir, pki, name, cp, next(ports), **kw)
        nodes.append(n)
        return n.start() if start else n

    try:
        yield make_cp, node
    finally:
        for n in nodes:
            if n.proc and n.proc.poll() is None:
                try:
                    n.proc.send_signal(signal.SIGCONT)
                except ProcessLookupError:
                    pass
            n.stop(signal.SIGKILL)
        for cp in cps:
            cp.stop(signal.SIGKILL)


def by_id(nodes):
    return {n.node_id: n for n in nodes}


# ---- tests --------------------------------------------------------------------------------

def test_remote_submit_runs_on_another_node(cluster):
    make_cp, node = cluster
    cp = make_cp()
    a = node(cp, "node-a", remote_jobs="off")   # submitter never leases
    b = node(cp, "node-b")
    job = a.ctl("submit", "--remote", "--", "sh", "-c", "echo hello > $FIREFORGE_OUTPUT_DIR/out.txt").stdout.strip()
    assert job.startswith("job-")
    assert a.ctl("wait", "--remote", job, "--timeout", "30", check=False).returncode == 0
    st = json.loads(a.ctl("status", "--remote", job, "--json").stdout)
    assert st["state"] == "COMPLETED" and st["node_id"] == b.node_id
    out = st["output_artifacts"][0]

    # The executing node holds the output, ran it as a local job tied to the central id.
    local = json.loads(b.ctl("list", "--json").stdout)["jobs"]
    assert [j["remote_job_id"] for j in local] == [job]
    assert local[0]["job_id"] == f"{job}.a1"
    info = cp.admin_json("artifact", out)
    assert info["node_ids"] == [b.node_id]

    # Local submit is still the default and stays local.
    local_job = a.ctl("submit", "--", "true").stdout.strip()
    assert a.ctl("wait", local_job, "--timeout", "10", check=False).returncode == 0
    assert all(j["job_id"] != local_job for j in cp.jobs())


def test_jobs_spread_across_nodes(cluster):
    make_cp, node = cluster
    cp = make_cp()
    nodes = [node(cp, f"node-{c}") for c in "abc"]
    ids = [cp.submit("sleep", "1") for _ in range(12)]
    for j in ids:
        cp.wait_state(j, "COMPLETED", timeout=60)
    used = {cp.job(j)["node_id"] for j in ids}
    assert len(used) >= 2, used
    assert used <= set(by_id(nodes))


def test_killed_node_lease_expires_and_job_completes_elsewhere(cluster):
    make_cp, node = cluster
    cp = make_cp()
    a, b = node(cp, "node-a", jobs=1), node(cp, "node-b", jobs=1)
    nodes = by_id([a, b])
    job = cp.submit("sh", "-c", "sleep 3; echo ok > $FIREFORGE_OUTPUT_DIR/r")
    first = cp.wait_state(job, "RUNNING")
    victim = nodes[first["node_id"]]
    survivor = a if victim is b else b
    victim.stop(signal.SIGKILL)

    done = cp.wait_state(job, TERMINAL, timeout=40)
    assert done["state"] == "COMPLETED", done
    assert done["node_id"] == survivor.node_id
    attempts = done["attempts"]
    assert [at["state"] for at in attempts] == ["ABANDONED", "SUCCEEDED"]
    assert attempts[0]["error"] == "lease expired"
    assert [int(at["fencing_token"]) for at in attempts] == [1, 2]


def test_paused_node_result_is_fenced(cluster):
    make_cp, node = cluster
    cp = make_cp()
    a, b = node(cp, "node-a", jobs=1), node(cp, "node-b", jobs=1)
    nodes = by_id([a, b])
    job = cp.submit("sh", "-c", "sleep 3; echo $RANDOM > $FIREFORGE_OUTPUT_DIR/r")
    first = cp.wait_state(job, "RUNNING")
    paused = nodes[first["node_id"]]
    other = a if paused is b else b
    paused.proc.send_signal(signal.SIGSTOP)       # the child keeps running and finishes
    try:
        done = cp.wait_state(job, "COMPLETED", timeout=40)
        assert done["node_id"] == other.node_id
    finally:
        paused.proc.send_signal(signal.SIGCONT)
    # The paused node now reports its (stale) result: fenced, job unchanged.
    stale_local = f"{job}.a1"
    wait_for(lambda: json.loads(paused.ctl("status", stale_local, "--json").stdout)["state"] in TERMINAL,
             timeout=20, what="paused node notices its job finished")
    time.sleep(3)  # outbox flush + a couple of heartbeats
    after = cp.job(job)
    assert after["state"] == "COMPLETED" and after["node_id"] == other.node_id
    assert after["attempts"][0]["state"] == "ABANDONED"
    assert after["outputs"] == done["outputs"]
    log = (paused.dir / "tdrd.log").read_text()
    assert "stale" in log or "409" in log


def test_cancel_running_remote_job(cluster):
    make_cp, node = cluster
    cp = make_cp()
    a = node(cp, "node-a")
    job = cp.submit("sleep", "60")
    cp.wait_state(job, "RUNNING")
    cp.admin("job-cancel", job, "--reason", "test")
    done = cp.wait_state(job, TERMINAL, timeout=20)
    assert done["state"] == "CANCELLED", done
    wait_for(lambda: json.loads(a.ctl("status", f"{job}.a1", "--json").stdout)["state"] == "CANCELLED",
             timeout=15, what="local job cancelled")
    # Queued jobs cancel immediately.
    a.stop()
    q = cp.submit("true")
    assert cp.admin_json("job-cancel", q)["state"] == "CANCELLED"


def test_campaign_state_gates_leasing(cluster):
    make_cp, node = cluster
    cp = make_cp()
    node(cp, "node-a")
    camp = cp.admin_json("campaign-create", "gate-test")["campaign_id"]
    job = cp.submit("true", campaign=camp)
    time.sleep(3)
    assert cp.job(job)["state"] == "QUEUED"        # DRAFT holds
    cp.admin("campaign-set", camp, "--state", "ACTIVE")
    cp.wait_state(job, "COMPLETED", timeout=20)
    cp.admin("campaign-set", camp, "--state", "PAUSED")
    held = cp.submit("true", campaign=camp)
    time.sleep(3)
    assert cp.job(held)["state"] == "QUEUED"       # PAUSED holds
    cp.admin("campaign-set", camp, "--state", "ACTIVE")
    cp.wait_state(held, "COMPLETED", timeout=20)


def test_jobs_with_inputs_go_to_the_node_holding_them(cluster):
    make_cp, node = cluster
    cp = make_cp()
    a, b = node(cp, "node-a", cpus=4, jobs=4), node(cp, "node-b", jobs=2)
    # Produce an artifact on node-b with a local job; it is announced to the control plane.
    lj = b.ctl("submit", "--", "sh", "-c", "echo input-data > $FIREFORGE_OUTPUT_DIR/in.txt").stdout.strip()
    assert b.ctl("wait", lj, "--timeout", "10", check=False).returncode == 0
    art = json.loads(b.ctl("status", lj, "--json").stdout)["output_artifacts"][0]
    wait_for(lambda: cp.admin("artifact", art, check=False).returncode == 0, timeout=15, what="artifact announced")

    ids = [cp.submit("sh", "-c", "cat $FIREFORGE_INPUT_DIR/* > $FIREFORGE_OUTPUT_DIR/copy", input=art)
           for _ in range(4)]
    for j in ids:
        done = cp.wait_state(j, TERMINAL, timeout=30)
        assert done["state"] == "COMPLETED", done
        assert done["node_id"] == b.node_id
    assert a.node_id not in {cp.job(j)["node_id"] for j in ids}


def test_control_plane_outage_during_remote_job(cluster):
    make_cp, node = cluster
    cp = make_cp(lease_ttl=30)
    a = node(cp, "node-a")
    job = cp.submit("sh", "-c", "sleep 4; echo done > $FIREFORGE_OUTPUT_DIR/out")
    cp.wait_state(job, "RUNNING")
    cp.stop(signal.SIGKILL)
    wait_for(lambda: json.loads(a.ctl("status", f"{job}.a1", "--json").stdout)["state"] == "COMPLETED",
             timeout=20, what="job finished while the control plane was down")
    cp.start()  # same port and database
    done = cp.wait_state(job, "COMPLETED", timeout=90)
    assert done["attempts_used"] == 1 and done["outputs"]


def test_many_small_jobs_complete_exactly_once(cluster):
    make_cp, node = cluster
    cp = make_cp()
    nodes = [node(cp, f"node-{c}", cpus=4, jobs=4) for c in "abc"]
    ids = {cp.submit_raw(["true"]) for _ in range(200)}

    def all_done():
        jobs = {j["job_id"]: j for j in cp.jobs() if j["job_id"] in ids}
        return len(jobs) == len(ids) and all(j["state"] in TERMINAL for j in jobs.values()) and jobs
    jobs = wait_for(all_done, timeout=180, interval=1, what="200 jobs terminal")
    assert all(j["state"] == "COMPLETED" for j in jobs.values())
    assert all(j["attempts_used"] == 1 for j in jobs.values())
    # Each ran exactly once across the cluster.
    ran = []
    for n in nodes:
        ran += [j["remote_job_id"] for j in json.loads(n.ctl("list", "--limit", "1000", "--json").stdout).get("jobs", [])
                if j.get("remote_job_id")]
    assert sorted(ran) == sorted(ids)
    assert len({j["node_id"] for j in jobs.values()}) == 3
