"""End-to-end tests for the restricted job user (docs/design/phase1.md §11).

Needs one-time setup:  sudo tools/setup-job-user.sh   (and --caps-only after rebuilding tdrd)
Skipped when the user or the capability-enabled binary (build/caps/tdrd) is missing.
"""
import grp
import os
import pwd
import subprocess
import tempfile
from pathlib import Path

import pytest

from test_tdrd import ROOT, TDRD, Daemon

JOB_USER = os.environ.get("JOB_USER", "fireforge-job")
CAPS_TDRD = ROOT / "build/caps/tdrd"

try:
    PW = pwd.getpwnam(JOB_USER)
except KeyError:
    PW = None


def has_caps(path: Path) -> bool:
    if not path.exists():
        return False
    r = subprocess.run(["getcap", str(path)], capture_output=True, text=True)
    return all(c in r.stdout for c in ("cap_setuid", "cap_setgid", "cap_kill"))


needs_user = pytest.mark.skipif(PW is None, reason=f"user {JOB_USER} missing: sudo tools/setup-job-user.sh")
needs_caps = pytest.mark.skipif(PW is None or not has_caps(CAPS_TDRD),
                                reason="build/caps/tdrd missing or stale: sudo tools/setup-job-user.sh --caps-only")


def make_env(tdrd):
    d = tempfile.mkdtemp(prefix="ffju-", dir="/tmp")
    os.chmod(d, 0o711)  # job user must traverse to state_dir/work
    conf = Path(d) / "tdr.conf"
    conf.write_text(f"[resources]\njob_user = {JOB_USER}\n")
    return Daemon(Path(d) / "st", tdrd=tdrd, config=conf), d


@pytest.fixture
def jdaemon():
    dm, d = make_env(CAPS_TDRD)
    dm.start()
    yield dm
    dm.stop()
    subprocess.run(["rm", "-rf", d])


def run_ok(dm, script, *extra):
    st = dm.wait(dm.submit(*extra, "--", "sh", "-c", script))
    assert st["state"] == "COMPLETED", st
    return st


def read_artifact(dm, art):
    h = art.removeprefix("artifact://sha256/")
    return (dm.state / "artifacts/sha256" / h[:2] / h).read_text()


@needs_user
def test_refuses_without_capabilities():
    dm, d = make_env(TDRD)  # plain build output has no file caps
    with pytest.raises(RuntimeError, match="CAP_SETUID"):
        dm.start()
    subprocess.run(["rm", "-rf", d])


@needs_caps
def test_job_runs_as_job_user_without_caps(jdaemon):
    st = run_ok(jdaemon, "{ id -u; id -g; grep -E '^(CapEff|CapPrm|CapAmb|NoNewPrivs)' /proc/self/status; umask; }"
                         " > $FIREFORGE_OUTPUT_DIR/who")
    lines = read_artifact(jdaemon, st["output_artifacts"][0]).split("\n")
    assert lines[0] == str(PW.pw_uid) and lines[1] == str(PW.pw_gid)
    text = "\n".join(lines)
    for cap in ("CapEff", "CapPrm", "CapAmb"):
        assert f"{cap}:\t0000000000000000" in text
    assert "NoNewPrivs:\t1" in text and "0007" in text


@needs_caps
def test_job_cannot_modify_input_artifact(jdaemon):
    st = run_ok(jdaemon, "echo original > $FIREFORGE_OUTPUT_DIR/data")
    art = st["output_artifacts"][0]
    # Each forbidden action exits with its own code if it unexpectedly succeeds.
    run_ok(jdaemon, "f=$(ls $FIREFORGE_INPUT_DIR/*); "
                    "chmod u+w $f 2>/dev/null && exit 10; "
                    "(echo x >> $f) 2>/dev/null && exit 11; "
                    "rm -f $f 2>/dev/null; test -e $f || exit 12; "
                    "touch $FIREFORGE_INPUT_DIR/new 2>/dev/null && exit 13; exit 0", "--input", art)
    assert read_artifact(jdaemon, art) == "original\n"


@needs_caps
def test_job_cannot_reach_control_socket_or_store(jdaemon):
    run_ok(jdaemon, '! python3 -c "import socket,sys; s=socket.socket(socket.AF_UNIX); s.connect(sys.argv[1])" "$CTL"'
                    " 2>/dev/null && ! ls $STATE/artifacts 2>/dev/null && ! ls $STATE/state 2>/dev/null",
           "--env", f"CTL={jdaemon.sock}", "--env", f"STATE={jdaemon.state}")


@needs_caps
def test_outputs_are_daemon_owned_copies(jdaemon):
    st = run_ok(jdaemon, "echo hi > $FIREFORGE_OUTPUT_DIR/o")
    h = st["output_artifacts"][0].removeprefix("artifact://sha256/")
    f = jdaemon.state / "artifacts/sha256" / h[:2] / h
    assert f.stat().st_uid == os.geteuid() and (f.stat().st_mode & 0o777) == 0o444


@needs_caps
def test_timeout_and_cancel_kill_job_user_processes(jdaemon):
    st = jdaemon.wait(jdaemon.submit("--timeout", "1", "--", "sleep", "30"))
    assert st["state"] == "EXPIRED"
    j = jdaemon.submit("--", "sleep", "30")
    for _ in range(100):
        if jdaemon.status(j)["state"] == "RUNNING":
            break
    jdaemon.ctl("cancel", j)
    assert jdaemon.wait(j)["state"] == "CANCELLED"
    r = subprocess.run(["pgrep", "-u", JOB_USER, "-f", "sleep 30"], capture_output=True)
    assert r.returncode == 1, "job-user process survived"


@needs_caps
def test_unreadable_output_fails_commit(jdaemon):
    st = jdaemon.wait(jdaemon.submit("--", "sh", "-c", "echo s > $FIREFORGE_OUTPUT_DIR/x; chmod 600 $FIREFORGE_OUTPUT_DIR/x"))
    assert st["state"] in ("FAILED", "RETRY_PENDING", "VALIDATION_FAILED") and "ermission" in st["last_error"]


@needs_caps
def test_work_dir_cleaned_after_success(jdaemon):
    j = jdaemon.submit("--", "sh", "-c", "mkdir -p sub/deeper && echo x > sub/deeper/f && echo y > $FIREFORGE_OUTPUT_DIR/o")
    assert jdaemon.wait(j)["state"] == "COMPLETED"
    assert not any((jdaemon.state / "work" / j).iterdir()) if (jdaemon.state / "work" / j).exists() else True
