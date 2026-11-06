"""Run pw.x (docs/design/phase8.md §6).

    mpirun -np <cpus> --bind-to none pw.x -nk 1 -in pw.in     (cpus > 1)
    pw.x -nk 1 -in pw.in                                      (cpus == 1)

with OMP_NUM_THREADS=1, cwd = the calculation directory, stdout+stderr to `pw.out`.
Overrides: FIREFORGE_PW_X (pw.x path), FIREFORGE_SCI_MPIRUN (launcher command, may carry
arguments, e.g. "mpirun --oversubscribe"). No retries here: a failed run raises PwError
and the TDR attempt fails (`retryable` says whether another attempt could help).

Ubuntu's quantum-espresso 6.7 package (built with _FORTIFY_SOURCE) aborts with "buffer
overflow detected" in its md5-of-pseudopotential routine (`snprintf(&md5[2*i], 8, "%02x")`
into a buffer with fewer bytes left: a false positive of the fortify check; the write
itself is 3 bytes). Every run reads pseudopotentials, so pw.x is unusable as shipped. The
runner therefore LD_PRELOADs a tiny shim (SHIM_SOURCE) whose `__snprintf_chk` formats with
min(maxlen, buffer size) instead of aborting, compiled once with cc into
~/.cache/fireforge/qe/ (FIREFORGE_PW_PRELOAD=<path.so> to use another one,
FIREFORGE_PW_PRELOAD=none to disable). DISPLAY is removed from pw.x's environment (Open
MPI/hwloc otherwise probes X and prints "Authorization required").
"""
from __future__ import annotations

import os
import re
import shlex
import shutil
import signal
import subprocess
import hashlib
import tempfile
import time
from pathlib import Path
from typing import Callable

LOG_NAME = "pw.out"
INPUT_NAME = "pw.in"
TAIL_LINES = 40
PW_BANNER = re.compile(r"Program PWSCF v\.?\s*(\S+)")


class PwError(RuntimeError):
    """pw.x failed. `retryable`: crash / timeout (True) vs. a deterministic failure such
    as SCF non-convergence or an input error (False). `log_tail`: last lines of pw.out."""

    def __init__(self, message: str, *, retryable: bool, log_tail: str = "") -> None:
        super().__init__(message + (f"\n--- pw.out (tail) ---\n{log_tail}" if log_tail else ""))
        self.retryable = retryable
        self.log_tail = log_tail


def pw_executable() -> str:
    exe = os.environ.get("FIREFORGE_PW_X") or shutil.which("pw.x")
    if not exe:
        raise PwError("pw.x not found (install quantum-espresso or set FIREFORGE_PW_X)",
                      retryable=False)
    return exe


def command(cpus: int, input_name: str = INPUT_NAME) -> list[str]:
    exe = pw_executable()
    pw = [exe, "-nk", "1", "-in", input_name]
    if cpus <= 1:
        return pw
    launcher = os.environ.get("FIREFORGE_SCI_MPIRUN") or shutil.which("mpirun")
    if not launcher:
        raise PwError("mpirun not found (needed for cpus > 1; set FIREFORGE_SCI_MPIRUN)",
                      retryable=False)
    return [*shlex.split(launcher), "-np", str(int(cpus)), "--bind-to", "none", *pw]


SHIM_SOURCE = r"""
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
/* fireforge: fortify false positive in QE 6.7 md5_from_file (see runner.py) */
int __snprintf_chk(char *s, size_t maxlen, int flag, size_t slen, const char *fmt, ...) {
    (void)flag;
    va_list ap;
    va_start(ap, fmt);
    int r = vsnprintf(s, maxlen < slen ? maxlen : slen, fmt, ap);
    va_end(ap);
    return r;
}
"""
SHIM_DIR = Path("~/.cache/fireforge/qe")


def preload_shim() -> str | None:
    """Path of the snprintf shim to LD_PRELOAD into pw.x (built on first use), or None
    (disabled, or no C compiler: pw.x then runs as is)."""
    env = os.environ.get("FIREFORGE_PW_PRELOAD")
    if env is not None and env.strip():
        return None if env.strip().lower() == "none" else env.strip()
    tag = hashlib.sha256(SHIM_SOURCE.encode()).hexdigest()[:12]
    so = SHIM_DIR.expanduser() / f"pw_snprintf_shim-{tag}.so"
    if so.is_file():
        return str(so)
    cc = shutil.which("cc") or shutil.which("gcc")
    if not cc:
        return None
    so.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=so.parent, prefix=".build-") as tmp:
        src = Path(tmp) / "shim.c"
        src.write_text(SHIM_SOURCE)
        out = Path(tmp) / "shim.so"
        r = subprocess.run([cc, "-O2", "-U_FORTIFY_SOURCE", "-D_FORTIFY_SOURCE=0", "-shared",
                            "-fPIC", "-o", str(out), str(src)], capture_output=True, text=True)
        if r.returncode != 0 or not out.is_file():
            return None
        os.replace(out, so)
    return str(so)


def pw_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k != "DISPLAY"}
    env["OMP_NUM_THREADS"] = "1"
    shim = preload_shim()
    if shim:
        env["LD_PRELOAD"] = " ".join(x for x in (shim, env.get("LD_PRELOAD", "")) if x)
    return env


def tail(text: str, n: int = TAIL_LINES) -> str:
    return "\n".join(text.rstrip().splitlines()[-n:])


def check_log(text: str, returncode: int | None, workdir: Path | None = None) -> None:
    """Raise PwError if the run failed: non-zero exit, `convergence NOT achieved`, an
    `Error in routine` block, or no `JOB DONE`."""
    crash = ""
    if workdir is not None and (workdir / "CRASH").is_file():
        crash = (workdir / "CRASH").read_text(errors="replace")
    err_block = ""
    for m in re.finditer(r"%{20,}[ \t]*\n(.*?)\n[ \t]*%{20,}", text + "\n" + crash, re.S):
        if "Error" in m.group(1):
            err_block = " ".join(m.group(1).split())
            break
    if "convergence NOT achieved" in text:
        raise PwError("pw.x: SCF convergence NOT achieved", retryable=False,
                      log_tail=tail(text))
    if err_block:
        raise PwError(f"pw.x error: {err_block}", retryable=False, log_tail=tail(text))
    if returncode not in (0, None):
        raise PwError(f"pw.x exited with status {returncode}", retryable=True,
                      log_tail=tail(text))
    if "JOB DONE" not in text:
        raise PwError("pw.x stopped without JOB DONE", retryable=True, log_tail=tail(text))


def run_pw(input_text: str, workdir: Path, cpus: int = 1, *, timeout: float | None = None,
           cancelled: Callable[[], bool] | None = None, poll_s: float = 0.5) -> str:
    """Write pw.in into `workdir`, run pw.x there and return its output (pw.out).

    `cancelled()` is polled; when it returns True the process group is killed and PwError
    (not retryable) raised. Timeout: PwError (retryable)."""
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / INPUT_NAME).write_text(input_text)
    crash = workdir / "CRASH"
    if crash.exists():
        crash.unlink()
    cmd = command(cpus)
    env = pw_env()
    log_path = workdir / LOG_NAME
    start = time.monotonic()
    with open(log_path, "wb") as log:
        try:
            proc = subprocess.Popen(cmd, cwd=workdir, stdout=log, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL, env=env, start_new_session=True)
        except OSError as e:
            raise PwError(f"cannot start {cmd[0]}: {e}", retryable=False) from None
        reason = ""
        while True:
            try:
                rc = proc.wait(timeout=poll_s)
                break
            except subprocess.TimeoutExpired:
                pass
            if cancelled is not None and cancelled():
                reason = "cancelled"
            elif timeout is not None and time.monotonic() - start > timeout:
                reason = f"timeout after {timeout:g} s"
            if reason:
                _kill(proc)
                rc = proc.wait()
                break
    text = log_path.read_text(errors="replace")
    if reason:
        raise PwError(f"pw.x {reason}", retryable=reason != "cancelled", log_tail=tail(text))
    check_log(text, rc, workdir)
    return text


def _kill(proc: subprocess.Popen) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            continue


def version(text: str) -> str:
    m = PW_BANNER.search(text)
    return m.group(1) if m else "unknown"
