"""Phase 1 benchmark: submit N trivial jobs to a running tdrd and time them.

    tools/capped .venv/bin/python tests/e2e/bench_local.py --socket <ctl.sock> -n 10000
"""
import argparse
import socket
import time

from fireforge import protocol
from fireforge.tdr.v1 import envelope_pb2 as E, local_pb2 as L, job_pb2 as J

TERMINAL = {J.COMPLETED, J.FAILED, J.CANCELLED, J.EXPIRED, J.VALIDATION_FAILED}


class Ctl:
    def __init__(self, path):
        self.s = socket.socket(socket.AF_UNIX)
        self.s.connect(path)
        self.r = protocol.FrameReader(self.s)
        self.ids = protocol.MessageIdCounter()

    def call(self, mtype, msg, reply_cls):
        mid = self.ids.next()
        self.s.sendall(protocol.encode_frame(mtype, msg, mid))
        header, payload = self.r.read_frame()
        if header.message_type == E.ERROR:
            err = L.Error.FromString(payload)
            raise RuntimeError(err.message)
        return reply_cls.FromString(payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--socket", required=True)
    ap.add_argument("-n", type=int, default=10000)
    ap.add_argument("--cmd", default="true")
    a = ap.parse_args()
    c = Ctl(a.socket)

    t0 = time.perf_counter()
    ids = []
    for _ in range(a.n):
        req = L.SubmitJobRequest()
        req.spec.command.argv.append(a.cmd)
        req.spec.resources.memory_bytes = 16 << 20
        ids.append(c.call(E.CTL_SUBMIT, req, L.SubmitJobReply).job_id)
    t_submit = time.perf_counter() - t0

    pending = set(ids)
    states = {}
    while pending:
        for jid in list(pending):
            st = c.call(E.CTL_GET_JOB, L.GetJobRequest(job_id=jid), J.JobStatus)
            if st.state in TERMINAL:
                states[jid] = st.state
                pending.discard(jid)
            else:
                break  # FIFO: later jobs are not done either
        if pending:
            time.sleep(0.05)
    t_total = time.perf_counter() - t0

    done = sum(1 for s in states.values() if s == J.COMPLETED)
    print(f"jobs={a.n} completed={done} other={a.n - done}")
    print(f"submit: {t_submit:.2f}s ({a.n / t_submit:.0f}/s, {t_submit / a.n * 1e3:.2f} ms/job)")
    print(f"total:  {t_total:.2f}s ({a.n / t_total:.0f} jobs/s end to end)")


if __name__ == "__main__":
    main()
