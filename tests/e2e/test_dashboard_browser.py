"""Phase 9 headless-Chrome smoke test (docs/design/phase9.md §5).

Real tdrcp, real dashboard assets, a real browser. Chrome can't be handed a client
certificate without NSS tools, so a small local proxy terminates plain HTTP on 127.0.0.1
and forwards GETs to tdrcp over mTLS with a viewer certificate. Each page must render
with no console errors, no CSP violations and no error banner.

Run:  tools/e2e.sh -k dashboard_browser   (skipped when google-chrome is missing)
"""
import http.client
import http.server
import json
import shutil
import ssl
import subprocess
import tempfile
import threading
from pathlib import Path

import pytest

from test_control_plane import ROOT  # noqa: F401
from test_dashboard import dash  # noqa: F401  (fixture)
from test_control_plane import infra  # noqa: F401  (fixture)

CHROME = shutil.which("google-chrome") or shutil.which("chromium")

PAGES = ["#/", "#/nodes", "#/jobs", "#/workflows", "#/campaigns", "#/materials",
         "#/calculations", "#/results", "#/audit"]


def viewer_proxy(cp, pki, who="vera"):
    """Plain-HTTP proxy -> tdrcp (mTLS as `who`). GET/HEAD only."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(pki.dir / "ca.crt")
    ctx.load_cert_chain(pki.dir / f"{who}.crt", pki.dir / f"{who}.key")

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            conn = http.client.HTTPSConnection("127.0.0.1", cp.port, context=ctx, timeout=10)
            try:
                hdr = {k: v for k, v in self.headers.items()
                       if k.lower() in ("accept", "if-none-match", "x-fireforge-client")}
                conn.request("GET", self.path, headers=hdr)
                r = conn.getresponse()
                body = r.read()
                self.send_response(r.status)
                for k, v in r.getheaders():
                    if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            finally:
                conn.close()

        def do_POST(self):
            self.send_error(405)

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def render(url, profile):
    """(dom, stderr) after letting the page's scripts and fetches settle."""
    p = subprocess.run(
        [str(ROOT / "tools/capped"), CHROME, "--headless=new", "--disable-gpu", "--no-first-run",
         "--no-default-browser-check", f"--user-data-dir={profile}", "--enable-logging=stderr",
         "--v=0", "--virtual-time-budget=8000", "--window-size=1400,900", "--dump-dom", url],
        capture_output=True, text=True, timeout=120)
    return p.stdout, p.stderr


@pytest.mark.skipif(CHROME is None, reason="no google-chrome")
def test_pages_render_in_chrome(dash):
    cp, pki = dash
    # Some data for the science pages: the literature values.
    from fireforge_science.validate import literature_import_request
    req = literature_import_request()
    lit = Path(tempfile.mkdtemp()) / "lit.json"
    lit.write_text(req if isinstance(req, str) else json.dumps(req))
    cp.admin("calc-import", str(lit))

    srv = viewer_proxy(cp, pki)
    profile = tempfile.mkdtemp(prefix="chrome-")
    try:
        base = f"http://127.0.0.1:{srv.server_address[1]}/ui/"
        problems = {}
        for page in PAGES:
            dom, err = render(base + page, profile)
            bad = [ln for ln in err.splitlines()
                   if "CONSOLE" in ln and ("Uncaught" in ln or "Refused" in ln or "Error" in ln)]
            if 'class="view"' not in dom:
                bad.append("view not rendered")
            if "banner-error" in dom:
                bad.append("error banner shown")
            if page == "#/materials" and "ZrNiSn" not in dom:
                bad.append("materials list lacks ZrNiSn")
            if 'role-viewer' not in dom:
                bad.append("identity is not viewer")
            if bad:
                problems[page] = bad
        assert not problems, json.dumps(problems, indent=1)
    finally:
        srv.shutdown()
        shutil.rmtree(profile, ignore_errors=True)
