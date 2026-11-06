"""Phase 9 end-to-end: the web dashboard served by a real tdrcp (docs/design/phase9.md §5).

Run:  tools/e2e.sh -k dashboard
Covers: / -> /ui/ redirect, index.html and every asset it references (and every ES module
they import), ETag/304, security headers, viewer certificates (real `tdrctl pki issue
--role viewer`) reading the API, viewer mutations -> 403, admin cancel from a same-origin
browser request, cross-site / Origin-mismatch -> 403 csrf, non-JSON body -> 415, whoami,
and `tdrctl pki p12`. The headless-Chrome smoke test lives elsewhere.
"""
import http.client
import json
import re
import shutil
import ssl
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import pytest

from test_control_plane import ControlPlane, ctl, infra  # noqa: F401  (fixture)

CSP = ("default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; connect-src 'self'; "
       "frame-ancestors 'none'; base-uri 'none'; form-action 'none'")


@pytest.fixture
def dash(infra):
    base, pg, pki = infra
    if not (pki.dir / "vera.crt").exists():
        ctl("pki", "issue", pki.dir, "--role", "viewer", "vera")
    tdir = Path(tempfile.mkdtemp(prefix="t-", dir=base))
    cp = ControlPlane(tdir, pki, pg.new_db()).start()
    try:
        yield cp, pki
    finally:
        cp.stop()


def request(cp, pki, method, path, who="vera", body=None, headers=None):
    """(status, headers dict with lower-case names, body bytes)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(pki.dir / "ca.crt")
    ctx.load_cert_chain(pki.dir / f"{who}.crt", pki.dir / f"{who}.key")
    conn = http.client.HTTPSConnection("127.0.0.1", cp.port, context=ctx, timeout=10)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        r = conn.getresponse()
        data = r.read()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, data
    finally:
        conn.close()


def api(cp, pki, method, path, who="vera", body=None, headers=None):
    h = dict(headers or {})
    if body is not None and not isinstance(body, (bytes, str)):
        body = json.dumps(body)
        h.setdefault("Content-Type", "application/json")
    status, _, data = request(cp, pki, method, path, who, body, h)
    try:
        return status, json.loads(data or b"null")
    except ValueError:
        return status, data.decode(errors="replace")


def browser_headers(cp, site="same-origin", origin=None):
    """What a browser's fetch() from the dashboard sends with a POST."""
    return {"Origin": origin or f"https://127.0.0.1:{cp.port}", "Sec-Fetch-Site": site,
            "X-FireForge-Client": "ui", "Content-Type": "application/json"}


def check_security(h):
    assert h.get("content-security-policy") == CSP
    assert h.get("x-content-type-options") == "nosniff"
    assert h.get("referrer-policy") == "no-referrer"


def local_refs(html: str):
    return re.findall(r'(?:src|href)\s*=\s*"([^"#?]+)"', html)


def js_imports(src: str):
    pats = [r'\bfrom\s*["\']([^"\']+)["\']', r'\bimport\s*\(\s*["\']([^"\']+)["\']\s*\)', r'\bimport\s+["\']([^"\']+)["\']']
    return [m for p in pats for m in re.findall(p, src)]


def test_dashboard_assets(dash):
    cp, pki = dash
    status, h, _ = request(cp, pki, "GET", "/")
    assert status == 302 and h["location"] == "/ui/"
    check_security(h)

    status, h, body = request(cp, pki, "GET", "/ui/")
    check_security(h)
    if status == 404:
        pytest.skip("control/ui/web has no index.html yet")
    assert status == 200 and h["content-type"].startswith("text/html")
    assert h["cache-control"] == "no-cache"
    index = body.decode()
    assert "<script>" not in index.replace(" ", "")  # no inline scripts (CSP)

    # Hash-routed fallback, HEAD, ETag.
    s2, _, b2 = request(cp, pki, "GET", "/ui/materials/mat-1")
    assert s2 == 200 and b2 == body
    s3, h3, b3 = request(cp, pki, "HEAD", "/ui/")
    assert s3 == 200 and b3 == b"" and int(h3["content-length"]) == len(body)
    s4, h4, b4 = request(cp, pki, "GET", "/ui/", headers={"If-None-Match": h["etag"]})
    assert s4 == 304 and b4 == b"" and h4["etag"] == h["etag"]
    check_security(h4)

    # Every asset index.html references, then every module those import.
    todo = [urljoin("/ui/", r) for r in local_refs(index) if not urlsplit(r).scheme and not r.startswith("//")]
    assert todo, "index.html references no assets"
    seen = set()
    while todo:
        path = todo.pop()
        if path in seen:
            continue
        seen.add(path)
        assert path.startswith("/ui/"), f"asset outside /ui/: {path}"
        status, h, data = request(cp, pki, "GET", path)
        assert status == 200, f"{path} -> {status}"
        check_security(h)
        assert h["cache-control"] == "max-age=3600"
        assert h["etag"].startswith('"') and len(h["etag"]) == 18
        ct = h["content-type"]
        if path.endswith(".js"):
            assert ct.startswith("text/javascript"), (path, ct)
            todo += [urljoin(path, m) for m in js_imports(data.decode()) if m.startswith(".")]
        elif path.endswith(".css"):
            assert ct.startswith("text/css"), (path, ct)
        s304, _, _ = request(cp, pki, "GET", path, headers={"If-None-Match": h["etag"]})
        assert s304 == 304

    status, h, _ = request(cp, pki, "GET", "/ui/does-not-exist.js")
    assert status == 404
    check_security(h)
    status, h, _ = request(cp, pki, "GET", "/ui/../CMakeLists.txt")
    assert status == 404
    status, h, _ = request(cp, pki, "POST", "/ui/", body="{}", headers={"Content-Type": "application/json"})
    assert status == 405 and h["allow"] == "GET, HEAD"


def test_whoami_every_role(dash):
    cp, pki = dash
    for who, role in (("vera", "viewer"), ("alice", "admin"), ("node-a", "node")):
        status, out = api(cp, pki, "GET", "/api/v1/whoami", who)
        assert status == 200, out
        assert out["role"] == role and out["name"] == who
        fp = ctl("pki", "show", pki.dir / f"{who}.crt").stdout
        assert f"fingerprint: {out['fingerprint']}" in fp


def test_viewer_reads_but_cannot_write(dash):
    cp, pki = dash
    camp = cp.admin("campaign-create", "dash-camp").stdout
    assert camp
    job_id = cp.admin("job-submit", "--", "/bin/true").stdout.strip()

    for path in ("/api/v1/nodes", "/api/v1/campaigns", "/api/v1/jobs", f"/api/v1/jobs/{job_id}",
                 "/api/v1/workflows", "/api/v1/materials", "/api/v1/calculations", "/api/v1/audit"):
        status, out = api(cp, pki, "GET", path)
        assert status == 200, (path, out)
    _, jobs = api(cp, pki, "GET", "/api/v1/jobs")
    assert job_id in {j["job_id"] for j in jobs["jobs"]}
    _, audit = api(cp, pki, "GET", "/api/v1/audit")
    assert "campaign.create" in {e["action"] for e in audit["entries"]}

    # Mutations and node-only endpoints: 403 even from a same-origin browser request.
    for method, path, body in (("POST", f"/api/v1/jobs/{job_id}/cancel", {}),
                               ("POST", "/api/v1/campaigns", {"name": "nope"}),
                               ("POST", "/api/v1/nodes/register", {"name": "vera"}),
                               ("POST", "/api/v1/leases", {}),
                               ("GET", "/api/v1/artifacts/sha256/" + "a" * 64, None)):
        status, out = api(cp, pki, method, path, body=body,
                          headers=browser_headers(cp) if method != "GET" else None)
        assert status == 403 and out["code"] == "forbidden", (path, status, out)
    _, job = api(cp, pki, "GET", f"/api/v1/jobs/{job_id}")
    assert job["state"] == "QUEUED"


def test_admin_cancel_from_browser_and_csrf_guard(dash):
    cp, pki = dash
    j1 = cp.admin("job-submit", "--", "/bin/true").stdout.strip()
    j2 = cp.admin("job-submit", "--", "/bin/true").stdout.strip()

    # Cross-site and Origin-mismatch requests are refused before anything happens.
    for headers in (browser_headers(cp, site="cross-site"),
                    browser_headers(cp, site="same-site"),
                    browser_headers(cp, origin="https://evil.example"),
                    browser_headers(cp, origin=f"https://127.0.0.1:{cp.port + 1}"),
                    {"Origin": "null", "Content-Type": "application/json"}):
        status, out = api(cp, pki, "POST", f"/api/v1/jobs/{j1}/cancel", "alice", body={}, headers=headers)
        assert status == 403 and out["code"] == "csrf", (headers, status, out)
    # A cross-site HTML form (urlencoded body) is 415 even without fetch metadata.
    status, _ = api(cp, pki, "POST", f"/api/v1/jobs/{j1}/cancel", "alice", body="reason=x",
                    headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert status == 415
    _, job = api(cp, pki, "GET", f"/api/v1/jobs/{j1}", "alice")
    assert job["state"] == "QUEUED"

    # Same-origin browser request from an admin certificate works.
    status, job = api(cp, pki, "POST", f"/api/v1/jobs/{j1}/cancel", "alice", body={"reason": "from the dashboard"},
                      headers=browser_headers(cp))
    assert status == 200, job
    assert job["state"] == "CANCELLED"
    # tdrctl (no Origin, no Sec-Fetch-Site) is unaffected.
    cp.admin("job-cancel", j2)
    assert cp.admin_json("job", j2)["state"] == "CANCELLED"
    actions = [e["action"] for e in cp.admin_json("audit")["entries"]]
    assert actions.count("job.cancel") == 2


def test_ui_disabled(dash):
    cp, pki = dash
    cp.stop()
    cp.conf.write_text(cp.conf.read_text() + "[ui]\nenabled = false\n")
    cp.start()
    for path in ("/", "/ui/", "/ui/index.html"):
        status, _, _ = request(cp, pki, "GET", path)
        assert status == 404, path
    assert api(cp, pki, "GET", "/api/v1/whoami")[0] == 200


def test_pki_p12_export(infra, tmp_path):
    _, _, pki = infra
    if not (pki.dir / "vera.crt").exists():
        ctl("pki", "issue", pki.dir, "--role", "viewer", "vera")
    out = tmp_path / "vera.p12"
    r = ctl("pki", "p12", pki.dir, "vera", "--out", out)
    pw_file = Path(str(out) + ".pass")
    assert str(pw_file) in r.stdout
    assert out.stat().st_mode & 0o777 == 0o600 and pw_file.stat().st_mode & 0o777 == 0o600
    assert len(pw_file.read_text().strip()) >= 20
    assert ctl("pki", "p12", pki.dir, "vera", "--out", out, check=False).returncode != 0  # no overwrite

    given = tmp_path / "pw"
    given.write_text("correct horse\n")
    out2 = tmp_path / "vera2.p12"
    ctl("pki", "p12", pki.dir, "vera", "--out", out2, "--password-file", given)
    assert not Path(str(out2) + ".pass").exists()

    if not shutil.which("openssl"):
        pytest.skip("openssl CLI not installed")
    for f, pw in ((out, f"file:{pw_file}"), (out2, "pass:correct horse")):
        info = subprocess.run(["openssl", "pkcs12", "-in", f, "-passin", pw, "-nokeys", "-info"],
                              capture_output=True, text=True)
        assert info.returncode == 0, info.stderr
        assert "AES-256-CBC" in info.stderr + info.stdout
        assert "CN = vera" in info.stdout or "CN=vera" in info.stdout
        assert info.stdout.count("BEGIN CERTIFICATE") == 2  # client + CA
    bad = subprocess.run(["openssl", "pkcs12", "-in", out2, "-passin", "pass:wrong", "-nokeys"],
                         capture_output=True, text=True)
    assert bad.returncode != 0
