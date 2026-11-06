# Phase 9: Web dashboard

A browser UI for FireForge, served by `tdrcp` itself: cluster health, jobs, workflows,
campaigns, the materials database and thermoelectric results, plus a few operator actions.
Decisions (2026-11-06):

- **Served by tdrcp**, on its existing HTTPS listener. Browsers authenticate with a client
  certificate, imported once as a PKCS#12 file, so identity and roles are the same as for
  `tdrctl`.
- **Read-only, plus a few admin actions**: cancel a job or workflow, pause or resume a
  campaign, set a node offline or change its limits. Submitting work stays in the CLI.
- **Plain JavaScript modules, no build step**, plus one vendored chart library: uPlot
  1.6.32 (MIT) in `control/ui/web/vendor/uplot/`, checked against its npm integrity hash.
  No npm, and no network access at build or run time.
- **New `viewer` role** for read-only certificates.

## 1. Components

```
control/ui/web/                static files (index.html, app.js, pages/*.js, lib/*.js, style.css, vendor/)
control/ui/                    tdr_cpui: embeds web/ into the binary, serves /ui/*
control/ui/tests/              Boost.Test: asset table, headers, content types
control/ui/web/tests/*.test.js node --test unit tests for the pure JS modules
runtime/tls                    Role::kViewer, PKI issue --role viewer
runtime/ctl                    tdrctl pki p12 (PKCS#12 export for browsers)
control/api                    viewer role checks, GET /api/v1/whoami, browser CSRF guard
tests/e2e/test_dashboard.py    real tdrcp: assets, whoami, viewer vs admin, CSRF guard
```

## 2. Serving

- `GET /` redirects (302) to `/ui/`. `GET /ui/` and every `/ui/<path>` without a file
  extension return `index.html`; the app uses hash routing (`#/materials/mat-…`), so this
  is only a fallback. Static files are served from an asset table compiled into tdrcp.
  CMake turns each file in `control/ui/web/` into a byte array (`file(READ … HEX)`), and new
  files are picked up by a glob.
- Responses:
  - Content-Type by extension: html, js (`text/javascript`), css, svg, json, woff2.
  - `ETag` is the sha256 of the asset, prefix only; `If-None-Match` returns 304.
  - `Cache-Control: no-cache` for index.html, `max-age=3600` for everything else.
- Security headers on every `/ui` response:
  - `Content-Security-Policy: default-src 'self'; img-src 'self' data:; style-src 'self';
    script-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none';
    form-action 'none'`
  - `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`
  - No inline scripts or styles anywhere in the UI.
- Any certificate role (viewer, admin, node) may fetch the assets. The data comes from the
  API with its usual role checks.
- `[ui] enabled = true` in tdrcp.conf (default true); false returns 404 for `/` and `/ui/*`.

## 3. Roles and API changes

- **`Role::kViewer`** (`OU=viewer`, EKU clientAuth). `tdrctl pki issue DIR --role viewer
  NAME`.
- Read endpoints accept viewer as well as admin. These are every GET on nodes, events,
  campaigns, jobs, workflows, materials, structures, calculations and audit. Node-only
  endpoints (register, heartbeat, leases, status, announce, artifacts) reject viewers with
  403. All mutations stay admin-only.
- **`GET /api/v1/whoami`** (any role) returns `{"role": "viewer|admin|node", "name": CN,
  "fingerprint": hex}`. The UI uses it to show or hide the action buttons. It is a UI
  convenience only; the server enforces every permission.
- **Browser CSRF guard.** Client-certificate authentication is ambient, like a cookie, so
  every request that isn't GET or HEAD:
  - is rejected with 403 `csrf` when it carries a `Sec-Fetch-Site` header whose value isn't
    `same-origin`;
  - is rejected when it carries an `Origin` header that doesn't match the request's
    `Host`.

  The UI sends `X-FireForge-Client: ui`; tdrctl and tdrd send neither header and are
  unaffected. JSON-only bodies are kept: requests whose Content-Type isn't
  `application/json` get 415, which also blocks cross-site HTML forms.
- **`tdrctl pki p12 DIR NAME [--out FILE] [--password-file F]`** writes `NAME.p12` (cert,
  key and the CA chain, AES-256), for import into Firefox or Chrome. The CA itself must also
  be trusted by the browser (import `ca.crt` as an authority) or the browser rejects
  tdrcp's server certificate. Server certificates must carry the host name or IP the
  browser uses as a SAN; `pki issue --dns/--ip` already supports this.

## 4. Pages

Hash routes, one ES module per page (`pages/<name>.js` exporting `render(el, params, ctx)`).
Data is polled every 5 s while the tab is visible, and polling stops when it is hidden. All
API JSON is proto3 JSON, rendered with `textContent` and never `innerHTML` with data.

| Route | Content |
|---|---|
| `#/` overview | nodes (health, CPU and memory bars), job counts by state, running workflows with step progress, latest audit entries |
| `#/nodes`, `#/nodes/ID` | table; detail: capabilities, hardware, policy, load, recent events, running jobs; admin: offline, limits form |
| `#/jobs`, `#/jobs/ID` | filters (state, campaign); detail: spec (command, resources, capabilities), attempts, outputs, error; admin: cancel |
| `#/workflows`, `#/workflows/ID` | list; detail: step DAG as SVG (layered layout, colour by state, spawned subtrees collapsed with counts), step table; admin: cancel |
| `#/campaigns`, `#/campaigns/ID` | list; detail: description, params, workflows, job counts; admin: pause/resume/complete |
| `#/materials`, `#/materials/ID` | list with formula/element filters; detail: structure (formula, space group, lattice, sites), latest calculation per kind, **predicted vs literature** table, charts (ZT(T) p/n, κ_l(T), power factor) |
| `#/calculations`, `#/calculations/ID` | filters (kind, evidence, material, campaign, workflow); detail: properties, conditions, method, convergence, uncertainty, provenance links (job, workflow, node) |
| `#/results` | thermoelectric ranking: every `te.evaluate` calculation, sortable (peak ZT p/n, T at peak, κ_l 300 K, gap, e_hull, screen pass); latest `te.validation` checks table |
| `#/audit` | audit log |

**Charts.** The `te.evaluate` record gets a `curves` property: `t_k`, best-doping
`zt_p` and `zt_n`, `pf_p_uw_cmk2` and `pf_n_uw_cmk2`, and `kappa_l_w_mk`, each a list
aligned with `t_k`. It is small (about 25 temperatures), so it fits the record limits.
Older records without `curves` show only the peak values.

**Look.** One stylesheet with light and dark themes (`prefers-color-scheme`), system font
stack, tables with sticky headers, state colours shared with the CLI's state names, and a
responsive layout that collapses the sidebar below 800 px.

## 5. Testing

- C++: asset table (every file in web/ present, content types, ETag/304, CSP header on
  every response), `/` redirect, viewer role checks for every route, the CSRF guard rejects
  cross-site and Origin-mismatch requests and lets through tdrctl-style requests without
  those headers, `whoami`, PKCS#12 export round trip (OpenSSL parses it back).
- JS (`node --test control/ui/web/tests`, through `tools/capped`): formatting, DAG layout
  (layers, no overlaps, spawned grouping), the predicted-vs-literature matcher, the poller
  (stops when hidden), and the router.
- e2e (`tests/e2e/test_dashboard.py`): real tdrcp. Fetch `/ui/` and every asset, read
  through the API with a viewer certificate, viewer mutation → 403, admin cancel from a
  same-origin request → ok, cross-site → 403. Plus one headless-Chrome smoke test when
  `google-chrome` exists: load the overview with a test certificate (Chrome policy
  `AutoSelectCertificateForUrls`) and assert there are no console errors and the node
  table renders.

Exit criteria: the dashboard shows the live Phase 8 validation run (nodes, workflow DAG,
materials, predicted vs literature, ZT charts) from a browser holding a viewer
certificate, and admin actions work from a browser holding an admin certificate.

## 6. Implementation notes

- **Module.** `control/ui` (`tdr_cpui`, `tdr/cpui/ui.hpp`): `Ui(UiOptions{enabled}).serve(req)` returns
  a response for `/`, `/ui` and `/ui/*`, nullopt otherwise; tdrcp tries it before `ControlApi::handle`.
  `[ui] enabled = false` makes `serve` return nullopt, so the API router answers 404.
  `embed.cmake` generates `ui_assets.cpp` from every file under `web/` except `web/tests/` and
  dot-files. The glob uses `CONFIGURE_DEPENDS` and the file list's hash is part of the command, so added,
  edited and removed files all regenerate it. A missing `index.html` makes `/ui/` a 404 ("dashboard not
  built"), not a build failure.
- **Serving details.** ETags (16 hex of sha256) are computed once at startup. `/ui` redirects like `/`.
  Lookup is by exact key, and keys with `%`, `\`, empty, `.` or `..` segments get 404, so `/ui/../x` and
  `/ui/%2e%2e/` never fall back to index.html. Extension-less paths that aren't in the table get
  index.html, and so does a trailing `/`. Unknown extensions are served as `application/octet-stream`,
  and `txt`/`md` as `text/plain`. Responses without a peer identity get 401 (the TLS layer already
  requires a client certificate). 405 responses carry `Allow: GET, HEAD`. HEAD needed a small HttpsServer
  change: it now sends the headers and Content-Length without the body.
- **Roles.** `kRead` (node, admin, viewer) on every GET that nodes could already call. Node events and
  audit were admin-only, so they are now admin + viewer. `GET /api/v1/artifacts/sha256/{hex}` stays node
  + admin. `POST /nodes/{id}/offline` keeps its `admin` flag: it only skips the own-node check for admins,
  and viewers are refused by the role check before it, so viewers count as non-admin there.
  `ControlApi::routes()` feeds a table test that fails when a new route has no viewer decision.
- **whoami** returns plain JSON `{"role","name","fingerprint"}`. The fingerprint is
  `Identity::fingerprint_sha256` (lower-case hex of SHA-256 over the certificate DER, the same value
  `tdrctl pki show` prints).
- **CSRF / 415** run in `ControlApi::handle` before routing, so they apply to unknown routes too, and
  before the role check. `Sec-Fetch-Site` must equal `same-origin` when present, so `same-site` and
  `none` are refused too. When `Origin` is present, its authority must equal `Host` (case-insensitive).
  `Origin: null` and a missing Host are refused. A non-empty body must have media type
  `application/json` (parameters such as `; charset=utf-8` are allowed). A body with no Content-Type at
  all also gets 415. Empty-body POSTs need no Content-Type. `X-FireForge-Client` is not checked.
- **`tdrctl pki p12`.** Reads `DIR/NAME.crt` and `NAME.key`, and `DIR/ca.crt` as the chain. Without
  `--password-file` it generates a 24-character random password (144 bits) and writes it to
  `<out>.pass` (0600), because empty-password bundles don't import reliably in browsers. Both files are
  0600 and never overwritten. `pki issue --role viewer` needed no tls change (clientAuth EKU,
  `OU=viewer`).

## 7. Status

Implemented (2026-11-06). Tests:
- C++: 18 of 18 suites pass with `-DFF_WERROR=ON`. That includes `tdr_cpui` and `api_viewer_tests`, and
  a fix to the racy `fetches_beyond_max_fetching_queue` transfer test.
- JS: 61 tests (`node --test`).
- e2e: `test_dashboard.py` (6 tests) and `test_dashboard_browser.py`. The browser test runs headless
  Chrome on every list page through a local plain-HTTP proxy that presents a viewer certificate. It
  checks for console errors, CSP violations and error banners, and that the identity shows as viewer.

API gaps found while building the UI (future work):
- `/jobs` has no pagination or counts endpoint, so the UI computes counts from at most 1000 jobs.
- Jobs can't be filtered by node or workflow on the server.
- The materials `formula` filter matches exactly; substring search happens in the browser.
- No browser-usable artifact download.
- No server-side "latest calculation of kind K per material".
