# AGENTS.md

## Purpose

SondeHub+ Local is a local Python web dashboard and HTTP API for radiosonde monitoring. It combines SondeHub data, optional Radiosonde Watch data, and optional local `radiosonde_auto_rx` logs.

This file is the machine-oriented operating and contribution guide for coding agents and automated tools working in this repository.

## Read first

Before changing code, read:

1. `README.md`
2. `.env.example`
3. `install.sh`
4. `systemd/sondehub-plus.service.example`
5. the relevant section of `app.py`
6. `THIRD_PARTY.md` for licensing and attribution

Do not infer installation-specific values that are not present in the repository.

## Repository map

- `app.py` — main application; HTTP server, SondeHub integration, Radiosonde Watch integration, local `radiosonde_auto_rx` parsing, caching, realtime telemetry, dashboard HTML/JS, and API endpoints.
- `static/rw-altitude-chart.css` — local chart styling.
- `static/rw-altitude-chart.js` — local chart logic.
- `.env.example` — public configuration contract.
- `install.sh` — installer and repository self-check.
- `systemd/sondehub-plus.service.example` — systemd template and memory guards.
- `README.md` — user-facing setup and operation documentation.
- `THIRD_PARTY.md` — third-party software/services and licensing notes.
- `LICENSE` — GPL-3.0-or-later license text.
- `llms.txt` — concise machine-readable index for AI/LLM tools.

## Configuration contract

Supported environment variables include:

- `SONDEHUB_PLUS_PORT`
- `SONDEHUB_PLUS_BIND`
- `SONDEHUB_LISTENER_CALLSIGN`
- `RADIOSONDE_WATCH_URL`
- `RADIOSONDE_WATCH_LOCAL_ENDPOINT`
- `RADIOSONDE_AUTO_RX_DIR`
- `SONDEHUB_API_URL`
- `SONDEHUB_PLUS_USER_AGENT`

Use `.env.example` as the source of public example values.

Important constraints:

- Keep `SONDEHUB_PLUS_BIND=127.0.0.1` as the safe example default.
- Do not hard-code a private hostname, private callsign, private IP, username, absolute home directory, token, credential, or station-specific endpoint.
- `RADIOSONDE_WATCH_LOCAL_ENDPOINT` is installation-specific. Do not replace it with an invented generic endpoint.
- `RADIOSONDE_AUTO_RX_DIR` is expanded with `os.path.expanduser()`; portable paths such as `~/radiosonde_auto_rx` are intentional.

## HTTP interface

The main handler currently exposes these routes:

- `/` — dashboard HTML.
- `/watch` — redirect to the configured Radiosonde Watch browser URL.
- `/healthz` — health response.
- `/api/status` — combined station status.
- `/api/local-receptions` — local reception sessions for the last 24 hours.
- `/api/local-receptions-7d` — local reception sessions for the last 7 days.
- `/api/local-receptions-all` — all locally available reception sessions.
- `/api/local-flight?serial=...` — local flight analytics.
- `/api/station-stats-7d` — local station statistics.
- `/api/sondes` — current SondeHub+ sonde payload.
- `/api/sonde?serial=...` — detailed sonde/history response.
- `/api/realtime?serial=...` — realtime telemetry and stream status.
- `/api/sites?distance_km=...` — filtered launch sites.
- `/api/site-sondes?site=...&last=...` — sondes for a launch site.
- `/api/listener` — configured listener information.
- `/api/listeners?distance_km=...` — nearby listeners.
- `/api/recovery-stats?distance_km=...` — recovery statistics.
- `/api/amateur?distance_km=...&last=...` — SondeHub amateur data.
- `/api/listener-stats` — SondeHub listener statistics.
- `/api/websocket-info` — redacted websocket endpoint metadata; the presigned URL is intentionally not exposed.

When changing a route, update user-facing documentation if the behavior or configuration contract changes.

## Required safety invariants

Preserve these behaviors unless a change explicitly and deliberately replaces them:

- The public example binds to loopback by default.
- The application does not claim to provide built-in authentication or TLS.
- The README warns against direct Internet exposure.
- The presigned SondeHub websocket URL is not returned by the local API.
- Inline callsign serialization in `render_page()` must remain safe for an inline `<script>` context. Keep JSON ASCII escaping and the explicit escaping of `<`, `>`, and `&`.
- The installer privacy check must continue rejecting obvious local hostnames and absolute user-home paths embedded in `app.py`.
- Systemd memory guards are intentional:
  - `MemoryHigh=350M`
  - `MemoryMax=500M`
  - `MemorySwapMax=64M`
- SondeHub listener freshness intentionally uses a `1d` listener query and an 8-hour freshness threshold. Do not reduce it to a 3-hour presence test: `radiosonde_auto_rx` station-position metadata is normally refreshed on a multi-hour cadence.

Do not weaken these protections as a side effect of unrelated work.

## Change strategy

Prefer small, reviewable changes over large rewrites.

The application intentionally remains primarily a single Python file. Do not perform a broad framework migration or frontend rewrite as part of an unrelated bug fix.

When changing memory-sensitive SondeHub history processing, preserve bounded-memory behavior. Avoid rebuilding a design that must materialize a complete large history archive in memory.

When changing local receiver integration, fail gracefully if an optional local endpoint or optional local data source is unavailable.

## Validation

At minimum, after a code or configuration change run:

```bash
./install.sh --check
git diff --check
```

For Python syntax without creating repository artifacts:

```bash
python3 - <<'PY'
from pathlib import Path
p = Path("app.py")
compile(p.read_text(encoding="utf-8"), str(p), "exec")
print("APP_COMPILE=OK")
PY
```

For changes to `render_page()`, include a hostile-input test that verifies:

- JSON round-trip still returns the normalized callsign.
- no raw `<`, `>`, or `&` survives inside the injected JSON.
- a literal `</script>` cannot survive the serialization boundary.
- Unicode line separators are escaped.

For installer changes also run:

```bash
bash -n install.sh
./install.sh --check
```

## Privacy and secrets

Never commit:

- GitHub tokens or other access tokens.
- private keys.
- real `.env` files.
- private station credentials.
- machine-specific usernames or absolute home paths.
- private hostnames that identify a local installation.

Use placeholders in documentation and examples.

## Dependencies and licensing

The Python dependency is declared in `requirements.txt`. Third-party frontend/network components and attribution notes are documented in `THIRD_PARTY.md`.

Do not remove required attribution or license notices. If adding a dependency, document its purpose and license.

Keep the upstream acknowledgements in `README.md` and `THIRD_PARTY.md` accurate. Do not describe an integration as copied source code unless exact provenance is known; when copied/adapted code is identified, record the upstream repository, path/commit and license.

## Definition of done

A change is ready when:

- the public configuration remains portable,
- no private installation data was introduced,
- `./install.sh --check` passes,
- `git diff --check` passes,
- relevant targeted tests pass,
- documentation is updated when behavior or configuration changes,
- the change does not silently weaken the security or memory-safety invariants above.
