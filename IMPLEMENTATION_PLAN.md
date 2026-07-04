# scanrr — Implementation Plan

> Companion to **[SPEC.md](./SPEC.md)** (the source of truth). This plan sequences
> the build into vertical slices, each independently testable. Section refs (§N)
> point into the spec. Checkboxes track progress.

## Guiding principles

- **Spec is law.** If implementation reveals a better design, update SPEC.md first,
  then code. Keep the `#n` / `Qn` decision tags intact.
- **Test the core, hard.** Idempotency and corruption detection are the product.
  Every idempotency rule in §3 and every detection verdict gets a test before we
  trust it. The detector fixture harness already exists — extend it.
- **Vertical slices.** Each milestone ends at something runnable and demoable, not
  a pile of unwired modules.
- **Model first, concurrency second.** M1 implements the *full* §3/§6 data model
  (shared queue, subscribers, fan-out, `run_files`) but drains it **synchronously**
  in-process. M2 swaps only the executor (pebble pool + async writer thread +
  scheduler + crash recovery). This de-risks the tricky accounting early, in a
  single-threaded context that's trivial to test.

## Current state (done)

- [x] SPEC.md v0.3 — reviewed, internally consistent.
- [x] **M0 foundation** — deps, `ruff` + `mypy` clean, typed `RuntimeConfig`
      (§13 defaults), WAL engine, JSON decision logging, GitHub Actions CI.
- [x] **M1 core scan engine** — schema + dedup index, blake3 hashing, worker-side
      hash→cache→decode, Phase A discovery + synchronous Phase B drain,
      `reconcile_detections`, CLI (`scanrr scan`), minimal API. 20 tests green
      (14 detection + 6 idempotency); verified end-to-end via the CLI.
- [x] All constrained values are enums (`scanrr/enums.py`); no `Any`/type-ignores.
- [x] **M2 concurrency & durability** — async `Orchestrator` draining the shared
      queue via a `pebble.ProcessPool` (bounded, per-file timeout, worker
      termination); single-thread `Database` (atomic claims); crash recovery
      (`scanning`→`pending` on startup); run cancellation; APScheduler cron wiring;
      async `POST /jobs/:id/run` + `/runs/:id/cancel`. Executor is pluggable
      (`InlineExecutor` for tests, `PebbleExecutor` in prod). 27 tests green incl.
      bounded-concurrency, timeout→unreadable, cancel-terminates-inflight,
      crash-recovery-resumes, real-pool + full-stack HTTP smokes.

- [x] **M3 UI** — SSE event bus (`core/events.py`) the orchestrator publishes to
      (run.started/progress/completed, task.done); read/action API (stats, jobs
      CRUD, runs, run files, detection triage, settings, `/api/events`); React +
      Vite + TS + Tailwind v4 SPA (Dashboard, Jobs, live Run detail, Detections
      triage, Settings) wired via TanStack Query + live SSE invalidation; FastAPI
      serves the built `dist` (with an SPA fallback route so client routes deep-link).
      Backend 29 tests (incl. event-wiring + endpoint + SPA-serve); `npm run build`
      (tsc + vite) green; CI builds both. **Playwright E2E** (`frontend/e2e/`, 7
      tests) drives the real app — every view renders correctly, navigation and the
      create-job form work — validated visually against a live seeded instance.

**Deviations / deferrals:** pip + venv (not `uv`); **Alembic still deferred**
(schema via `create_all` + raw DDL). UI uses hand-written Tailwind components
(not the shadcn CLI), CSS bars instead of Recharts, and no Playwright yet —
kept deps lean and the build reliable. **Settings is read-only** and `POST
/library/revalidate` (#7) still pending — both land when settings editing does.

---

## M0 — Foundation (tooling & skeleton)

**Goal:** a clean, linted, migratable project skeleton the rest builds on.

- [ ] Adopt **uv** for the backend; lock deps. Add `fastapi`, `uvicorn`, `sqlmodel`,
      `alembic`, `apscheduler`, `httpx`, `pebble`, `prometheus-client`,
      `pydantic-settings`, `cryptography` (Fernet).
- [ ] `ruff` + `mypy` config; `pytest` already wired. Pre-commit hook.
- [ ] `core/config.py` — env settings via `pydantic-settings` (DB path, media roots,
      Fernet key, `X-Scanrr-Token`, log level) + a typed accessor over the `settings`
      table with the §13 defaults as the canonical source.
- [ ] `db/engine.py` — SQLite engine with WAL, `busy_timeout=5000`, FK pragma;
      session factory.
- [ ] `alembic init`; wire `env.py` to the models' metadata.
- [ ] Structured JSON logging setup (§14a) — one logger, decision-event helper.
- [ ] **CI** (GitHub Actions): install ffmpeg, run ruff + mypy + pytest.

**DoD:** `uv run pytest` green in CI; `alembic upgrade head` creates an empty DB;
`ruff`/`mypy` clean.

---

## M1 — Core scan engine (CLI-testable)  ← next

**Goal:** `scanrr scan <path>` walks a directory, applies the §3 algorithm, and is
provably idempotent and correct — no async, no scheduler, one process.

### Schema & models
- [ ] `db/models.py` — SQLModel models + first Alembic migration for the core tables
      (§8): `settings`, `jobs`, `job_runs`, `scan_tasks`, `scan_task_subscribers`,
      `run_files`, `files`, `scan_results`, `detections`. (Arr/replacement/
      notification tables deferred to M4/M5.)
- [ ] Enforce the dedup guard: partial unique index on `scan_tasks(path) WHERE
      status IN ('pending','scanning')`; the `scan_results` validity is
      `(detector_version, detector_backend)`.

### Scan pipeline
- [ ] `scanning/hashing.py` — blake3 whole-file hash; `stat`-based (size, mtime)
      fast-path helper. Configurable to sha256.
- [ ] `scanning/worker.py` — pure `scan_file(path) -> Outcome` (hash + integrity),
      the function M2 will hand to the pebble pool. Uses `integrity.check`.
- [ ] `jobs/discovery.py` — recursive path walk with `os.scandir`; extension +
      `min_file_size` filter (§7).
- [ ] `jobs/queue.py` — `enqueue`/`active_task`/`subscribe`/`claim_next_pending`
      over `scan_tasks` + `scan_task_subscribers`.
- [ ] `scanning/engine.py` — the heart:
  - [ ] **Phase A** discovery (§3, **stat-only** — no content reads): stability gate
        → TTL fast-path → `enqueue_or_subscribe` (dedup by path); writes `run_files`
        dispositions.
  - [ ] **Phase B** synchronous drain: claim → **hash** → content-cache check
        (hit → skip decode) → `scan_file` decode → write `scan_results`/`files` →
        `reconcile_detections` → `fan_out` (credit runs, set outcomes) → `maybe_finalize`.
  - [ ] `reconcile_detections` (#6): open on corrupt; auto-resolve an open detection
        whose path now scans `ok` under a different hash.
  - [ ] Transient `error`/`timeout` retry policy (#5/#8): back to `pending` w/
        backoff up to `scan_max_attempts`, then `unreadable` + fan-out.

### Entry points
- [ ] `cli.py` — `scanrr scan <path> [--ttl] [--backend]`; prints run summary.
- [ ] Minimal FastAPI: `POST /api/jobs`, `POST /api/jobs/:id/run` (synchronous for
      now), `GET /api/runs/:id`, `GET /api/detections`, `GET /api/health`.

### Tests (the important part)
- [ ] Idempotency matrix on the fixture media: first run scans; **second run skips**
      via TTL; **touching mtime** past TTL re-hashes but hits the content cache;
      **same content at a second path** skips via hash dedup (cross-path).
- [ ] `reconcile_detections`: corrupt→detection; replace fixture with clean copy→
      detection auto-resolves.
- [ ] Transient failure: an unreadable/timeout path retries then goes `unreadable`,
      not `corrupt`; never written to `scan_results`.
- [ ] Fan-out/dedup: two runs over overlapping dirs decode a shared file **once**,
      both runs credited; both finalize.
- [ ] Detector-version bump invalidates cache (re-scan), backend mismatch too (#11).

**DoD:** the idempotency matrix passes; a corrupt file in a scanned tree shows up in
`GET /detections`; re-running a scan does ~zero work.

**Parallel spike (unblocks §7 [OPEN]):** benchmark `pyav` vs `subprocess` throughput
on a few real large files; record numbers in SPEC §7 and pick the prod default.

---

## M2 — Scheduling, concurrency & durability

**Goal:** replace M1's synchronous drain with the real concurrent executor; runs
survive restarts and run on a schedule.

- [ ] `pebble.ProcessPool` dispatcher (§6): bounded `max_scan_workers`, drain by
      `seq`, per-file `max_scan_seconds` timeout → transient.
- [ ] **Single async DB writer thread** + `asyncio.Queue` (#4); reads via threadpool;
      batched discovery inserts + WAL checkpoint.
- [ ] Cancellation (§6): `POST /runs/:id/cancel` — unsubscribe; drop task only at
      zero subscribers; terminate worker.
- [ ] Crash recovery on startup: `scanning`→`pending`; resume `running` runs.
- [ ] APScheduler: cron per job, `coalesce`, `max_instances=1`, `misfire_grace_time`
      (#14).
- [ ] `POST /library/revalidate` (#7) with scope preview.
- [ ] Concurrency tests: overlapping concurrent runs; cancel mid-flight; kill -9 +
      restart resumes without dup work or lost runs; timeout path.

**DoD:** two scheduled jobs run concurrently sharing the pool; a `kill` mid-scan
resumes cleanly; a hung file times out and retries.

---

## M3 — UI

**Goal:** the beautiful operational surface (§12).

- [ ] Scaffold `frontend/` — Vite + React + TS + Tailwind + shadcn/ui; TanStack
      Query; React Router; api client; SSE hook.
- [ ] `GET /api/events` SSE bus on the backend; wire `run.progress` / `task.updated`
      / `detection.created` / `run.completed`.
- [ ] `/stats` aggregates endpoint.
- [ ] Views: Dashboard, Jobs + Job editor, Run detail (live per-file via `run_files`),
      Detections triage, Files search, Settings (general/integrations/notifications).
- [ ] Single-image serving: FastAPI mounts the built SPA.
- [ ] `vitest` component tests + one Playwright smoke (create job → run → see result).

**DoD:** create a path job in the UI, run it, watch live progress, triage a detection.

---

## M4 — Sonarr / Radarr integration

- [ ] `arr_instances`, `path_mappings`, `file_arr_links` tables + migration.
- [ ] `integrations/sonarr.py`, `radarr.py` (httpx): connection test; enumerate
      episode/movie files; capture arr ids.
- [ ] Longest-prefix path mapping; unmapped/missing files → discovery warnings.
- [ ] `arr` job type in discovery; populate `file_arr_links`.
- [ ] API + UI: manage instances (encrypted keys, test button), path mappings; manual
      `POST /detections/:id/replace` (→ `pending_approval`).
- [ ] Tests against a mocked arr API (recorded fixtures).

**DoD:** an arr job discovers real library paths via mapping and links them to media ids.

---

## M5 — Auto-replace & notifications

- [ ] `replacements` table + migration; approval gate (Q3): `pending_approval` →
      approve/reject (per-item + bulk); `auto_approve_replacements` bypass;
      `max_deletions_per_run` cap.
- [ ] Replacement executor (§9): delete → search → **bounded poll** (Q4) → **verify
      re-scan** (#6) → resolve / retry / `exhausted`+`needs_attention`.
- [ ] `notification_channels`/`rules`/`queue`/`log` tables; Pushover client.
- [ ] Notification **queue + periodic flusher** (Q5): individual under
      `notification_batch_threshold`, else batched digest.
- [ ] Replacements UI view; notification settings + test push.
- [ ] Tests: approval gate blocks deletion; poll→verify happy path; still-corrupt
      loop caps at `max_replace_attempts`; flush batching threshold.

**DoD:** a corrupt arr file, once approved, is deleted, re-requested, re-scanned, and
its detection auto-resolves — with a Pushover digest.

---

## M6 — Deploy (kube-saturn)

- [ ] Multi-stage Dockerfile (pnpm build → Python image + ffmpeg/libav → uvicorn).
- [ ] Flux manifests: Deployment (1 replica), Service, PVC (SQLite, volsync-backed),
      read-only NFS media mounts, Fernet key + `X-Scanrr-Token` Secrets.
- [ ] Cloudflare tunnel ingress + **Zero Trust** entry (per kube-saturn CLAUDE.md —
      restricted/admin).
- [ ] `/health` liveness/readiness; `/metrics` scrape annotation.
- [ ] Volsync bootstrap per the repo's PVC convention.

**DoD:** scanrr reachable at its `*.markmckessock.com` host behind Zero Trust,
scanning a real Synology mount, DB surviving pod restarts.

---

## Cross-cutting

- **Migrations:** every schema-touching PR ships an Alembic revision; never edit a
  released migration.
- **Config/defaults:** §13 is canonical; code reads defaults from one module.
- **Observability (§14a):** land the JSON decision-logger in M1, `/metrics` in M2,
  grow counters per milestone.
- **Security:** Fernet-encrypt arr keys / Pushover tokens from M4; `X-Scanrr-Token`
  middleware on mutating routes from the first API in M1.

## Risks & de-risking

| Risk | Mitigation |
|---|---|
| pebble + async writer thread + SQLite interplay | M1 proves the model synchronously; M2 changes only the executor, behind the same tests |
| Detector throughput on 4K remuxes over NFS | Benchmark spike in M1; §7 default is data-driven |
| Fan-out/finalization accounting bugs | Exhaustively unit-tested in M1's single-threaded context before concurrency |
| arr API surface drift (v3) | Wrap in a thin client with recorded-fixture tests; pin to API v3 |
| Destructive replacement | Approval-gated by default + per-run cap + full audit, all tested in M5 |
