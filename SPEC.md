# scanrr — Design Specification

> **Status:** Draft v0.2 · **Owner:** Mark Mckessock
> This document is the **source of truth** for scanrr's design. Code follows the
> spec; when they disagree, update the spec first. Decisions marked
> **[OPEN]** need a call before the affected code is written.
>
> **v0.2** — folded in the adversarial-review outcomes: validated integrity
> detection (§7), file-stability gate + transient-error retries + lazy
> revalidation (§3), per-file timeouts & real cancellation via `pebble` (§6),
> single-writer DB thread (§5), replacement verify-loop (§9), in-cluster authz
> (§11/§14), and observability (§14a). `#n` tags trace back to review points.
>
> **v0.3** — resolved the five open questions (`Qn` tags): concurrent runs over a
> global path-deduplicated queue (§6), human-approval gate for replacements (§9),
> bounded polling of arr (§9), and a queued/periodically-flushed notification
> pipeline (§10). Schema updated accordingly (§8).

---

## 1. Overview

**scanrr** periodically scans a media library for **corrupt video files** (bad
frames, broken i-frames, bitstream errors) using ffmpeg, records results in a
durable store, and surfaces failures through a modern web UI. It integrates with
Sonarr/Radarr to discover library paths and to automatically re-request corrupt
files, and it emits notifications via Pushover.

The design centres on **content-addressed idempotency**: a file's integrity is a
deterministic property of its bytes, so results are cached by content hash and
reused across paths, runs, and restarts. A scan that fails partway through never
re-does completed work.

### Goals

- Detect corrupt media reliably via full-file ffmpeg integrity checks.
- Never re-scan unchanged content: skip via (size+mtime) fast-path, per-job TTL,
  and cross-path content-hash dedup.
- Survive crashes/restarts without losing progress or duplicating work.
- Two job types: **path** (absolute directory) and **arr** (Sonarr/Radarr library).
- Optional automatic re-request of corrupt files through Sonarr/Radarr.
- Pushover notifications for key lifecycle events.
- A beautiful, responsive UI for config, live scan progress, and corrupt-file triage.

### Non-Goals (v1)

- Transcoding / remuxing / repair of files (we detect, we don't fix bytes).
- Multi-node distributed workers (single container, internal worker pool).
- Auth/multi-user (deployed behind Cloudflare Zero Trust like the rest of the homelab).
- Media types other than video (audio-only / images out of scope for v1).

---

## 2. Core Concepts (Domain Model)

| Concept | Meaning |
|---|---|
| **Job** | A reusable, configurable unit of work: a source (path or arr instance) + TTL + schedule + options. Defines *what* to scan and *how often*. |
| **Job Run** | One execution of a Job. Has discovery + scan phases and aggregate stats. |
| **Scan Task** | One file's work item on the **global, path-deduplicated** queue. Shared across runs (many runs may subscribe); processed once. |
| **Run File** | A run's per-file ledger entry (disposition + outcome) — how each run "sees" the shared work, incl. skips. |
| **File** | A path on disk: its current hash, size, mtime, and last-scan bookkeeping. |
| **Scan Result** | Content-addressed (keyed by hash) ffmpeg verdict: `ok` / `corrupt`. The reusable cache. Transient `error`/`timeout` outcomes are *not* stored here. |
| **Detection** | A corrupt file observed at a path, with remediation state (open → resolved). What the user triages. |
| **Replacement** | An attempt to re-acquire a corrupt file via Sonarr/Radarr. |
| **Arr Instance** | A configured Sonarr or Radarr endpoint (URL + API key). |
| **Path Mapping** | Translates an arr-namespace path to scanrr's local mount path. |

**Key invariant:** `Scan Result` is keyed by **content hash**, valid only for a
given **detector version + backend**, never by path. `File` maps path → hash.
`Detection` maps a corrupt observation → remediation. This separation is what
makes cross-path dedup and idempotent resume fall out naturally.

---

## 3. Scan Algorithm & Idempotency (the heart of the system)

Work happens in two phases: **discovery** (per run, cheap, decides each file's
disposition and enqueues only real work) and the **worker** (once per shared
queue task, does the expensive scan and fans the result out to every subscribed run).

**Phase A — discovery (per run):**
```
for path in discover(job):
    stat = os.stat(path)

    # 0. Stability gate — never scan a file that may still be written/importing.
    if (now - stat.mtime) < min_file_age_seconds:            # default 120s
        record(run, path, "skipped_too_fresh"); continue

    f = files.get(path)

    # 1. Cheapest path — no disk read. last_scanned_at is GLOBAL (a scan is a scan
    #    regardless of which job did it); TTL is evaluated against it.
    if f and f.size == stat.size and f.mtime == stat.mtime
           and f.last_scanned_at and (now - f.last_scanned_at) < job.ttl:
        record(run, path, "skipped_ttl"); continue

    # 1.5 Concurrent-dedup — a task for this path is already active: subscribe and
    #     skip hashing entirely; the result will fan out to us too (§6).
    if task := active_task(path):
        subscribe(task, run); record(run, path, "queued", task); continue

    # 2. Content identity — one full read to hash (blake3). Accepts a second full
    #    read on the eventual cache miss for exact whole-file dedup (#2).
    h = hash_file(path)

    # 3. Content-addressed cache hit — reused only if the cached verdict is valid
    #    for the CURRENT detector (version AND backend). A mismatch falls through
    #    (lazy revalidation, below), NOT a forced library-wide rescan.
    sr = scan_results.get(h)
    if sr and sr.detector_version == CURRENT_DETECTOR_VERSION
           and sr.detector_backend == CURRENT_DETECTOR_BACKEND:
        files.upsert(path, hash=h, size, mtime, last_scanned_at=now)
        reconcile_detections(path, h, sr.status, run)     # open/resolve as needed
        record(run, path, "skipped_hash_cached", outcome=sr.status); continue

    # 4. Cache miss — enqueue on the SHARED queue (dedup by path) carrying the hash,
    #    and subscribe. The worker (Phase B) does the actual scan.
    task = enqueue(path, content_hash=h); subscribe(task, run)
    record(run, path, "queued", task)
```

**Phase B — worker (once per shared task, result fanned out to all subscribers).**
The dispatcher (§6) claims the next `pending` task by `seq`, marks it `scanning`,
and runs the worker. `fan_out(task, outcome)` is the single place every subscribed
run is credited — it exists so `ok`, `corrupt`, and `unreadable` all converge on
the same accounting, which is what guarantees runs always finalize (§6).
```
task = claim_next_pending()                # by seq; task.status = "scanning"
result = ffmpeg_integrity_check(task.path, timeout=max_scan_seconds)   # §7

if result.status in ("ok", "corrupt"):     # deterministic verdict
    scan_results.upsert(task.content_hash, result, DETECTOR_VERSION, DETECTOR_BACKEND)
    files.upsert(task.path, hash=task.content_hash, last_scanned_at=now)
    task.status, task.result_status = "done", result.status
    detection = reconcile_detections(task.path, task.content_hash, result.status)
    if result.status == "corrupt":
        enqueue_notification("corrupt_found", path=task.path)   # §10 queue
    fan_out(task, result.status)
else:                                       # error / timeout = TRANSIENT
    retry_or_fail(task)                     # NOT cached; back to 'pending' w/ backoff...
    if task.status == "unreadable":         # ...until scan_max_attempts exhausted
        fan_out(task, "unreadable")

def fan_out(task, outcome):                 # credit EVERY subscribed run
    for run in subscribers(task):
        set_outcome(run, task.path, outcome)             # run_files.outcome
        if outcome == "corrupt" and run.job.auto_replace:
            propose_replacement(detection, run.job)       # §9 (approval-gated)
        emit_sse(run, "task.updated"); maybe_finalize(run)   # §6 lifecycle step 5
```

`reconcile_detections` closes the remediation loop (#6): a `corrupt` verdict opens
(or reuses) a detection; an `ok` verdict on a path that had an **open detection for
a different hash** auto-resolves it. So once a file is replaced with a clean copy,
its old detection clears itself. Replacement proposal is **per subscribing job**
(a shared task may have subscribers whose jobs differ on `auto_replace`).
`maybe_finalize` completes a run once its every `run_files` row has a terminal
disposition/outcome — reached for all three outcomes, so no run is left hanging.

**Why this satisfies every requirement:**

- *"Don't re-scan within TTL"* → steps 0–1 (TTL gates the unchanged fast path).
- *"Skip if hash unchanged"* → steps 1 & 3.
- *"Skip if hash already recorded under a different path, even outside TTL"* →
  step 3 skips regardless of TTL, because a hash → verdict is deterministic.
- *"Idempotent if a scan fails"* → `ok`/`corrupt` tasks are terminal and cached by
  hash; a re-run skips them in step 1/3. Transient failures retry (below).

**Transient errors & retries (#5, #8):** `error` (couldn't open) and `timeout`
are **not** verdicts — an NFS blip, a lock, or a still-importing file can cause
them, so they are **never content-cached**. The task retries with exponential
backoff up to `scan_max_attempts` (default 3); on exhaustion the task becomes
`unreadable` and is surfaced in the UI as needs-attention — never silently
dropped, and never confused with `corrupt`. The step-0 stability gate prevents
most mid-write false errors up front.

**Detector versioning — lazy revalidation (#7, #11):** cache validity keys on
both `detector_version` (bumped when detection logic/args change) and
`detector_backend`. A mismatch does **not** trigger an immediate library-wide
re-scan (which could be days of NFS I/O). The cached verdict is shown flagged
`stale`; the file is only re-scanned when it's next naturally due (TTL / content
change) or via an explicit, scope-previewed **"Revalidate library"** action.

**Hashing:** whole-file [blake3](https://github.com/oconnor663/blake3-py)
(multithreaded, far faster than sha256; the NFS read dominates on large remuxes).
The (size, mtime) fast-path (step 1) avoids reading the file at all on the common
"nothing changed" case. The extra full read on a cache miss (#2) is an accepted
cost — decided in favour of simple, exact whole-file dedup over a partial
signature. **Decided:** blake3 (multi-core headroom for local/cached scans, no
adversary to need SHA's standardisation); NFS read dominates so the choice is
low-stakes. `hash_algorithm` remains configurable to sha256.

---

## 4. Tech Stack

### Backend
- **Python 3.12**, **FastAPI** + **Uvicorn** (async orchestrator + REST + SSE).
- **PyAV** (libav bindings, in-process) as the primary integrity checker — honours
  the "prefer bindings over shelling out" preference. **subprocess ffmpeg** kept as
  a configurable fallback (§7).
- **blake3** for content hashing.
- **SQLModel** (SQLAlchemy 2.x core + Pydantic models) over **SQLite** (WAL mode).
- **Alembic** for migrations.
- **APScheduler** for cron-based job scheduling.
- **httpx** for Sonarr/Radarr and Pushover clients.
- **`pebble.ProcessPool`** for the scan workers — chosen over `ProcessPoolExecutor`
  for **per-task timeouts** and real **cancellation** (terminates the worker
  process), which stock futures cannot do (§6).
- **prometheus-client** for the `/metrics` endpoint (§14a).

### Frontend
- **React 18 + TypeScript + Vite**.
- **Tailwind CSS** + **shadcn/ui** (Radix primitives) for a clean, modern look.
- **TanStack Query** for server state; **React Router** for routing.
- **Recharts** for dashboard charts; **lucide-react** icons.
- **Server-Sent Events (SSE)** for live scan/run progress (one-directional, simpler
  than WebSockets and a perfect fit).

### Packaging / Ops
- Single **multi-stage Docker image**: build frontend → serve static assets from
  FastAPI, one container.
- Deployed to the **kube-saturn** cluster via Flux; SQLite on a **PVC**
  (volsync-backed), media mounted **read-only** (NFS from Synology).
- Tooling: **uv** (Python), **pnpm** (frontend), **ruff** + **mypy** + **pytest**,
  **vitest** + **Playwright** (frontend).

---

## 5. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ scanrr container (single process, single SQLite writer)        │
│                                                                │
│  FastAPI ──┬── REST API  (/api/*)                              │
│            └── SSE        (/api/events)                         │
│                                                                │
│  APScheduler ── triggers Jobs on cron → creates Job Runs       │
│                                                                │
│  Orchestrator (async)                                          │
│    • discovery: enumerate files → BATCH-insert scan_tasks      │
│    • pre-checks: TTL / hash-cache lookups (skip cheaply)       │
│    • dispatch: submit pending tasks to the pool (bounded)      │
│    • collect: results → detections → notify                    │
│                                                                │
│  DB writer thread (SOLE WRITER)  ← asyncio.Queue ← orchestrator │
│    serialises all writes; reads use a threadpool (WAL)         │
│                                                                │
│  pebble.ProcessPool  (N workers, pure CPU, NO DB access)       │
│    worker(path) -> (hash, status, error_log, duration_ms)      │
│    per-task timeout + cancellable ── blake3 + PyAV decode      │
│                                                                │
│  Integrations:  Sonarr/Radarr (httpx) · Pushover (httpx)       │
└──────────────────────────────────────────────────────────────┘
        │ read-only NFS mounts            │ PVC
        ▼                                 ▼
   Synology media shares            scanrr.db (SQLite/WAL)
```

**Single-writer design (#4).** SQLModel is synchronous, so DB calls must never run
on the asyncio event loop. All **writes** are funnelled through one dedicated
writer thread fed by an `asyncio.Queue`, preserving a single writer and keeping
the loop unblocked; **reads** run in a threadpool (WAL permits concurrent readers).
Discovery inserts are **batched** (`executemany`, commit every N rows) with
periodic WAL checkpoints so a 100k-file enumeration can't stall the API/SSE.
Worker processes never touch SQLite — they are pure functions returning results,
which also sidesteps multi-process write contention.

---

## 6. Job Queue & Concurrency Model

**The database is the queue.** A **single, global, deduplicated scan queue**
(`scan_tasks`) holds durable per-file work items shared across all runs; it
survives restarts.

### Runs are concurrent; file processing is a single shared queue (Q2)
Multiple `job_runs` may be **active at once** — a run is a *logical grouping* of
the files a job cares about, not an exclusive owner of the workers. All runs feed
one global queue, and workers drain it in **queue (FIFO) order**, interleaving
files from different runs. Throughput is still bounded by `max_scan_workers` (the
NFS-bandwidth throttle), so concurrency improves *responsiveness* (an ad-hoc run
starts immediately) without adding NFS thrash.

**Path deduplication.** A `scan_task` is unique by `path` while active
(`pending`/`scanning`). When a run's discovery needs to scan a path:
- if an active task for that path already exists → the run **subscribes** to it
  (`scan_task_subscribers`) instead of enqueuing a duplicate;
- otherwise it creates the task and subscribes.

The file is decoded **once**; on completion the outcome fans out to **every**
subscribed run (stats, detections, SSE), and is written once to `scan_results` /
`files`. This is dedup at the *processing* layer — content-addressed caching (§3)
still covers the already-scanned case; this covers the concurrent-pending case
two overlapping jobs would otherwise double-scan.

Only **active** (`pending`/`scanning`) tasks are dedup targets; a `done` task is
never reused — a later run instead takes the §3 cache path. And a task is dropped
only when it has **zero subscribers**; since only *cancelled* runs unsubscribe, no
still-active run can reference a dropped task. A permanent failure (`unreadable`)
fans out like any terminal outcome, so every non-cancelled run's `run_files`
reaches a terminal `outcome` and **finalization always progresses** (no orphaned
run waiting on a vanished task).

### Lifecycle of a run
1. **Trigger** — scheduler (cron) or manual (`POST /api/jobs/:id/run`) creates a
   `job_run` (`queued`), then `running`.
2. **Discovery** — resolve the source to a file list (walk dir, or query arr +
   apply path mappings); apply §3 pre-checks. Cheap skips are recorded against the
   run immediately; cache-miss files are enqueued/subscribed on the shared queue.
3. **Drain** — the dispatcher pulls the next `pending` task by `seq`, submits it to
   the pool (bounded in-flight), regardless of which runs it belongs to.
4. **Collect & fan-out** — on completion, write `scan_results`/`files` once,
   `reconcile_detections`, enqueue replacements, and credit **all** subscribed runs.
5. **Finalize** — a run completes when every file it referenced is terminal
   (skipped, or its shared task `done`/`unreadable`); aggregate stats, fire events.

### Concurrency knobs
- Global `max_scan_workers` (default **3**) and optional per-job override — the
  single NFS-bandwidth throttle across *all* concurrent runs. Deliberately low.
- Hashing and decoding run in separate **worker processes** (`pebble.ProcessPool`),
  giving true parallelism (separate processes, so no shared GIL at all).

### Timeouts & cancellation (#3)
- Every scan runs with a **per-file timeout** (`max_scan_seconds`, default 1800).
  `pebble` terminates the worker process on expiry; the task is recorded as a
  transient `timeout` (retryable per the §3 policy), so one pathological file can
  never wedge a worker indefinitely.
- `POST /api/runs/:id/cancel` sets the run `cancelling` and **unsubscribes** it
  from the shared queue. A pending/in-flight task is only stopped when it has **no
  remaining subscribers** — then `pebble` terminates the worker process (which
  stock `ProcessPoolExecutor` cannot) and the task is dropped. Tasks another active
  run still needs keep running. The run is then marked `cancelled`.

### Scheduling & misfires (#14)
- APScheduler runs each job with `coalesce=True` and `max_instances=1`, so a job
  can never overlap itself and a burst of missed triggers collapses to one run.
- `misfire_grace_time` is configurable (default 3600s). If a job's previous run is
  still active when its next trigger fires, the trigger is **skipped with a logged
  notice** rather than queued.

### Crash recovery / idempotent resume
- On startup: reset any `scanning` task → `pending` (re-drained in `seq` order);
  `running` runs simply resume against the shared queue via their existing
  subscriptions. Already-`done` tasks and cached results (§3) make re-processing a
  cheap skip. No duplication, no lost progress, no lost run/file associations.

---

## 7. FFmpeg Integrity Checking

Two interchangeable backends, validated to agree against a shared corrupted-media
fixture set (`backend/tests/test_integrity.py`). Implemented in
`backend/scanrr/scanning/integrity.py`.

**Primary — PyAV (in-process libav bindings):** open with aggressive error
detection, demux and decode every frame of every stream, and capture libav's
**ERROR-level log stream** — not just exceptions. This last point is the whole
game and was proven necessary by the M1 spike:

- libav *conceals* most decode errors (bad macroblocks, damaged GOPs, premature
  EOF) and returns the frame **successfully** — reporting the problem only via
  `av_log`. A loop that only catches `av.FFmpegError` reports these files `ok`
  (verified false negative on a truncated file). So we must read the log stream.
- Capture it through **Python's stdlib `logging`** (PyAV forwards libav logs to
  the `libav` logger), *not* `av.logging.Capture()`: `Capture()` is thread-local
  and misses errors emitted from libav's **decoder worker threads**. A stdlib
  logging handler on the `libav` logger is thread-safe and catches them.
- Disable `AV_LOG_SKIP_REPEATED` (`av.logging.set_skip_repeated(False)`): libav
  suppresses identical consecutive messages, so in a **reused worker process** a
  second file emitting the same error string would be misclassified `ok`.
- Constraint: the libav logger is process-global → one file decoded per process
  at a time (matches the worker-pool model, §6).

**Statuses:** `ok` (decoded clean), `corrupt` (opened + decoded but ERROR logs),
`error` (couldn't open/demux — mangled header, not media).

**Reference — subprocess** (config `detector_backend: pyav | subprocess`):
`ffmpeg -v error -err_detect aggressive -i <file> -map 0 -f null -`. Classify by
**exit code** (robust where stderr string-matching is not): `rc != 0` → `error`;
`rc == 0` + stderr → `corrupt`; `rc == 0` + empty → `ok`. `-map 0` decodes every
stream for parity with PyAV's `demux()`.

**Spike result:** both backends agree on the pass/fail verdict for clean,
bit-flipped, truncated, and header-corrupted samples. **[OPEN]** which is primary
in prod — decide on throughput once we benchmark on real 4K remuxes over NFS; the
subprocess contract is simpler, PyAV avoids a fork per file.

**Discovery filter:** configurable media extensions
(`.mkv .mp4 .avi .m4v .ts .mov .wmv .flv .webm .mpg .mpeg .m2ts` …) and a minimum
file size, to skip samples/artwork/subtitles.

---

## 8. Database Schema

SQLite, WAL mode, `busy_timeout=5000`, foreign keys on. DDL is indicative; Alembic
owns the canonical migrations.

**Conventions.** All timestamps are `TEXT` in **ISO-8601 UTC** (`YYYY-MM-DDTHH:MM:SSZ`).
**Retention:** `scan_results` is the durable content cache — intentionally permanent
(one row per distinct content ever seen). `scan_tasks` rows are pruned once `done`
and all subscribing runs have finalized (the per-run record lives on in `run_files`);
`notification_queue` rows are deleted after a successful flush.

```sql
-- Global key/value config (concurrency, hash algo, detector backend, ext list…)
CREATE TABLE settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,               -- JSON-encoded
    updated_at  TEXT NOT NULL
);

-- Job definitions
CREATE TABLE jobs (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    type          TEXT NOT NULL CHECK (type IN ('path','arr')),
    enabled       INTEGER NOT NULL DEFAULT 1,
    ttl_seconds   INTEGER NOT NULL,          -- rescan window
    schedule_cron TEXT,                      -- NULL = manual only
    config        TEXT NOT NULL,             -- JSON: {root_path} | {arr_instance_id}
    concurrency   INTEGER,                   -- NULL = use global default
    auto_replace  INTEGER NOT NULL DEFAULT 0,  -- enable arr re-request on corruption
    auto_approve_replacements INTEGER NOT NULL DEFAULT 0,
                  -- 0 = require human approval before deleting (default, Q3);
                  -- 1 = user opted to bypass approval and execute automatically
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

-- One execution of a job
CREATE TABLE job_runs (
    id                 INTEGER PRIMARY KEY,
    job_id             INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    status             TEXT NOT NULL           -- queued|running|completed|failed|
                       DEFAULT 'queued',       --   cancelling|cancelled|interrupted
    trigger            TEXT NOT NULL,           -- manual | scheduled
    started_at         TEXT,
    finished_at        TEXT,
    files_discovered   INTEGER NOT NULL DEFAULT 0,
    files_scanned      INTEGER NOT NULL DEFAULT 0,
    files_skipped      INTEGER NOT NULL DEFAULT 0,
    files_corrupt      INTEGER NOT NULL DEFAULT 0,
    files_unreadable   INTEGER NOT NULL DEFAULT 0,   -- transient failures exhausted
    error_message      TEXT
);
CREATE INDEX ix_job_runs_job ON job_runs(job_id, started_at);

-- Global, path-deduplicated scan queue shared by ALL runs (Q2). One row per file
-- currently needing work; unique by path while active so overlapping jobs never
-- double-scan. Workers drain by `seq` (FIFO).
CREATE TABLE scan_tasks (
    id             INTEGER PRIMARY KEY,   -- also the drain tiebreak within seq
    seq            INTEGER NOT NULL,   -- queue order (= insertion; a column so manual
                                       -- triggers can jump the queue); drain ascending
    path           TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
                   -- pending|scanning|done|unreadable
                   -- 'pending' also covers retry-backoff waiting (gated by
                   -- next_attempt_at); a transient failure returns the task to
                   -- 'pending' with attempts++ until scan_max_attempts -> 'unreadable'.
    content_hash   TEXT NOT NULL, -- blake3 computed at discovery; key into scan_results
    result_status  TEXT,          -- ok|corrupt|unreadable (fanned out to subscribers)
    attempts       INTEGER NOT NULL DEFAULT 0,   -- transient failures retried, §3
    next_attempt_at TEXT,         -- backoff gate; NULL = ready now
    error          TEXT,          -- last transient error / timeout detail
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
-- 'unreadable' = retries exhausted on a transient error/timeout (NOT corrupt).
-- Dedup: at most one ACTIVE task per path (done tasks are not dedup targets).
CREATE UNIQUE INDEX ux_scan_tasks_active_path
    ON scan_tasks(path) WHERE status IN ('pending','scanning');
CREATE INDEX ix_scan_tasks_drain ON scan_tasks(status, seq);

-- Which runs are waiting on a shared task; the outcome fans out to all of them.
CREATE TABLE scan_task_subscribers (
    scan_task_id INTEGER NOT NULL REFERENCES scan_tasks(id) ON DELETE CASCADE,
    job_run_id   INTEGER NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    PRIMARY KEY (scan_task_id, job_run_id)
);
CREATE INDEX ix_scan_task_subs_run ON scan_task_subscribers(job_run_id);

-- Per-run, per-file ledger: every file a run touched and its disposition. Skips
-- (which never enter the shared queue) live here too, powering the run-detail
-- view and per-run stats.
CREATE TABLE run_files (
    job_run_id   INTEGER NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    path         TEXT NOT NULL,
    disposition  TEXT NOT NULL,  -- queued|skipped_ttl|skipped_hash_cached|skipped_too_fresh
    outcome      TEXT,           -- ok|corrupt|unreadable  (NULL until its task finishes)
    scan_task_id INTEGER REFERENCES scan_tasks(id) ON DELETE SET NULL,
    PRIMARY KEY (job_run_id, path)
);

-- Path -> content mapping + scan bookkeeping
CREATE TABLE files (
    id               INTEGER PRIMARY KEY,
    path             TEXT NOT NULL UNIQUE,
    hash             TEXT,                    -- current content hash (blake3)
    size_bytes       INTEGER,
    mtime            REAL,
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    last_scanned_at  TEXT
);
CREATE INDEX ix_files_hash ON files(hash);

-- Content-addressed integrity verdict (the reusable cache).
-- Only DETERMINISTIC verdicts are cached: transient error/timeout outcomes are
-- never written here (they retry, §3). A row is reused only when BOTH
-- detector_version AND detector_backend match current config (#7, #11).
CREATE TABLE scan_results (
    hash             TEXT PRIMARY KEY,        -- blake3 of file content
    status           TEXT NOT NULL CHECK (status IN ('ok','corrupt')),
    error_log        TEXT,
    detector_version INTEGER NOT NULL,
    detector_backend TEXT NOT NULL,           -- pyav | subprocess
    scan_duration_ms INTEGER,
    scanned_at       TEXT NOT NULL
);

-- A corrupt file observed at a path, with remediation state
CREATE TABLE detections (
    id           INTEGER PRIMARY KEY,
    file_id      INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    hash         TEXT NOT NULL,
    job_run_id   INTEGER REFERENCES job_runs(id) ON DELETE SET NULL,
    status       TEXT NOT NULL DEFAULT 'open',
                 -- open|acknowledged|replacing|resolved|ignored|needs_attention
                 -- resolved: a later clean scan of this path cleared it (#6)
                 -- needs_attention: replacement attempts exhausted, still corrupt
    detected_at  TEXT NOT NULL,
    resolved_at  TEXT,
    UNIQUE (file_id, hash)
);
CREATE INDEX ix_detections_status ON detections(status);
CREATE INDEX ix_detections_file ON detections(file_id);

-- Sonarr/Radarr endpoints
CREATE TABLE arr_instances (
    id         INTEGER PRIMARY KEY,
    type       TEXT NOT NULL CHECK (type IN ('sonarr','radarr')),
    name       TEXT NOT NULL,
    base_url   TEXT NOT NULL,
    api_key    TEXT NOT NULL,                 -- encrypted at rest (§14)
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

-- arr-namespace path -> scanrr local mount path (longest-prefix match)
CREATE TABLE path_mappings (
    id               INTEGER PRIMARY KEY,
    arr_instance_id  INTEGER NOT NULL REFERENCES arr_instances(id) ON DELETE CASCADE,
    remote_path      TEXT NOT NULL,           -- e.g. /data/media/tv
    local_path       TEXT NOT NULL            -- e.g. /mnt/tv
);

-- Links a scanned file to its arr media item (populated during arr discovery)
CREATE TABLE file_arr_links (
    file_id          INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    arr_instance_id  INTEGER NOT NULL REFERENCES arr_instances(id) ON DELETE CASCADE,
    media_type       TEXT NOT NULL,           -- episode | movie
    media_id         INTEGER NOT NULL,        -- series/episode or movie id
    arr_file_id      INTEGER NOT NULL,        -- episodeFile / movieFile id
    PRIMARY KEY (file_id, arr_instance_id)
);
-- Reverse lookup used when arr history/webhook references a file by its arr id.
CREATE INDEX ix_file_arr_links_arrfile ON file_arr_links(arr_instance_id, arr_file_id);

-- Re-request attempts (one row per attempt; capped per detection -- §9, #6)
CREATE TABLE replacements (
    id           INTEGER PRIMARY KEY,
    detection_id INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    attempt      INTEGER NOT NULL DEFAULT 1,        -- 1..max_replace_attempts
    arr_instance_id INTEGER REFERENCES arr_instances(id) ON DELETE SET NULL,
    media_type   TEXT,
    media_id     INTEGER,
    arr_file_id  INTEGER,
    status       TEXT NOT NULL DEFAULT 'pending_approval',
                 -- pending_approval|approved|rejected|requested|searching|grabbed|
                 --   imported|verifying|succeeded|failed|exhausted|aborted
                 -- pending_approval: awaiting human OK to delete (Q3 default)
                 -- rejected: user declined; no deletion happens
                 -- verifying: re-scanning the imported file to confirm it's clean
                 -- succeeded: re-scan came back ok   exhausted: attempts used up
    approved_by  TEXT,          -- who approved (or 'auto' when bypassed via job flag)
    approved_at  TEXT,
    requested_at TEXT,           -- set when the search is actually issued
    updated_at   TEXT NOT NULL,
    notes        TEXT
);
CREATE INDEX ix_replacements_detection ON replacements(detection_id);
CREATE INDEX ix_replacements_status ON replacements(status);  -- pending_approval queue

-- Notification config + audit
CREATE TABLE notification_channels (
    id       INTEGER PRIMARY KEY,
    type     TEXT NOT NULL DEFAULT 'pushover',
    config   TEXT NOT NULL,                   -- JSON {user_key, api_token, priority} (encrypted)
    enabled  INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE notification_rules (
    channel_id INTEGER NOT NULL REFERENCES notification_channels(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,     -- scan_started|scan_completed|corrupt_found|
                                  --   replacement_pending_approval|replacement_requested|
                                  --   replacement_completed|job_failed  (canonical list: §10)
    enabled    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (channel_id, event_type)
);
-- Outbound notification QUEUE (Q5). Events enqueue here instead of sending
-- inline; a periodic flusher drains it (individual vs batched by threshold, §10).
CREATE TABLE notification_queue (
    id          INTEGER PRIMARY KEY,
    event_type  TEXT NOT NULL,   -- scan_started|scan_completed|corrupt_found|...
    dedup_key   TEXT,            -- collapse duplicates before flush (e.g. per file)
    payload     TEXT NOT NULL,   -- JSON event detail (path, run id, counts, …)
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | sent | failed
    created_at  TEXT NOT NULL,
    sent_at     TEXT
);
CREATE INDEX ix_notification_queue_pending ON notification_queue(status, event_type, created_at);

CREATE TABLE notification_log (
    id          INTEGER PRIMARY KEY,
    event_type  TEXT NOT NULL,
    channel_id  INTEGER REFERENCES notification_channels(id) ON DELETE SET NULL,
    payload     TEXT,
    batched     INTEGER NOT NULL DEFAULT 0,  -- 1 = this send covered multiple events
    status      TEXT NOT NULL,               -- sent | failed
    error       TEXT,
    created_at  TEXT NOT NULL
);
```

---

## 9. Sonarr / Radarr Integration

### Discovery (arr-type jobs)
- **Sonarr** (API v3): enumerate `GET /api/v3/series` → per series
  `GET /api/v3/episodefile?seriesId=…`; collect `episodeFile.path`, `episodeFileId`,
  episode id. **Radarr:** `GET /api/v3/movie` → `movieFile.path`, `movieFileId`,
  movie id.
- **Path mapping:** arr returns paths in *its* namespace. Apply the longest-prefix
  `path_mappings` rule for that instance to translate to scanrr's local mount, then
  scan as usual. Record the arr linkage in `file_arr_links` so remediation is possible.
- Files that don't match any mapping (or aren't present on the mount) are flagged as
  discovery warnings, not scanned.

### Auto-replacement (destructive — opt-in per job)
When a corrupt detection has an arr link and the job has `auto_replace`, the run
**finishes scanning first**, then proposes replacements:

1. **Propose & gate (Q3).** For each corrupt arr-linked file, create a
   `replacement` in `pending_approval` capturing the intended delete + search. If
   the job has `auto_approve_replacements = 0` (**default**), the run completes and
   the UI shows *"N files will be deleted & re-requested"* for the user to
   **approve or reject** (individually or as a batch). If `auto_approve_replacements
   = 1` (user explicitly opted to bypass), rows are auto-approved (`approved_by =
   'auto'`) and proceed immediately. A **per-run deletion cap** (`max_deletions_per_run`,
   default 25) aborts and raises `needs_attention` if a single run would delete more
   than the cap — a guard against a path-mapping mistake, especially when approval
   is bypassed.
2. **Delete** (only after approval): Sonarr `DELETE /api/v3/episodefile/:id`,
   Radarr `DELETE /api/v3/moviefile/:id`.
3. **Search:** `POST /api/v3/command` (`EpisodeSearch` / `MoviesSearch`).
   Fire `replacement_requested`, set `requested_at`.
4. **Bounded polling (Q4).** A poller checks arr `history`/`queue` for that media
   id every `replacement_poll_interval` (default 120s), advancing `searching` →
   `grabbed` → `imported`, up to `replacement_search_timeout` (default 12h). On
   timeout with no grab → `failed`, detection `needs_attention`, notify
   ("no release found"). Self-contained — needs no arr-side webhook config.
   (Webhook-driven confirmation is a possible future optimisation.)
5. **Verify (#6):** on import, enqueue a **targeted re-scan of the new file**.
   - clean → `succeeded`, resolve the detection, fire `replacement_completed`.
   - still corrupt → if `attempt < max_replace_attempts` (default 2) start the next
     attempt at step 2; otherwise `exhausted`, detection `needs_attention`, notify.
     Closes the loop; prevents an endless delete → grab-bad-release → delete cycle.

**Safety:** `auto_replace` defaults **off**. Approval is required by default and
must be explicitly disabled per job. Deletions are irreversible, so the UI shows a
clear warning, the per-run cap bounds blast radius, and every action is audited in
`replacements`.

---

## 10. Notifications (Pushover)

Events: `scan_started`, `scan_completed` (with summary counts),
`corrupt_found`, `replacement_pending_approval`, `replacement_requested`,
`replacement_completed`, `job_failed`.

**Queue + periodic flush (Q5).** Events do **not** send inline. Producers enqueue
into `notification_queue`; a scheduled **flusher** runs every
`notification_flush_interval` (default 300s) and drains pending events:
- grouped per channel + `event_type`, duplicates collapsed by `dedup_key`;
- if a group has **fewer than `notification_batch_threshold`** (default 5) events →
  send them **individually** (timely per-file alerts in steady state);
- otherwise send **one batched digest** ("47 corrupt files found — see scanrr"),
  avoiding a push-storm on a big first scan and Pushover rate limits.

This decouples detection from delivery: sends never block scanning, transient
Pushover failures retry on the next flush, and `notification_log` records each send
(with `batched`).

- Per-channel, per-event toggles (`notification_rules`); the flusher honours them.
- Provider abstracted behind a `NotificationChannel` interface so more backends
  (ntfy, Discord, …) can be added later without schema churn.

---

## 11. REST API & Realtime

Base: `/api`. JSON throughout.

| Method | Route | Purpose |
|---|---|---|
| GET/POST | `/jobs` | List / create jobs |
| GET/PUT/DELETE | `/jobs/:id` | Read / update / delete a job |
| POST | `/jobs/:id/run` | Trigger a run now |
| GET | `/runs` · `/runs/:id` | Run history / detail + stats |
| POST | `/runs/:id/cancel` | Cancel a running job |
| GET | `/runs/:id/files` | This run's per-file ledger (`run_files`: disposition + outcome), paged |
| GET | `/detections` | Corrupt files (filter by status) |
| POST | `/detections/:id/replace` | Manually propose a replacement (→ `pending_approval`) |
| POST | `/detections/:id/acknowledge` · `/ignore` · `/resolve` | Triage transitions |
| GET | `/replacements` | List replacements (filter by status, e.g. `pending_approval`) |
| POST | `/replacements/:id/approve` · `/reject` | Approve/reject a proposed deletion (Q3) |
| POST | `/replacements/approve` | Bulk-approve a batch (body: ids or run id) |
| GET | `/files` | Search scanned files (path/hash/status) |
| GET/POST | `/arr-instances` · `/arr-instances/:id` | Manage arr endpoints |
| POST | `/arr-instances/:id/test` | Connection test |
| GET/POST/DELETE | `/path-mappings` | Manage path mappings |
| GET/PUT | `/settings` | Global settings |
| GET/PUT | `/notifications` | Channels + rules |
| POST | `/notifications/test` | Send a test push |
| POST | `/library/revalidate` | Re-scan against current detector (scope-previewed, rate-limited) (#7) |
| GET | `/stats` | Dashboard aggregates |
| GET | `/events` | **SSE** stream: run progress, task updates, new detections |
| GET | `/metrics` | Prometheus metrics (§14a) |
| GET | `/health` | Liveness/readiness |

**Auth (#9):** all **mutating** routes (POST/PUT/DELETE — especially the
destructive `/detections/:id/replace` and `auto_replace` config) require a shared
secret (`X-Scanrr-Token`, from an env/k8s Secret), enforced by middleware. GET
routes and `/health`/`/metrics` are open behind Cloudflare Zero Trust. This is
defense-in-depth against other in-cluster callers, not a user-auth system.

**Realtime:** clients subscribe to `/api/events` (SSE). The orchestrator publishes
`run.progress`, `task.updated`, `detection.created`, `run.completed` events; the UI
updates live without polling. TanStack Query caches are invalidated on relevant events.

---

## 12. UI Views & Routes

Modern, dark-mode-first, shadcn/ui components. Left nav + content.

| Route | View | Contents |
|---|---|---|
| `/` | **Dashboard** | Active runs (live progress bars), library health donut (ok/corrupt/unreadable), recent runs, open-detection count, scan-throughput chart. |
| `/jobs` | **Jobs** | Cards/table of jobs: type, schedule, TTL, last run, status; Run-now, enable/disable, edit, delete. |
| `/jobs/new`, `/jobs/:id` | **Job editor** | Type (path/arr), source config, TTL, cron builder, concurrency, `auto_replace` + `auto_approve_replacements` toggles (with warning), + run history for existing jobs. |
| `/runs/:id` | **Run detail** | Live phase indicator, aggregate stats, streaming per-file table (path · disposition/status · outcome · duration), cancel. |
| `/detections` | **Corrupt files** | The triage list: path, detected date, run, status; expandable ffmpeg error log; actions: replace, acknowledge, ignore, resolve. Bulk actions. |
| `/replacements` | **Replacements** | Proposed deletions awaiting **approval** (approve/reject, per-item or batch), plus in-flight/verifying/exhausted history. |
| `/files` | **Files** | Searchable scan history across the library (path, hash, last scanned, verdict). |
| `/settings` | **Settings hub** | Tabs below. |
| `/settings/general` | General | Concurrency, hash algo, detector backend, media extensions, min size, stability gate, scan timeout, retry/replacement caps. |
| `/settings/integrations` | Integrations | Sonarr/Radarr instances (add/test), path mappings editor. |
| `/settings/notifications` | Notifications | Pushover keys, per-event toggles, flush interval + batch threshold, test button. |

Design touches: live-updating progress via SSE, optimistic triage actions, empty
states, toast on notifications, colour-coded status badges (green ok / amber
unreadable / red corrupt).

---

## 13. Configuration & Settings

Layered: **env vars** (deploy-time: DB path, media mount roots, encryption key,
**mutating-route shared secret**, log level) → **`settings` table** (runtime-editable
via UI). **This section is the canonical list of tunables and their defaults**
(values echoed in other sections' comments are illustrative — defaults live here):
`max_scan_workers`, `hash_algorithm`, `detector_backend`, `media_extensions`,
`min_file_size_bytes`, `min_file_age_seconds` (stability gate, default 120),
`max_scan_seconds` (per-file timeout, default 1800),
`scan_max_attempts` (transient-failure retries, default 3),
`misfire_grace_time` (default 3600).
Replacement: `max_replace_attempts` (default 2), `max_deletions_per_run`
(default 25), `replacement_poll_interval` (default 120s),
`replacement_search_timeout` (default 12h).
Notifications: `notification_flush_interval` (default 300s),
`notification_batch_threshold` (default 5).

---

## 14. Security & Safety

- **Secrets at rest:** arr API keys and Pushover tokens encrypted with a key from
  env/k8s Secret (Fernet). Never returned in plaintext by the API.
- **Media mounts read-only** — scanrr never writes to the library. The only writes
  to arr-managed files are explicit `auto_replace` deletions via the arr API.
- **Destructive ops gated:** `auto_replace` off by default; deletions require
  **human approval** by default (bypass is an explicit per-job opt-in), bounded by a
  per-run deletion cap and per-detection attempt cap, with a full audit trail in
  `replacements`.
- **In-cluster authz (#9):** mutating API routes require the `X-Scanrr-Token`
  shared secret (§11), so a compromised/rogue pod can't trigger arr deletions even
  inside the cluster edge.
- **Deployment:** behind Cloudflare Zero Trust (owner-only) like the rest of the
  homelab; no built-in *user* auth in v1. Add a Zero Trust entry per the kube-saturn
  CLAUDE.md workflow when deploying.

---

## 14a. Observability (#15)

- **Structured logs (JSON):** every per-file decision is logged with its reason —
  `scanned` (with verdict + duration), `skipped` (with `disposition`), `retry`,
  `unreadable`, `timeout` — so "why did/didn't this file get scanned?" is
  answerable after the fact without re-deriving it from the DB.
- **`/metrics` (Prometheus):** counters (`files_scanned_total`,
  `corrupt_found_total`, `replacements_total`, `scan_errors_total`), a scan-duration
  histogram, and gauges for queue depth and active workers. Scrapeable by the
  homelab Prometheus for dashboards/alerts.
- **`notification_log`** remains the audit trail for outbound events.

---

## 15. Deployment

- Multi-stage Dockerfile (pnpm build frontend → copy into Python image → uvicorn).
- k8s (kube-saturn / Flux): Deployment + Service + PVC (SQLite, volsync-backed) +
  read-only NFS media mounts + Cloudflare tunnel ingress + Zero Trust policy.
  ffmpeg/libav provided by the base image (PyAV wheels bundle libav, or install ffmpeg).
- Single replica (SQLite writer + in-process scheduler are not HA); liveness/readiness
  on `/api/health`.

---

## 16. Repository Layout

```
scanrr/
├── SPEC.md                 # this document (source of truth)
├── README.md
├── backend/
│   ├── pyproject.toml
│   ├── alembic/
│   └── scanrr/
│       ├── main.py         # FastAPI app + lifespan (scheduler, orchestrator)
│       ├── api/            # routers
│       ├── core/           # config, settings, security, events (SSE bus)
│       ├── db/             # engine, session, models (SQLModel)
│       ├── scanning/       # hashing, ffmpeg integrity, worker fn, orchestrator
│       ├── jobs/           # discovery (path, arr), scheduler, queue
│       └── integrations/   # sonarr, radarr, pushover clients
├── frontend/
│   ├── package.json
│   └── src/                # React app (routes, components, api client, sse)
└── deploy/
    ├── Dockerfile
    └── k8s/                # or a reference into kube-saturn
```

---

## 17. Open Questions

_All resolved for v1 — kept as a decision log._

1. ~~blake3 vs sha256 default hash.~~ **blake3** (configurable to sha256).
2. ~~Concurrent runs vs global serialize.~~ **Concurrent runs over one global,
   path-deduplicated FIFO queue** (§6).
3. ~~Auto-replace safety on first enable.~~ **Human approval required by default**;
   per-job `auto_approve_replacements` opt-out; per-run deletion cap (§9).
4. ~~Confirm replacement via polling vs fire-and-forget.~~ **Bounded polling** of arr
   history with a give-up timeout (§9).
5. ~~Per-file vs batched corrupt notifications.~~ **Queued + periodic flush**;
   individual under `notification_batch_threshold`, else batched digest (§10).

Remaining `[OPEN]` in-line: primary detector backend in prod (§7) — pending an
NFS throughput benchmark.

---

## 18. Milestones

- **M1 — Core scan engine:** SQLite schema, path jobs, blake3 + PyAV integrity,
  content-addressed idempotency, manual run, minimal run/detection API. CLI-testable.
- **M2 — Scheduling & queue:** APScheduler, durable `scan_tasks` queue, worker pool,
  crash recovery, TTL fast-path.
- **M3 — UI:** dashboard, jobs, run detail (live SSE), detections triage, settings.
- **M4 — Arr integration:** discovery + path mapping, `file_arr_links`, manual replace.
- **M5 — Auto-replace & notifications:** opt-in re-request lifecycle, Pushover events.
- **M6 — Deploy:** Docker image, kube-saturn manifests, Zero Trust, volsync PVC.
