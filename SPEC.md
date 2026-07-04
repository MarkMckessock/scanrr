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
| **Scan Task** | One file's work item inside a run — the durable queue row. |
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

For each file discovered during a run:

```
stat = os.stat(path)

# 0. Stability gate — never scan a file that may still be written/importing.
if (now - stat.mtime) < min_file_age_seconds:      # default 120s
    skip(reason="too_fresh"); continue

f = files.get(path)

# 1. Cheapest path — no disk read at all. last_scanned_at is GLOBAL (a scan is a
#    scan regardless of which job did it); TTL is evaluated against it.
if f and f.size == stat.size and f.mtime == stat.mtime
       and f.last_scanned_at and (now - f.last_scanned_at) < job.ttl:
    skip(reason="unchanged_within_ttl"); continue

# 2. Content identity — one full read to hash (blake3). On a cache miss this is a
#    second full read on top of the decode; accepted for the dedup benefit (#2).
h = hash_file(path)

# 3. Content-addressed cache hit — reused only if the cached verdict is still
#    valid for the CURRENT detector (version AND backend). A mismatch is treated
#    as a miss (falls through), NOT a forced library-wide rescan — see lazy
#    revalidation below.
sr = scan_results.get(h)
if sr and sr.detector_version == CURRENT_DETECTOR_VERSION
       and sr.detector_backend == CURRENT_DETECTOR_BACKEND:
    files.upsert(path, hash=h, size, mtime, last_scanned_at=now)
    reconcile_detections(path, h, sr.status, run)  # open if corrupt; resolve if now-ok
    skip(reason="hash_cached"); continue           # same bytes already scanned

# 4. Cache miss — the expensive full ffmpeg integrity check, bounded by a timeout.
result = ffmpeg_integrity_check(path, timeout=max_scan_seconds)  # §6, §7
if result.status in ("ok", "corrupt"):             # deterministic verdicts only
    scan_results.upsert(h, result, DETECTOR_VERSION, DETECTOR_BACKEND)
    files.upsert(path, hash=h, size, mtime, last_scanned_at=now)
    reconcile_detections(path, h, result.status, run)
    if result.status == "corrupt" and job.auto_replace:
        enqueue_replacement(detection)             # §9
else:                                              # error / timeout = TRANSIENT
    retry_or_fail(task)                            # NOT cached by hash; see retry policy
```

`reconcile_detections` closes the loop on remediation (#6): a `corrupt` verdict
opens (or reuses) a detection; an `ok` verdict on a path that had an **open
detection for a different hash** auto-resolves it (`status=resolved`). So once a
file is replaced with a clean copy, its old detection clears itself.

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

**The database is the queue.** `scan_tasks` rows are durable work items; the queue
survives restarts.

### Lifecycle of a run
1. **Trigger** — scheduler (cron) or manual (`POST /api/jobs/:id/run`) creates a
   `job_run` (`queued`).
2. **Discovery** — orchestrator resolves the source to a file list (walk dir, or
   query arr + apply path mappings), inserting one `scan_task` (`pending`) per file.
   Run → `running`.
3. **Pre-check & dispatch** — orchestrator streams pending tasks, applies §3 steps
   1–3 (cheap skips need no worker), and submits real work to the pool up to the
   concurrency limit (backpressure = bounded in-flight futures).
4. **Collect** — as futures resolve, the orchestrator writes `scan_results`,
   updates `files`, creates `detections`, enqueues replacements, fires notifications,
   and advances `scan_task` state.
5. **Finalize** — when all tasks are terminal, aggregate stats onto `job_run`,
   set `completed`/`failed`, fire `scan_completed`.

### Concurrency
- Global `max_scan_workers` (default **3**) and optional per-job override. Scanning
  is **NFS-read-bandwidth bound**, not CPU bound — too many parallel decodes thrash
  the network share, so the default is deliberately low. Documented in Settings.
- Hashing and decoding run in separate **worker processes** (`pebble.ProcessPool`),
  so they parallelise independently of the main process's GIL. (Being separate
  processes, they don't share a GIL at all — the point is true parallelism, not
  GIL release.)
- **[OPEN]** Whether two different Jobs may run concurrently, or runs are globally
  serialized with a shared worker pool. *Recommended:* one active run at a time in
  v1 (simpler, avoids double-scanning overlapping paths); queue additional triggers.

### Timeouts & cancellation (#3)
- Every scan runs with a **per-file timeout** (`max_scan_seconds`, default 1800).
  `pebble` terminates the worker process on expiry; the task is recorded as a
  transient `timeout` (retryable per the §3 policy), so one pathological file can
  never wedge a worker indefinitely.
- `POST /api/runs/:id/cancel` sets the run `cancelling`; the orchestrator stops
  dispatching and **terminates in-flight worker processes** (`pebble` supports
  this — stock `ProcessPoolExecutor` does not), requeuing their `scan_tasks` to
  `pending`, then marks the run `cancelled`.

### Scheduling & misfires (#14)
- APScheduler runs each job with `coalesce=True` and `max_instances=1`, so a job
  can never overlap itself and a burst of missed triggers collapses to one run.
- `misfire_grace_time` is configurable (default 3600s). If a job's previous run is
  still active when its next trigger fires, the trigger is **skipped with a logged
  notice** rather than queued.

### Crash recovery / idempotent resume
- On startup: any `job_run` left `running` → `interrupted`; its `scanning` tasks →
  `pending`. The next run (or an immediate resume) re-processes pending tasks; §3
  makes already-done work a cheap skip. No duplication, no lost progress.

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
    auto_replace  INTEGER NOT NULL DEFAULT 0,
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

-- Durable per-file queue rows for a run
CREATE TABLE scan_tasks (
    id             INTEGER PRIMARY KEY,
    job_run_id     INTEGER NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    path           TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
                   -- pending|scanning|done|skipped|unreadable  (see §3 retries)
    skip_reason    TEXT,          -- unchanged_within_ttl|hash_cached|too_fresh
    result_hash    TEXT,          -- FK-ish -> scan_results.hash
    attempts       INTEGER NOT NULL DEFAULT 0,   -- transient failures retried, §3
    next_attempt_at TEXT,         -- backoff gate; NULL = ready now
    error          TEXT,          -- last transient error / timeout detail
    updated_at     TEXT NOT NULL
);
-- 'unreadable' = retries exhausted on a transient error/timeout (NOT corrupt).
CREATE INDEX ix_scan_tasks_run_status ON scan_tasks(job_run_id, status);

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

-- Re-request attempts (one row per attempt; capped per detection -- §9, #6)
CREATE TABLE replacements (
    id           INTEGER PRIMARY KEY,
    detection_id INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    attempt      INTEGER NOT NULL DEFAULT 1,        -- 1..max_replace_attempts
    arr_instance_id INTEGER REFERENCES arr_instances(id) ON DELETE SET NULL,
    media_type   TEXT,
    media_id     INTEGER,
    arr_file_id  INTEGER,
    status       TEXT NOT NULL DEFAULT 'requested',
                 -- requested|searching|grabbed|imported|verifying|
                 --   succeeded|failed|exhausted|aborted
                 -- verifying: re-scanning the imported file to confirm it's clean
                 -- succeeded: re-scan came back ok   exhausted: attempts used up
    requested_at TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    notes        TEXT
);

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
                                  --   replacement_requested|replacement_completed|job_failed
    enabled    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (channel_id, event_type)
);
CREATE TABLE notification_log (
    id          INTEGER PRIMARY KEY,
    event_type  TEXT NOT NULL,
    channel_id  INTEGER REFERENCES notification_channels(id) ON DELETE SET NULL,
    payload     TEXT,
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
When a corrupt detection has an arr link and the job has `auto_replace`:
1. **Delete** the bad file: Sonarr `DELETE /api/v3/episodefile/:id`,
   Radarr `DELETE /api/v3/moviefile/:id`.
2. **Search** for a replacement: `POST /api/v3/command`
   (`EpisodeSearch` / `MoviesSearch` with the relevant id). Fire `replacement_requested`.
3. Poll arr history to advance `grabbed` → `imported`.
4. **Verify (#6):** on import, enqueue a **targeted re-scan of the new file**.
   - clean → `succeeded`, resolve the detection, fire `replacement_completed`.
   - still corrupt → if `attempt < max_replace_attempts` (default 2) start the next
     attempt at step 1; otherwise stop, mark the replacement `exhausted` and the
     detection `needs_attention`, and notify. This closes the loop and prevents an
     endless delete → grab-bad-release → delete cycle.

**Safety:** `auto_replace` defaults **off**. A **dry-run** mode logs the intended
delete+search without executing. Deletions are irreversible, so the UI requires an
explicit toggle and shows a clear warning. **[OPEN]** dry-run default on first enable?

---

## 10. Notifications (Pushover)

Events: `scan_started`, `scan_completed` (with summary counts),
`corrupt_found`, `replacement_requested`, `replacement_completed`, `job_failed`.

- Per-channel, per-event toggles (`notification_rules`); every send audited in
  `notification_log`.
- **Anti-spam:** `corrupt_found` is **batched** — a run summarises new detections in
  the `scan_completed` message rather than firing one push per bad file (a single
  run can surface dozens). A per-file push is available behind a setting for small libraries.
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
| GET | `/runs/:id/tasks` | Per-file task state (paged) |
| GET | `/detections` | Corrupt files (filter by status) |
| POST | `/detections/:id/replace` | Manually request replacement |
| POST | `/detections/:id/acknowledge` · `/ignore` · `/resolve` | Triage transitions |
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
| `/jobs/new`, `/jobs/:id` | **Job editor** | Type (path/arr), source config, TTL, cron builder, concurrency, `auto_replace` toggle (with warning), + run history for existing jobs. |
| `/runs/:id` | **Run detail** | Live phase indicator, aggregate stats, streaming per-file table (path · status · skip reason · duration), cancel. |
| `/detections` | **Corrupt files** | The triage list: path, detected date, run, status; expandable ffmpeg error log; actions: replace, acknowledge, ignore, resolve. Bulk actions. |
| `/files` | **Files** | Searchable scan history across the library (path, hash, last scanned, verdict). |
| `/settings` | **Settings hub** | Tabs below. |
| `/settings/general` | General | Concurrency, hash algo, detector backend, media extensions, min size, stability gate, scan timeout, retry/replacement caps. |
| `/settings/integrations` | Integrations | Sonarr/Radarr instances (add/test), path mappings editor. |
| `/settings/notifications` | Notifications | Pushover keys, per-event toggles, batching, test button. |

Design touches: live-updating progress via SSE, optimistic triage actions, empty
states, toast on notifications, colour-coded status badges (green ok / amber
unreadable / red corrupt).

---

## 13. Configuration & Settings

Layered: **env vars** (deploy-time: DB path, media mount roots, encryption key,
**mutating-route shared secret**, log level) → **`settings` table** (runtime-editable
via UI). Notable runtime settings:
`max_scan_workers`, `hash_algorithm`, `detector_backend`, `media_extensions`,
`min_file_size_bytes`, `min_file_age_seconds` (stability gate, default 120),
`max_scan_seconds` (per-file timeout, default 1800),
`scan_max_attempts` (transient-failure retries, default 3),
`max_replace_attempts` (default 2),
`corrupt_notification_mode` (batched|per_file), `serialize_runs`,
`misfire_grace_time` (default 3600).

---

## 14. Security & Safety

- **Secrets at rest:** arr API keys and Pushover tokens encrypted with a key from
  env/k8s Secret (Fernet). Never returned in plaintext by the API.
- **Media mounts read-only** — scanrr never writes to the library. The only writes
  to arr-managed files are explicit `auto_replace` deletions via the arr API.
- **Destructive ops gated:** `auto_replace` off by default, dry-run available,
  explicit UI confirmation, capped attempts, full audit trail in `replacements`.
- **In-cluster authz (#9):** mutating API routes require the `X-Scanrr-Token`
  shared secret (§11), so a compromised/rogue pod can't trigger arr deletions even
  inside the cluster edge.
- **Deployment:** behind Cloudflare Zero Trust (owner-only) like the rest of the
  homelab; no built-in *user* auth in v1. Add a Zero Trust entry per the kube-saturn
  CLAUDE.md workflow when deploying.

---

## 14a. Observability (#15)

- **Structured logs (JSON):** every per-file decision is logged with its reason —
  `scanned` (with verdict + duration), `skipped` (with `skip_reason`), `retry`,
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

1. ~~blake3 vs sha256 as the default hash.~~ **Decided: blake3** (configurable to sha256).
2. Allow concurrent runs across different jobs, or serialize globally? *(rec: serialize v1)*
3. Dry-run auto-replace on first enable by default?
4. Poll arr history to confirm `replacement_completed`, or fire-and-forget at request?
5. Per-file vs batched `corrupt_found` default. *(rec: batched)*

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
