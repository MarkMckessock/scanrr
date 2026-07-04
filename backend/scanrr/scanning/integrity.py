"""Video integrity checking.

Two interchangeable backends that must produce equivalent verdicts:

* ``pyav``      -- decodes every frame in-process via libav (PyAV) and captures
                   libav's ERROR-level log stream. Capturing the *log* stream --
                   not just exceptions -- is essential: libav conceals most
                   decode errors (bad macroblocks, damaged GOPs) and returns the
                   frame successfully, reporting the problem only via ``av_log``.
                   A loop that only catches exceptions silently passes exactly
                   the corruption we care about.

* ``subprocess`` -- the reference approach: ``ffmpeg -v error -i FILE -f null -``.
                   A file is corrupt iff ffmpeg writes anything to stderr.

The two are validated against a shared fixture set in the test-suite so we can
swap backends (or upgrade libav/ffmpeg) without silently regressing detection.

See SPEC.md sec.7.
"""

from __future__ import annotations

import logging
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass

from scanrr.enums import DetectorBackend
from scanrr.enums import DetectorStatus as Status

# Bumped when detection logic changes in a way that invalidates cached verdicts
# (SPEC.md sec.3 -- detector_version).
DETECTOR_VERSION = 1

# Aggressive error detection surfaces CRC/bitstream problems that the default
# decode path tolerates. Applied identically to both backends for parity.
_ERR_DETECT = "aggressive"

# Cap stored/returned log size -- a badly mangled file can emit thousands of
# lines and we don't want to blow up the DB row or the IPC payload.
_MAX_LOG_CHARS = 16_000


@dataclass
class Outcome:
    status: Status  # DetectorStatus: OK | CORRUPT | ERROR
    log: str = ""
    backend: DetectorBackend | None = None
    duration_ms: int = 0
    frames_decoded: int = 0
    error_count: int = 0

    @property
    def is_corrupt(self) -> bool:
        return self.status is Status.CORRUPT


def _truncate(text: str) -> str:
    if len(text) <= _MAX_LOG_CHARS:
        return text
    return text[:_MAX_LOG_CHARS] + f"\n... [truncated, {len(text)} chars total]"


# libav routes its logs to Python's stdlib logging under this logger name
# (e.g. "libav.h264", "libav.matroska,webm"). Tapping the parent logger captures
# every component -- and, crucially, is thread-safe, so it catches errors emitted
# from libav's internal decoder worker threads. PyAV's own av.logging.Capture()
# is thread-local by default and silently misses those (a truncated file's
# "File ended prematurely" is logged from a decode thread).
_LIBAV_LOGGER = "libav"


class _ErrorCollector(logging.Handler):
    """Collects ERROR+ libav log records emitted while decoding one file."""

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        name = record.name.removeprefix(_LIBAV_LOGGER + ".")
        self.messages.append(f"{name}: {record.getMessage()}".strip())


# --------------------------------------------------------------------------- #
# PyAV backend
# --------------------------------------------------------------------------- #
def check_pyav(
    path: str, *, on_progress: Callable[[float, float, int], None] | None = None
) -> Outcome:
    """Integrity check using in-process libav decoding, with libav's ERROR log
    stream captured via Python logging.

    Assumes one file is decoded per process at a time (our worker-pool model --
    SPEC.md sec.6): the libav logger is process-global, so concurrent decodes in
    a single process would cross-contaminate captured errors.

    ``on_progress(current_s, total_s, frames)`` is invoked (throttled to ~2s) as
    decoding advances through the file, so callers can surface live per-file
    progress -- current_s is the furthest packet PTS reached, total_s the
    container duration (0 if unknown).
    """
    import av
    import av.error
    import av.logging

    av.logging.set_level(av.logging.ERROR)  # match `ffmpeg -v error`: ERROR+ only
    # libav suppresses identical consecutive messages by default. In a reused
    # worker process, two files that both emit e.g. "File ended prematurely"
    # would have the second suppressed -> misclassified OK. Turn it off so every
    # file's errors are delivered independently.
    av.logging.set_skip_repeated(False)

    logger = logging.getLogger(_LIBAV_LOGGER)
    collector = _ErrorCollector()
    prev_level = logger.level
    logger.setLevel(logging.ERROR)
    logger.addHandler(collector)

    frames = 0
    exc_errors: list[str] = []
    try:
        try:
            container = av.open(path, options={"err_detect": _ERR_DETECT})
        except av.error.FFmpegError as exc:
            return Outcome(
                Status.ERROR, log=f"open failed: {exc}", backend=DetectorBackend.PYAV
            )
        # container.duration is in av.time_base (AV_TIME_BASE = 1e6 microseconds).
        total_s = (container.duration or 0) / 1_000_000
        max_s = 0.0
        last_report = time.monotonic()
        try:
            for packet in container.demux():
                if packet.pts is not None and packet.time_base is not None:
                    max_s = max(max_s, float(packet.pts * packet.time_base))
                try:
                    for _frame in packet.decode():
                        frames += 1
                except av.error.FFmpegError as exc:
                    exc_errors.append(str(exc))
                if on_progress is not None and (time.monotonic() - last_report) >= 2.0:
                    on_progress(max_s, total_s, frames)
                    last_report = time.monotonic()
        except av.error.FFmpegError as exc:
            exc_errors.append(f"demux: {exc}")
        finally:
            container.close()
            if on_progress is not None:
                on_progress(max_s, total_s, frames)
    finally:
        logger.removeHandler(collector)
        logger.setLevel(prev_level)

    all_errors = collector.messages + exc_errors
    status = Status.CORRUPT if all_errors else Status.OK
    return Outcome(
        status=status,
        log=_truncate("\n".join(all_errors)),
        backend=DetectorBackend.PYAV,
        frames_decoded=frames,
        error_count=len(all_errors),
    )


# --------------------------------------------------------------------------- #
# subprocess (reference) backend
# --------------------------------------------------------------------------- #
def check_subprocess(path: str, *, timeout: float | None = None) -> Outcome:
    """Integrity check by shelling out to ffmpeg -- the reference behaviour."""
    proc = subprocess.run(
        [
            "ffmpeg",
            "-nostdin",
            "-v", "error",
            "-err_detect", _ERR_DETECT,
            "-i", path,
            "-map", "0",   # decode every stream (parity with PyAV demux())
            "-f", "null",
            "-",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    stderr = proc.stderr.strip()

    # Classify by exit code, which is unambiguous where string-matching is not:
    #   rc != 0            -> ffmpeg aborted (couldn't open / fatal)     = ERROR
    #   rc == 0 + stderr   -> decoded through but logged decode errors   = CORRUPT
    #   rc == 0 + no stderr-> clean                                       = OK
    # This mirrors PyAV, where av.open() raising is the ERROR case and
    # concealed decode errors surface only in the log stream.
    if proc.returncode != 0:
        status = Status.ERROR
    elif stderr:
        status = Status.CORRUPT
    else:
        status = Status.OK
    return Outcome(
        status=status,
        log=_truncate(stderr),
        backend=DetectorBackend.SUBPROCESS,
        error_count=len(stderr.splitlines()) if stderr else 0,
    )


BACKENDS = {
    DetectorBackend.PYAV: check_pyav,
    DetectorBackend.SUBPROCESS: check_subprocess,
}


def check(
    path: str,
    backend: DetectorBackend = DetectorBackend.PYAV,
    *,
    on_progress: Callable[[float, float, int], None] | None = None,
) -> Outcome:
    """Run the configured backend against ``path``. ``on_progress`` is only wired
    for the PyAV backend (the subprocess backend has no in-loop hook)."""
    try:
        resolved = DetectorBackend(backend)
    except ValueError:
        raise ValueError(f"unknown detector backend: {backend!r}") from None
    if resolved is DetectorBackend.PYAV:
        return check_pyav(path, on_progress=on_progress)
    return check_subprocess(path)
