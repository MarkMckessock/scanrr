"""Detection-efficacy tests for the integrity backends.

These are the regression guard for the core function of scanrr: given a known
clean file and known-corrupt files, every backend must agree on the verdict, and
in particular must catch *concealed* corruption (decode errors that libav hides
behind successful frame returns). See SPEC.md sec.7.
"""

from __future__ import annotations

import pytest

from scanrr.scanning import integrity as it
from scanrr.scanning.integrity import Status

BACKENDS = ["pyav", "subprocess"]

# Expected verdict per fixture. The clean file must pass; corrupted files must
# not be reported OK. bitflip/truncated stay openable (CORRUPT); a mangled header
# cannot be opened at all (ERROR).
EXPECTED = {
    "clean": Status.OK,
    "bitflip": Status.CORRUPT,
    "truncated": Status.CORRUPT,
    "header": Status.ERROR,
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("sample", list(EXPECTED))
def test_verdict(media, sample, backend):
    """Each backend returns the expected status for each fixture."""
    outcome = it.check(str(media[sample]), backend=backend)
    assert outcome.status is EXPECTED[sample], (
        f"{backend} on {sample}: expected {EXPECTED[sample]}, "
        f"got {outcome.status} (log: {outcome.log[:200]!r})"
    )


@pytest.mark.parametrize("sample", list(EXPECTED))
def test_backends_agree_on_pass_fail(media, sample):
    """pyav and subprocess must agree on the binary OK-vs-not-OK verdict.

    ERROR vs CORRUPT may differ on edge cases, but 'is this file good?' must not.
    """
    pyav = it.check(str(media[sample]), backend="pyav")
    sub = it.check(str(media[sample]), backend="subprocess")
    assert (pyav.status is Status.OK) == (sub.status is Status.OK), (
        f"{sample}: pyav={pyav.status} subprocess={sub.status}"
    )


def test_clean_file_decodes_frames(media):
    """Sanity: the clean file actually produced frames (fixture isn't empty)."""
    outcome = it.check_pyav(str(media["clean"]))
    assert outcome.status is Status.OK
    assert outcome.frames_decoded > 100


def _naive_exception_only(path: str) -> Status:
    """The detection approach the spec ORIGINALLY proposed: decode every frame
    and only treat *raised exceptions* as corruption -- no log capture.

    Kept here as the regression sentinel for test_log_capture_is_required.
    """
    import av
    import av.error

    try:
        container = av.open(path, options={"err_detect": "aggressive"})
    except av.error.FFmpegError:
        return Status.ERROR
    raised = False
    try:
        for packet in container.demux():
            try:
                for _ in packet.decode():
                    pass
            except av.error.FFmpegError:
                raised = True
    except av.error.FFmpegError:
        raised = True
    finally:
        container.close()
    return Status.CORRUPT if raised else Status.OK


def test_log_capture_is_required(media):
    """The whole reason the pyav backend captures libav's log stream.

    A truncated file decodes 'successfully' frame-by-frame -- libav conceals the
    premature EOF and only reports it via an ERROR-level log record. The naive
    exception-only loop therefore reports it OK (a false negative), while our
    log-capturing backend correctly flags it CORRUPT.

    If a future libav starts *raising* on this case, the first assertion will
    fail -- which is a behaviour change we explicitly want surfaced and reviewed,
    not silently absorbed.
    """
    trunc = str(media["truncated"])
    assert _naive_exception_only(trunc) is Status.OK, (
        "naive exception-only detection unexpectedly caught truncation -- "
        "libav behaviour changed; review whether log capture is still needed"
    )
    assert it.check_pyav(trunc).status is Status.CORRUPT, (
        "log-capturing backend MUST catch the truncated file the naive loop misses"
    )
