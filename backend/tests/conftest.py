"""Shared fixtures: generate the clean + corrupted media samples once per session."""

from __future__ import annotations

from pathlib import Path

import pytest

import media_fixtures as mf

requires_ffmpeg = pytest.mark.skipif(
    not mf.ffmpeg_available(), reason="ffmpeg CLI not on PATH"
)


@pytest.fixture(scope="session")
def media(tmp_path_factory) -> dict[str, Path]:
    """A dict of sample name -> path, generated deterministically with ffmpeg.

    ``clean`` is a valid file; the rest are deliberately corrupted variants.
    Skips the whole suite if ffmpeg is unavailable (needed to synthesise samples).
    """
    if not mf.ffmpeg_available():
        pytest.skip("ffmpeg CLI not on PATH")
    d = tmp_path_factory.mktemp("media")
    clean = mf.make_clean(d / "clean.mkv")
    return {
        "clean": clean,
        "bitflip": mf.bitflip_stream(clean, d / "bitflip.mkv"),
        "truncated": mf.truncate(clean, d / "truncated.mkv"),
        "header": mf.corrupt_header(clean, d / "header.mkv"),
    }
