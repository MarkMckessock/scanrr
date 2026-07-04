"""Deterministic generation of clean and deliberately-corrupted media samples.

Used by the detection-efficacy tests. Everything here is reproducible (no RNG
seeded by wall-clock) so fixtures are byte-stable across runs.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

FFMPEG = shutil.which("ffmpeg")


def ffmpeg_available() -> bool:
    return FFMPEG is not None


def make_clean(path: Path, *, seconds: int = 4) -> Path:
    """Encode a clean H.264/AAC MKV with a short GOP.

    ``testsrc`` produces moving content, so P-frames genuinely depend on prior
    frames -- corrupting one then yields real inter-frame decode errors rather
    than a no-op. A short GOP (-g 12) gives several keyframes to damage.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            FFMPEG, "-y",
            "-f", "lavfi", "-i", f"testsrc=duration={seconds}:size=320x240:rate=25",
            "-f", "lavfi", "-i", f"sine=frequency=440:duration={seconds}",
            "-c:v", "libx264", "-preset", "ultrafast",
            "-g", "12", "-keyint_min", "12",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            str(path),
        ],
        check=True,
        capture_output=True,
    )
    return path


def _flip_runs(data: bytearray, regions: list[tuple[float, float, int]]) -> int:
    """XOR contiguous runs of bytes at fractional offsets. Returns bytes flipped.

    Each region is (start_fraction, ignored, run_length): a run of ``run_length``
    bytes starting at ``start_fraction`` of the file is XORed with 0xFF.
    Contiguous runs corrupt a localised region -> the decoder conceals and logs
    errors while still decoding the rest (the case a naive exception-only loop
    misses).
    """
    n = len(data)
    flipped = 0
    for start_frac, _, run_len in regions:
        start = int(n * start_frac)
        for i in range(start, min(start + run_len, n)):
            data[i] ^= 0xFF
            flipped += 1
    return flipped


def bitflip_stream(src: Path, dst: Path) -> Path:
    """Corrupt frame data in the middle of the file (keeps header intact).

    Produces a file that still opens and mostly decodes but emits decode errors
    -- the discriminating fixture for log-capture vs exception-only detection.
    """
    data = bytearray(src.read_bytes())
    # Stay clear of the front (EBML/track header) and the very end.
    _flip_runs(data, [(0.45, 0, 96), (0.60, 0, 96), (0.75, 0, 96)])
    dst.write_bytes(data)
    return dst


def truncate(src: Path, dst: Path, *, keep_fraction: float = 0.6) -> Path:
    """Cut the file off partway through -- a partial/interrupted download."""
    data = src.read_bytes()
    dst.write_bytes(data[: int(len(data) * keep_fraction)])
    return dst


def corrupt_header(src: Path, dst: Path) -> Path:
    """Mangle the first bytes so the container cannot be opened at all."""
    data = bytearray(src.read_bytes())
    for i in range(min(64, len(data))):
        data[i] ^= 0xFF
    dst.write_bytes(data)
    return dst
