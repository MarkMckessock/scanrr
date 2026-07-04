"""File discovery for path-type jobs (SPEC §7). Stat-only — no content reads."""

from __future__ import annotations

import os
from collections.abc import Iterator


def walk_media(
    root: str,
    extensions: list[str],
    min_size_bytes: int,
) -> Iterator[tuple[str, os.stat_result]]:
    """Yield (path, stat) for every media file under ``root`` passing the filters.

    Uses ``os.scandir`` (one stat per entry) so discovery stays cheap; it never
    opens/reads file contents (SPEC §5 — reads belong to the worker pool).
    """
    exts = {e.lower() for e in extensions}
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            entries = list(os.scandir(current))
        except (NotADirectoryError, FileNotFoundError, PermissionError):
            continue
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                stack.append(entry.path)
                continue
            if not entry.is_file(follow_symlinks=False):
                continue
            if os.path.splitext(entry.name)[1].lower() not in exts:
                continue
            st = entry.stat()
            if st.st_size < min_size_bytes:
                continue
            yield entry.path, st
