# scanrr

Periodically scans a media library for **corrupt video files** (bad frames, broken
i-frames, bitstream errors) using ffmpeg, records results durably, and surfaces
failures through a modern web UI. Integrates with Sonarr/Radarr for library
discovery and automatic re-requesting of corrupt files, with Pushover notifications.

Built for a homelab Kubernetes cluster, scanning media on NFS-mounted Synology shares.

## Key ideas

- **Content-addressed idempotency** — integrity is a deterministic property of a
  file's bytes, so results are cached by content hash and reused across paths, runs,
  and restarts. Failed scans resume without re-doing completed work.
- **Cheap skips** — a (size + mtime) fast-path, per-job TTL, and cross-path hash
  dedup avoid re-scanning unchanged content.
- **Two job types** — an absolute directory path, or a Sonarr/Radarr library.

## Status

Design phase. See **[SPEC.md](./SPEC.md)** — the source of truth for all design
decisions.

## Stack

FastAPI · SQLite · PyAV · blake3 · React + Tailwind + shadcn/ui. See the spec for detail.
