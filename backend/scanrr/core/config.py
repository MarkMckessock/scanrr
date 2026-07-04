"""Configuration (SPEC §0, §13). Effective config = ``DEFAULTS`` overlaid with the
YAML ``settings:`` stanza (loaded in ``core.fileconfig``).

* env vars (deploy-time) via ``Settings`` — DB URL, config-file path, API token, log level.
* ``RuntimeConfig``/``DEFAULTS`` — the **canonical source of tunable defaults**.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scanrr.enums import DetectorBackend, HashAlgorithm

# Media file extensions considered for scanning (SPEC §7).
DEFAULT_MEDIA_EXTENSIONS = [
    ".mkv", ".mp4", ".avi", ".m4v", ".ts", ".mov",
    ".wmv", ".flv", ".webm", ".mpg", ".mpeg", ".m2ts",
]


class RuntimeConfig(BaseModel):
    """Runtime tunables (SPEC §13 — canonical defaults). Persisted per-key in the
    ``settings`` table; overridable per-run. Fully typed — never a bare dict."""

    max_scan_workers: int = 3
    hash_algorithm: HashAlgorithm = HashAlgorithm.BLAKE3
    detector_backend: DetectorBackend = DetectorBackend.PYAV
    media_extensions: list[str] = Field(default_factory=lambda: list(DEFAULT_MEDIA_EXTENSIONS))
    min_file_size_bytes: int = 1_000_000    # 1 MB — skip samples/artwork
    min_file_age_seconds: int = 120         # stability gate
    max_scan_seconds: int = 1800            # per-file timeout
    scan_max_attempts: int = 3              # transient-failure retries
    misfire_grace_time: int = 3600
    max_replace_attempts: int = 2
    max_deletions_per_run: int = 25
    replacement_poll_interval: int = 120
    replacement_search_timeout: int = 43_200  # 12h
    notification_flush_interval: int = 300
    notification_batch_threshold: int = 5


DEFAULTS = RuntimeConfig()


class Settings(BaseSettings):
    """Deploy-time configuration from environment (prefix ``SCANRR_``)."""

    model_config = SettingsConfigDict(env_prefix="SCANRR_", env_file=".env", extra="ignore")

    database_url: str = "sqlite:///scanrr.db"
    # YAML config file (settings + jobs) read at startup. Empty = disabled.
    config_file: str = ""
    log_level: str = "INFO"
    # Shared secret required on mutating API routes (SPEC §11/§14). Empty = off (dev).
    api_token: str = ""


settings = Settings()
