"""Sonarr / Radarr API v3 clients (SPEC §9).

Async httpx clients that enumerate library media files (path + arr ids for later
remediation) and test connectivity. Sonarr and Radarr share auth/shape; only the
enumeration differs.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from scanrr.enums import ArrType, MediaType


@dataclass
class ArrFile:
    remote_path: str  # path in the arr's namespace (pre path-mapping)
    media_type: MediaType
    media_id: int  # series id (Sonarr) / movie id (Radarr) — for search
    arr_file_id: int  # episodeFile / movieFile id — for deletion


class ArrClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"X-Api-Key": api_key},
            timeout=timeout,
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, **params: str | int) -> httpx.Response:
        resp = await self._client.get(path, params=params or None)
        resp.raise_for_status()
        return resp

    async def test(self) -> dict:
        """Return system status, raising on auth/connection failure."""
        return (await self._get("/api/v3/system/status")).json()

    async def list_media_files(self) -> list[ArrFile]:  # pragma: no cover - overridden
        raise NotImplementedError

    # --- remediation (SPEC §9) ---------------------------------------------- #

    async def delete_file(self, media_type: MediaType, arr_file_id: int) -> None:
        endpoint = "episodefile" if media_type is MediaType.EPISODE else "moviefile"
        resp = await self._client.delete(f"/api/v3/{endpoint}/{arr_file_id}")
        resp.raise_for_status()

    async def search(self, media_type: MediaType, media_id: int) -> None:
        if media_type is MediaType.EPISODE:
            body: dict = {"name": "SeriesSearch", "seriesId": media_id}
        else:
            body = {"name": "MoviesSearch", "movieIds": [media_id]}
        resp = await self._client.post("/api/v3/command", json=body)
        resp.raise_for_status()

    async def imported(self, media_type: MediaType, media_id: int) -> bool:
        """True if the arr history shows a recent import for this media (SPEC §9 Q4)."""
        key = "seriesId" if media_type is MediaType.EPISODE else "movieId"
        events = {"downloadFolderImported", "movieFileImported", "episodeFileImported"}
        resp = await self._get(
            "/api/v3/history", pageSize=50, sortKey="date", sortDirection="descending"
        )
        records = resp.json().get("records", [])
        return any(
            rec.get("eventType") in events and rec.get(key) == media_id for rec in records
        )


class SonarrClient(ArrClient):
    async def list_media_files(self) -> list[ArrFile]:
        series = (await self._get("/api/v3/series")).json()
        files: list[ArrFile] = []
        for show in series:
            episode_files = (await self._get("/api/v3/episodefile", seriesId=show["id"])).json()
            for ef in episode_files:
                path = ef.get("path")
                if path:
                    files.append(
                        ArrFile(
                            remote_path=path,
                            media_type=MediaType.EPISODE,
                            media_id=show["id"],
                            arr_file_id=ef["id"],
                        )
                    )
        return files


class RadarrClient(ArrClient):
    async def list_media_files(self) -> list[ArrFile]:
        movies = (await self._get("/api/v3/movie")).json()
        files: list[ArrFile] = []
        for movie in movies:
            mf = movie.get("movieFile")
            if mf and mf.get("path"):
                files.append(
                    ArrFile(
                        remote_path=mf["path"],
                        media_type=MediaType.MOVIE,
                        media_id=movie["id"],
                        arr_file_id=mf["id"],
                    )
                )
        return files


def make_client(arr_type: ArrType, base_url: str, api_key: str) -> ArrClient:
    cls = SonarrClient if arr_type is ArrType.SONARR else RadarrClient
    return cls(base_url, api_key)


def apply_path_mapping(mappings: list[tuple[str, str]], remote_path: str) -> str | None:
    """Translate an arr-namespace path to a local path via longest-prefix match.

    ``mappings`` is a list of (remote_prefix, local_prefix). Returns None if no
    mapping applies (the caller flags it as a discovery warning).
    """
    best: tuple[str, str] | None = None
    for remote, local in mappings:
        prefix = remote.rstrip("/")
        if (remote_path == prefix or remote_path.startswith(prefix + "/")) and (
            best is None or len(prefix) > len(best[0].rstrip("/"))
        ):
            best = (remote, local)
    if best is None:
        return None
    remote_prefix, local_prefix = best
    suffix = remote_path[len(remote_prefix.rstrip("/")) :]
    return local_prefix.rstrip("/") + suffix
