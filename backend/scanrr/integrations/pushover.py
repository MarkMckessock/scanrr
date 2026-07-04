"""Pushover client (SPEC §10). Config comes from the YAML `pushover:` stanza (§0)."""

from __future__ import annotations

import httpx


class PushoverClient:
    URL = "https://api.pushover.net/1/messages.json"

    def __init__(
        self,
        user_key: str,
        api_token: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._user = user_key
        self._token = api_token
        self._client = httpx.AsyncClient(timeout=15.0, transport=transport)

    async def send(self, title: str, message: str, *, priority: int = 0) -> None:
        resp = await self._client.post(
            self.URL,
            data={
                "token": self._token,
                "user": self._user,
                "title": title,
                "message": message[:1024],
                "priority": priority,
            },
        )
        resp.raise_for_status()

    async def close(self) -> None:
        await self._client.aclose()
