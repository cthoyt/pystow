"""Interactions with GitHub."""

from __future__ import annotations

from typing import Any

import requests
from ratelimit import rate_limited

from .config_api import get_config
from .constants import TimeoutHint

__all__ = [
    "requests_get_github",
]


def get_headers(token: str | None = None, raise_on_missing: bool = False) -> dict[str, str]:
    """Get GitHub headers."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
    }
    token = get_config("github", "token", passthrough=token, raise_on_missing=raise_on_missing)
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


@rate_limited(calls=5_000, period=60 * 60)
def requests_get_github(
    path: str,
    params: dict[str, Any] | None = None,
    token: str | None = None,
    timeout: TimeoutHint = None,
    require_token: bool = False,
) -> requests.Response:
    """Make a GET request to the GitHub API."""
    path = path.lstrip("/")
    url = f"https://api.github.com/{path}"
    headers = get_headers(token=token, raise_on_missing=require_token)
    return requests.get(url, headers=headers, params=params, timeout=timeout)
