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


@rate_limited(calls=5_000, period=60 * 60)
def requests_get_github(
    url: str,
    accept: str | None = None,
    params: dict[str, Any] | None = None,
    token: str | None = None,
    timeout: TimeoutHint = None,
) -> requests.Response:
    """Make a GET request to the GitHub API."""
    headers = {}
    token = get_config("github", "token", passthrough=token)
    if token:
        headers["Authorization"] = f"token {token}"
    if accept:
        headers["Accept"] = accept
    return requests.get(url, headers=headers, params=params, timeout=timeout)
