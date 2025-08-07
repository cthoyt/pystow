"""Interactions with GitHub."""

from __future__ import annotations

from typing import Any, cast

import requests
from ratelimit import rate_limited

from .config_api import get_config
from .constants import TimeoutHint

__all__ = [
    "get_default_branch",
    "get_issues",
    "get_pull_requests",
    "get_repository",
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


def get_repository(owner: str, repo: str, **kwargs: Any) -> requests.Response:
    """Get information about a repository."""
    return requests_get_github(f"repos/{owner}/{repo}", **kwargs)


def get_default_branch(owner: str, repo: str, **kwargs: Any) -> str:
    """Get the default branch for the repository."""
    res = get_repository(owner, repo, **kwargs).json()
    return cast(str, res["default_branch"])


def get_issues(owner: str, repo: str, **kwargs: Any) -> requests.Response:
    """Get issues from a repository."""
    return requests_get_github(f"repos/{owner}/{repo}/issues", **kwargs)


def get_pull_requests(owner: str, repo: str, **kwargs: Any) -> requests.Response:
    """Get pull requests from a repository."""
    return requests_get_github(f"repos/{owner}/{repo}/pulls", **kwargs)
