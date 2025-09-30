"""Interactions with GitHub."""

from __future__ import annotations

from collections.abc import Iterable
from functools import partial
from typing import Any, cast

import requests
from ratelimit import rate_limited
from tqdm import tqdm

from .config_api import get_config
from .constants import TimeoutHint

__all__ = [
    "MAXIMUM_SEARCH_PAGE_SIZE",
    "get_default_branch",
    "get_issues",
    "get_pull_requests",
    "get_repository",
    "requests_get_github",
    "search_code",
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


#: Maximum number of records per page in code search
MAXIMUM_SEARCH_PAGE_SIZE = 40


def search_code(
    query: str,
    *,
    page_size: int | None = None,
    progress: bool = True,
    inner_progress: bool = True,
) -> Iterable[dict[str, Any]]:
    """Search GitHub code."""
    page = 1
    if page_size is None:
        page_size = MAXIMUM_SEARCH_PAGE_SIZE
    if page_size > MAXIMUM_SEARCH_PAGE_SIZE:
        page_size = MAXIMUM_SEARCH_PAGE_SIZE

    inner_tqdm = partial(tqdm, disable=not inner_progress, unit="record", leave=False)

    initial_res = _search_code_helper(page_size=page_size, page=page, query=query)
    initial_res.raise_for_status()
    initial_res_json = initial_res.json()
    total = initial_res_json["total_count"]
    total_pages = 1 + (total // page_size)
    yield from inner_tqdm(initial_res_json["items"], desc="Page 1")

    with tqdm(
        total=total_pages, unit="page", disable=not progress, desc="Paginating code search results"
    ) as tbar:
        tbar.update(1)

        while page_size * page < total:
            page += 1
            tbar.update(1)
            res = _search_code_helper(page=page, page_size=page_size, query=query)
            res.raise_for_status()
            res_json = res.json()
            yield from inner_tqdm(res_json["items"], desc=f"Page {page}")


def _search_code_helper(page_size: int, page: int, query: str) -> requests.Response:
    return requests_get_github(
        "search/code",
        params={
            "per_page": page_size,
            "page": page,
            "sort": "indexed",
            "q": query,
        },
    )
