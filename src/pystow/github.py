"""Interactions with GitHub."""

from __future__ import annotations

from collections.abc import Iterable
from functools import partial
from typing import Any, Literal, cast

import requests
from ratelimit import rate_limited
from tqdm import tqdm

from .config_api import get_config
from .constants import TimeoutHint

__all__ = [
    "MAXIMUM_SEARCH_PAGE_SIZE",
    "delete_branch",
    "get_contributions",
    "get_default_branch",
    "get_issue",
    "get_issues",
    "get_pull_requests",
    "get_repository",
    "get_repository_commit_activity",
    "get_token",
    "get_topics",
    "get_user_events",
    "post_pull",
    "requests_get_github",
    "requests_post_github",
    "search_code",
]


def get_token(*, passthrough: str | None = None, raise_on_missing: bool = False) -> str | None:
    """Get a GitHub token."""
    rv = get_config("github", "token", passthrough=passthrough, raise_on_missing=raise_on_missing)
    return rv  # type:ignore


def get_headers(
    token: str | None = None, *, raise_on_missing: bool = False, preview: bool = False
) -> dict[str, str]:
    """Get GitHub headers."""
    headers = {
        "Accept": "application/vnd.github.mercy-preview+json"
        if preview
        else "application/vnd.github.v3+json",
    }
    token = get_token(passthrough=token, raise_on_missing=raise_on_missing)
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def requests_get_github(path: str, **kwargs: Any) -> requests.Response:
    """Make a GET request to the GitHub API."""
    return _request_github("GET", path, **kwargs)


def requests_post_github(path: str, **kwargs: Any) -> requests.Response:
    """Make a POST request to the GitHub API."""
    return _request_github("POST", path, **kwargs)


def requests_delete_github(path: str, **kwargs: Any) -> requests.Response:
    """Make a DELETE request to the GitHub API."""
    return _request_github("DELETE", path, **kwargs)


@rate_limited(calls=5_000, period=60 * 60)
def _request_github(
    method: Literal["GET", "POST", "DELETE"],
    path: str,
    params: dict[str, Any] | None = None,
    token: str | None = None,
    timeout: TimeoutHint = None,
    require_token: bool = False,
    preview: bool = False,
    **kwargs: Any,
) -> requests.Response:
    """Make a POST request to the GitHub API."""
    path = path.lstrip("/")
    url = f"https://api.github.com/{path}"
    headers = get_headers(token=token, raise_on_missing=require_token, preview=preview)
    return requests.request(method, url, headers=headers, params=params, timeout=timeout, **kwargs)


def post_pull(
    owner: str,
    repo: str,
    *,
    title: str | None = None,
    head: str,
    head_repo: str | None = None,
    base: str,
    body: str | None = None,
    maintainer_can_modify: bool = True,
    draft: bool = False,
    issue: int | None = None,
    **kwargs: Any,
) -> requests.Response:
    """Post a pull request."""
    # https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28#create-a-pull-request
    if title is None and issue is None:
        raise ValueError("title and issue can't both be none")
    if "params" in kwargs:
        raise ValueError("should pass arguments directly")
    return requests_post_github(
        f"repos/{owner}/{repo}/pulls",
        params={
            "title": title,
            "head": head,
            "head_repo": head_repo,
            "base": base,
            "body": body,
            "maintainer_can_modify": maintainer_can_modify,
            "draft": draft,
            "issue": issue,
        },
        **kwargs,
    )


def delete_branch(owner: str, repo: str, branch_name: str, **kwargs: Any) -> requests.Response:
    """Delete a branch.

    :param owner: The repository owner
    :param repo: The repository name
    :param branch_name: The branch name or head ref
    :returns: A response from the GitHub API after running the (secret) delete endpoint
    """
    # see https://github.com/orgs/community/discussions/24603
    return requests_delete_github(f"repos/{owner}/{repo}/git/refs/heads/{branch_name}", **kwargs)


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


def get_issue(owner: str, repo: str, issue: int, **kwargs: Any) -> requests.Response:
    """Get an issue from a repository."""
    return requests_get_github(f"repos/{owner}/{repo}/issues/{issue}", **kwargs)


def get_pull_requests(owner: str, repo: str, **kwargs: Any) -> requests.Response:
    """Get pull requests from a repository."""
    return requests_get_github(f"repos/{owner}/{repo}/pulls", **kwargs)


def get_user_events(user: str) -> requests.Response:
    """Get events for a user."""
    return requests_get_github(f"users/{user}/events")


def get_contributions(owner: str, repo: str, **kwargs: Any) -> requests.Response:
    """Get contributors to a repository."""
    return requests_get_github(f"repos/{owner}/{repo}/stats/contributors", **kwargs)


def get_repository_commit_activity(owner: str, repo: str, **kwargs: Any) -> requests.Response:
    """Get commit activity to a repository."""
    return requests_get_github(f"repos/{owner}/{repo}/stats/commit_activity", **kwargs)


def get_topics(owner: str, repo: str, *, preview: bool = True, **kwargs: Any) -> requests.Response:
    """Get topics from the repository."""
    return requests_get_github(f"repos/{owner}/{repo}/topics", preview=preview, **kwargs)


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
