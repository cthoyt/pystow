"""Utilities for the web app."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess, run
from typing import TypeAlias

__all__ = [
    "commit",
    "create_branch",
    "fetch",
    "get_current_branch",
    "git",
    "push",
    "temporary_git_clone",
]


@contextmanager
def temporary_github_clone(owner: str, repo: str) -> Generator[Path, None, None]:
    """Temporarily clone a repository from a URL."""
    url = f"https://github.com/{owner}/{repo}.git"
    with temporary_git_clone(url) as directory:
        yield directory


@contextmanager
def temporary_git_clone(url: str) -> Generator[Path, None, None]:
    """Temporarily clone a repository from a URL."""
    with tempfile.TemporaryDirectory() as directory_:
        directory = Path(directory_)
        clone(directory, url)
        yield directory
        shutil.rmtree(directory)


GitReturn: TypeAlias = CompletedProcess[str] | CalledProcessError


def git(directory: Path, *args: str) -> GitReturn:
    """Run the git command with the given arguments in the given directory."""
    return _check_output("git", *args, directory=directory)


def clone(directory: Path, url: str) -> GitReturn:
    """Clone the git repository with the given URL."""
    args = ["git", "clone", url, directory.as_posix()]
    return _check_output(*args, directory=directory)


def _check_output(*args: str, directory: Path | None = None) -> GitReturn:
    with open(os.devnull, "w"):
        try:
            completed_process = run(  # noqa: S603
                args,
                cwd=None if directory is None else directory.as_posix(),
                capture_output=True,
                text=True,
            )
        except CalledProcessError as e:
            return e
        else:
            return completed_process


def commit(directory: Path, message: str) -> GitReturn:
    """Make a commit with the following message."""
    return git(directory, "commit", "-m", message, "-a")


def push(directory: Path, branch: str | None = None) -> GitReturn:
    """Push the git repo."""
    if branch is not None:
        return git(directory, "push", "origin", branch)
    else:
        return git(directory, "push")


def fetch(directory: Path) -> GitReturn:
    """Fetch the git repo."""
    return git(directory, "fetch", "--all")


def has_local_branch(directory: Path, branch: str) -> bool:
    """Check if a branch exists in the git repo."""
    completed_process = git(directory, "show-ref", "--verify", "--quiet", f"refs/heads/{branch}")
    match completed_process.returncode:
        case 0:
            return True
        case 1:
            return False
        case other:
            raise RuntimeError(f"unexpected return code: {other}")


def get_current_branch(directory: Path) -> str:
    """Return if on the master/main branch."""
    completed_process = git(directory, "rev-parse", "--abbrev-ref", "HEAD")
    return completed_process.stdout.strip()


def create_branch(directory: Path, branch: str) -> GitReturn:
    """Create a branch and check it out."""
    return git(directory, "checkout", "-b", branch)
