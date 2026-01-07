"""Utilities for the web app."""

from __future__ import annotations

from collections.abc import Generator
from typing import TypeAlias

import shutil
from pathlib import Path
import tempfile
from contextlib import contextmanager
import os
from subprocess import CalledProcessError, check_output

__all__ = [
    "git",
    "push",
    "commit",
    "fetch",
    "create_branch",
    "get_current_branch",
    "temporary_git_clone",
]


@contextmanager
def temporary_git_clone_new_branch(url: str, name: str) -> Generator[Path, None, None]:
    """Temporarily clone a repository from a URL, create the given branch, and switch to it."""
    with temporary_git_clone(url) as directory:
        create_branch(directory, name)
        yield directory


@contextmanager
def temporary_github_clone(owner: str, repo: str) -> Generator[Path, None, None]:
    """Temporarily clone a repository from a URL."""
    url = f"https://github.com/{owner}/{repo}"
    with temporary_git_clone(url) as directory:
        yield directory


@contextmanager
def temporary_git_clone(url: str) -> Generator[Path, None, None]:
    """Temporarily clone a repository from a URL."""
    with tempfile.TemporaryDirectory() as directory_:
        directory = Path(directory_)
        git(directory, "clone", url)
        yield directory
        shutil.rmtree(directory)


GitReturn: TypeAlias = str | CalledProcessError


def git(directory: Path, *args: str) -> GitReturn:
    """Run the git command with the given arguments in the given directory."""
    with open(os.devnull, "w") as devnull:
        try:
            ret = check_output(  # noqa: S603
                ["git", *args],  # noqa:S607
                cwd=directory,
                stderr=devnull,
            )
        except CalledProcessError as e:
            return e
        else:
            return ret.strip().decode("utf-8")


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


def get_current_branch(directory: Path) -> GitReturn:
    """Return if on the master/main branch."""
    return git(directory, "rev-parse", "--abbrev-ref", "HEAD")


def create_branch(directory: Path, branch: str) -> GitReturn:
    """Create a branch and check it out."""
    return git(directory, "checkout", "-b", branch)
