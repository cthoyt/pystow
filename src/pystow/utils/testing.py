"""Utilities for testing."""

from collections.abc import Callable
from typing import Any, TypeVar

__all__ = [
    "requires_package",
]

_FT = TypeVar("_FT", bound=Callable[..., Any])


def requires_package(package: str) -> Callable[[_FT], _FT]:
    """Construct a decorator for a :class:`unittest.TestCase`."""
    import importlib.util
    import unittest

    return unittest.skipUnless(
        importlib.util.find_spec(package), f"packages is not installed: {package}"
    )
