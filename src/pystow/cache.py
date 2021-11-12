# -*- coding: utf-8 -*-

"""Utilities for caching files."""

import functools
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, Generic, List, TypeVar, Union

try:
    import pickle5 as pickle
except ImportError:
    import pickle  # type:ignore

__all__ = [
    "cached_json",
    "cached_pickle",
    "cached_collection",
]

logger = logging.getLogger(__name__)

JSONType = Union[
    Dict[str, Any],
    List[Any],
]

X = TypeVar("X")
Getter = Callable[[], X]
Modifier = Callable[[X], X]


class Cached(Generic[X]):
    def __init__(
        self,
        path: Union[str, Path, os.PathLike],
        force: bool = False,
    ):
        self.path = Path(path)
        self.force = force

    def __call__(self) -> Modifier[Getter[X]]:
        """Applied when using an instance of this class as a decorator."""

        def wrapped(func: Getter[X]) -> Getter[X]:  # noqa: D202
            """Wrap the function."""

            @functools.wraps(func)
            def _wrapped() -> X:
                if self.path.is_file() and not self.force:
                    return self.load()
                logger.debug("no cache found at %s", self.path)
                rv = func()
                logger.debug("writing cache to %s", self.path)
                self.dump(rv)
                return rv

            return _wrapped

        return wrapped

    def load(self) -> X:
        """Load data from the cache (typically by opening a file at the given path)."""
        raise NotImplementedError

    def dump(self, X) -> None:
        """Dump data to the cache (typically by opening a file at the given path)."""
        raise NotImplementedError


class CachedJSON(Cached[JSONType]):
    """Make a function lazily cache its return value as JSON."""

    def load(self) -> JSONType:
        with open(self.path) as file:
            return json.load(file)

    def dump(self, rv: JSONType) -> None:
        with open(self.path, "w") as file:
            json.dump(rv, file, indent=2)


class CachedPickle(Cached[Any]):
    """Make a function lazily cache its return value as a pickle."""

    def load(self) -> Any:
        with open(self.path, "rb") as file:
            return pickle.load(file)

    def dump(self, rv: Any) -> None:
        with open(self.path, "wb") as file:
            pickle.dump(rv, file, protocol=pickle.HIGHEST_PROTOCOL)


class CachedCollection(Cached[List[str]]):
    """Make a function lazily cache its return value as file."""

    def load(self) -> List[str]:
        with open(self.path) as file:
            return [line.strip() for line in file]

    def dump(self, rv: Any) -> None:
        with open(self.path, "w") as file:
            for line in rv:
                print(line, file=file)  # noqa:T001
