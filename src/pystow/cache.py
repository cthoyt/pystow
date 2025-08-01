"""Utilities for caching files."""

from __future__ import annotations

import functools
import json
import logging
import pickle
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    TypeVar,
    Union,
    cast,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    # Classes
    "Cached",
    "CachedCollection",
    "CachedDataFrame",
    "CachedJSON",
    "CachedPickle",
    # Types
    "Getter",
]

logger = logging.getLogger(__name__)

JSONType = Union[
    dict[str, Any],
    list[Any],
]

X = TypeVar("X")
Getter = Callable[[], X]


class Cached(Generic[X], ABC):
    """Caching decorator."""

    def __init__(
        self,
        path: str | Path,
        *,
        force: bool = False,
        cache: bool = True,
    ) -> None:
        """Instantiate the decorator.

        :param path: The path to the cache for the file
        :param cache: Should caching be done? Defaults to true, turn off for debugging
            purposes
        :param force: Should a pre-existing file be disregared/overwritten?
        """
        self.path = Path(path)
        self.force = force
        self.cache = cache

    def __call__(self, func: Getter[X]) -> Getter[X]:
        """Apply this instance as a decorator.

        :param func: The function to wrap

        :returns: A wrapped function
        """

        @functools.wraps(func)
        def _wrapped() -> X:
            if not self.cache:
                return func()

            if self.path.is_file() and not self.force:
                return self.load()
            logger.debug("no cache found at %s", self.path)
            rv = func()
            logger.debug("writing cache to %s", self.path)
            self.dump(rv)
            return rv

        return _wrapped

    @abstractmethod
    def load(self) -> X:
        """Load data from the cache (typically by opening a file at the given path)."""

    @abstractmethod
    def dump(self, rv: X) -> None:
        """Dump data to the cache (typically by opening a file at the given path).

        :param rv: The data to dump
        """


class CachedJSON(Cached[JSONType]):
    """Make a function lazily cache its return value as JSON."""

    def load(self) -> JSONType:
        """Load data from the cache as JSON.

        :returns: A python object with JSON-like data from the cache
        """
        with open(self.path) as file:
            return cast(JSONType, json.load(file))

    def dump(self, rv: JSONType) -> None:
        """Dump data to the cache as JSON.

        :param rv: The JSON data to dump
        """
        with open(self.path, "w") as file:
            json.dump(rv, file, indent=2)


class CachedPickle(Cached[Any]):
    """Make a function lazily cache its return value as a pickle."""

    def load(self) -> Any:
        """Load data from the cache as a pickle.

        :returns: A python object loaded from the cache
        """
        with open(self.path, "rb") as file:
            return pickle.load(file)

    def dump(self, rv: Any) -> None:
        """Dump data to the cache as a pickle.

        :param rv: The arbitrary python object to dump
        """
        with open(self.path, "wb") as file:
            pickle.dump(rv, file, protocol=pickle.HIGHEST_PROTOCOL)


class CachedCollection(Cached[list[str]]):
    """Make a function lazily cache its return value as file."""

    def load(self) -> list[str]:
        """Load data from the cache as a list of strings.

        :returns: A list of strings loaded from the cache
        """
        with open(self.path) as file:
            return [line.strip() for line in file]

    def dump(self, rv: list[str]) -> None:
        """Dump data to the cache as a list of strings.

        :param rv: The list of strings to dump
        """
        with open(self.path, "w") as file:
            for line in rv:
                print(line, file=file)


class CachedDataFrame(Cached["pd.DataFrame"]):
    """Make a function lazily cache its return value as a dataframe."""

    def __init__(
        self,
        path: str | Path,
        cache: bool = True,
        force: bool = False,
        sep: str | None = None,
        dtype: Any | None = None,
        read_csv_kwargs: MutableMapping[str, Any] | None = None,
    ) -> None:
        """Instantiate the decorator.

        :param path: The path to the cache for the file
        :param force: Should a pre-existing file be disregared/overwritten?
        :param sep: The separator. Defaults to TSV, since this is the only reasonable
            default.
        :param dtype: A shortcut for setting the dtype
        :param read_csv_kwargs: Additional kwargs to pass to :func:`pd.read_csv`.

        :raises ValueError: if sep is given as a kwarg and also in ``read_csv_kwargs``.
        """
        super().__init__(path=path, cache=cache, force=force)
        self.read_csv_kwargs = read_csv_kwargs or {}
        if "sep" not in self.read_csv_kwargs:
            self.sep = sep or "\t"
        elif sep is not None:
            raise ValueError
        else:
            self.sep = self.read_csv_kwargs.pop("sep")
        if dtype is not None:
            if "dtype" in self.read_csv_kwargs:
                raise ValueError
            self.read_csv_kwargs["dtype"] = dtype
        self.read_csv_kwargs.setdefault("keep_default_na", False)

    def load(self) -> pd.DataFrame:
        """Load data from the cache as a dataframe.

        :returns: A dataframe loaded from the cache.
        """
        import pandas as pd

        return pd.read_csv(
            self.path,
            sep=self.sep,
            **self.read_csv_kwargs,
        )

    def dump(self, rv: pd.DataFrame) -> None:
        """Dump data to the cache as a dataframe.

        :param rv: The dataframe to dump
        """
        rv.to_csv(self.path, sep=self.sep, index=False)
