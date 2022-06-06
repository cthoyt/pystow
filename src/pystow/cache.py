# -*- coding: utf-8 -*-

"""Utilities for caching files."""

import functools
import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    MutableMapping,
    Optional,
    TypeVar,
    Union,
    cast,
)

try:
    import pickle5 as pickle
except ImportError:
    import pickle  # type:ignore

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    # Classses
    "Cached",
    "CachedPickle",
    "CachedJSON",
    "CachedCollection",
    "CachedDataFrame",
    # Types
    "Getter",
]

logger = logging.getLogger(__name__)

JSONType = Union[
    Dict[str, Any],
    List[Any],
]

X = TypeVar("X")
Getter = Callable[[], X]


class Cached(Generic[X], ABC):
    """Caching decorator."""

    def __init__(
        self,
        path: Union[str, Path, os.PathLike],
        force: bool = False,
    ) -> None:
        """Instantiate the decorator.

        :param path: The path to the cache for the file
        :param force: Should a pre-existing file be disregared/overwritten?
        """
        self.path = Path(path)
        self.force = force

    def __call__(self, func: Getter[X]) -> Getter[X]:
        """Apply this instance as a decorator.

        :param func: The function to wrap
        :return: A wrapped function
        """

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


class CachedCollection(Cached[List[str]]):
    """Make a function lazily cache its return value as file."""

    def load(self) -> List[str]:
        """Load data from the cache as a list of strings.

        :returns: A list of strings loaded from the cache
        """
        with open(self.path) as file:
            return [line.strip() for line in file]

    def dump(self, rv: List[str]) -> None:
        """Dump data to the cache as a list of strings.

        :param rv: The list of strings to dump
        """
        with open(self.path, "w") as file:
            for line in rv:
                print(line, file=file)  # noqa:T001,T201


class CachedDataFrame(Cached["pd.DataFrame"]):
    """Make a function lazily cache its return value as a dataframe."""

    def __init__(
        self,
        path: Union[str, Path, os.PathLike],
        force: bool = False,
        sep: Optional[str] = None,
        dtype: Optional[Any] = None,
        read_csv_kwargs: Optional[MutableMapping[str, Any]] = None,
    ) -> None:
        """Instantiate the decorator.

        :param path: The path to the cache for the file
        :param force: Should a pre-existing file be disregared/overwritten?
        :param sep: The separator. Defaults to TSV, since this is the only reasonable default.
        :param dtype: A shortcut for setting the dtype
        :param read_csv_kwargs: Additional kwargs to pass to :func:`pd.read_csv`.
        :raises ValueError: if sep is given as a kwarg and also in ``read_csv_kwargs``.
        """
        super().__init__(path=path, force=force)
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

    def load(self) -> "pd.DataFrame":
        """Load data from the cache as a dataframe.

        :returns: A dataframe loaded from the cache.
        """
        import pandas as pd

        return pd.read_csv(
            self.path,
            sep=self.sep,
            **self.read_csv_kwargs,
        )

    def dump(self, rv: "pd.DataFrame") -> None:
        """Dump data to the cache as a dataframe.

        :param rv: The dataframe to dump
        """
        rv.to_csv(self.path, sep=self.sep, index=False)
