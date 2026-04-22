"""File opening utilities."""

from __future__ import annotations

import contextlib
import csv
import gzip
import io
import json
import typing
import urllib.request
import zipfile
from collections.abc import Generator, Mapping
from pathlib import Path
from typing import Any, BinaryIO, Literal, TextIO, TypeGuard, cast, overload

from .io_typing import (
    _MODE_TO_SIMPLE,
    MODE_MAP,
    OPERATION_VALUES,
    REPRESENTATION_VALUES,
    InvalidOperationError,
    InvalidRepresentationError,
    Operation,
    Representation,
    ensure_sensible_default_encoding,
    ensure_sensible_newline,
)

__all__ = [
    "is_url",
    "open_inner_zipfile",
    "open_url",
    "safe_open",
    "safe_open_dict_reader",
    "safe_open_json",
    "safe_open_yaml",
    "write_json",
    "write_yaml",
]


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: typing.BinaryIO,
    *,
    operation: Operation = ...,
    representation: Representation = ...,
    encoding: str | None = ...,
) -> Generator[typing.BinaryIO, None, None]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: typing.TextIO,
    *,
    operation: Operation = ...,
    representation: Representation = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[typing.TextIO, None, None]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: str | Path,
    *,
    operation: Operation = ...,
    representation: Literal["text"] = "text",
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[typing.TextIO, None, None]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: str | Path,
    *,
    operation: Operation = ...,
    representation: Literal["binary"] = "binary",
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[typing.BinaryIO, None, None]: ...


@contextlib.contextmanager
def safe_open(  # noqa:C901
    path: str | Path | typing.TextIO | typing.BinaryIO,
    *,
    operation: Operation = "read",
    representation: Representation = "text",
    encoding: str | None = None,
    newline: str | None = None,
) -> Generator[typing.TextIO, None, None] | Generator[typing.BinaryIO, None, None]:
    """Safely open a file for reading or writing text."""
    if operation not in OPERATION_VALUES:
        raise InvalidOperationError(operation)
    if representation not in REPRESENTATION_VALUES:
        raise InvalidRepresentationError(representation)

    if isinstance(path, (str, Path)):
        encoding = ensure_sensible_default_encoding(encoding, representation=representation)
        newline = ensure_sensible_newline(newline, representation=representation)

        if is_url(path):
            if operation != "read":
                raise ValueError('can only use operation="read" with URLs')
            with open_url(
                path, representation=representation, encoding=encoding, newline=newline
            ) as file:
                yield file
        else:
            mode = MODE_MAP[operation, representation]
            path = Path(path).expanduser().resolve()
            if path.suffix.endswith(".gz"):
                with gzip.open(path, mode=mode, encoding=encoding, newline=newline) as file:
                    yield file  # type:ignore
            else:
                with open(path, mode=mode, encoding=encoding, newline=newline) as file:
                    yield file  # type:ignore

    elif isinstance(path, typing.TextIO | io.TextIOWrapper | io.TextIOBase):
        if representation != "text":
            raise ValueError(
                "must specify `text` representation when passing through a text file-like object"
            )
        yield path
    elif isinstance(path, typing.BinaryIO | io.BufferedReader | gzip.GzipFile):
        if representation != "binary":
            raise ValueError(
                "must specify `binary` representation when passing through "
                "a binary file-like object"
            )
        yield path
    else:
        raise TypeError(f"unsupported type for opening: {type(path)} - {path}")


def safe_open_json(
    path_or_url: str | Path | TextIO,
    *,
    encoding: str | None = None,
    newline: str | None = None,
) -> Any:
    """Safely open a file and parse as JSON."""
    with safe_open(
        path_or_url, representation="text", operation="read", encoding=encoding, newline=newline
    ) as file:
        return json.load(file)


def safe_open_yaml(
    path_or_url: str | Path | TextIO,
    *,
    encoding: str | None = None,
    newline: str | None = None,
) -> Any:
    """Safely open a file and parse as YAML."""
    import yaml

    with safe_open(
        path_or_url, representation="text", operation="read", encoding=encoding, newline=newline
    ) as file:
        return yaml.safe_load(file)


def write_yaml(
    data: Any,
    path: str | Path | TextIO,
    *,
    encoding: str | None = None,
    newline: str | None = None,
    **kwargs: Any,
) -> Any:
    """Write YAML to a file."""
    import yaml

    with safe_open(
        path, representation="text", operation="write", encoding=encoding, newline=newline
    ) as file:
        yaml.safe_dump(data, file, **kwargs)


def write_json(
    data: Any,
    path: str | Path | TextIO,
    *,
    encoding: str | None = None,
    newline: str | None = None,
    **kwargs: Any,
) -> Any:
    """Write JSON to a file."""
    with safe_open(
        path, representation="text", operation="write", encoding=encoding, newline=newline
    ) as file:
        json.dump(data, file, **kwargs)


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def open_inner_zipfile(
    zip_file: zipfile.ZipFile,
    inner_path: str,
    *,
    operation: Operation = ...,
    representation: Literal["text"] = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[typing.TextIO, None, None]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def open_inner_zipfile(
    zip_file: zipfile.ZipFile,
    inner_path: str,
    *,
    operation: Operation = ...,
    representation: Literal["binary"] = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[typing.BinaryIO, None, None]: ...


@contextlib.contextmanager
def open_inner_zipfile(
    zip_file: zipfile.ZipFile,
    inner_path: str,
    *,
    operation: Operation = "read",
    representation: Representation = "text",
    open_kwargs: Mapping[str, Any] | None = None,
    encoding: str | None = None,
    newline: str | None = None,
) -> Generator[typing.TextIO, None, None] | Generator[typing.BinaryIO, None, None]:
    """Open a file inside an already opened zip archive."""
    mode = _MODE_TO_SIMPLE[operation]
    encoding = ensure_sensible_default_encoding(encoding, representation=representation)
    newline = ensure_sensible_newline(newline, representation=representation)
    with zip_file.open(inner_path, mode=mode, **(open_kwargs or {})) as binary_file:
        if representation == "text":
            with io.TextIOWrapper(binary_file, encoding=encoding, newline=newline) as text_file:
                yield text_file
        elif representation == "binary":
            yield cast(typing.BinaryIO, binary_file)
        else:
            raise InvalidRepresentationError(representation)


@contextlib.contextmanager
def safe_open_dict_reader(
    f: str | Path | TextIO, *, delimiter: str = "\t", **kwargs: Any
) -> Generator[csv.DictReader[str], None, None]:
    """Open a CSV dictionary reader, wrapping :func:`csv.DictReader`.

    :param f: A path to a file, or an already open text-based IO object
    :param delimiter: The delimiter for writing to CSV
    :param kwargs: Keyword arguments to pass to :func:`csv.DictReader`

    :yields: A CSV reader object, constructed from :func:`csv.DictReader`
    """
    with safe_open(f, operation="read", representation="text") as file:
        yield csv.DictReader(file, delimiter=delimiter, **kwargs)


def is_url(s: str | Path | TextIO | Any) -> TypeGuard[str]:
    """Check if the object is a URL."""
    if isinstance(s, str) and (s.startswith("http://") or s.startswith("https://")):
        return True
    return False


# docstr-coverage:excused `overload`
@overload
@contextlib.contextmanager
def open_url(
    url: str,
    *,
    representation: Literal["text"] = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[TextIO, None, None]: ...


# docstr-coverage:excused `overload`
@overload
@contextlib.contextmanager
def open_url(
    url: str,
    *,
    representation: Literal["binary"] = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[BinaryIO, None, None]: ...


@contextlib.contextmanager
def open_url(
    url: str,
    *,
    representation: Representation = "text",
    encoding: str | None = None,
    newline: str | None = None,
) -> Generator[TextIO, None, None] | Generator[BinaryIO, None, None]:
    """Get a file-like object from a URL."""
    with urllib.request.urlopen(url) as response:  # noqa:S310
        match representation:
            case "text":
                yield io.TextIOWrapper(response, encoding=encoding, newline=newline)
            case "binary":
                yield io.BufferedReader(response)
