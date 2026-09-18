"""File opening utilities."""

from __future__ import annotations

import bz2
import contextlib
import csv
import gzip
import io
import json
import lzma
import sys
import typing
import urllib.request
import zipfile
from collections.abc import Generator, Mapping
from pathlib import Path
from typing import (
    IO,
    Any,
    Literal,
    Never,
    NotRequired,
    TypeAlias,
    TypedDict,
    TypeGuard,
    Unpack,
    overload,
)

from .io_typing import (
    _OPERATION_TO_BINARY_MODE,
    _OPERATION_TO_UNQUALIFIED_MODE,
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

try:
    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd

    zstd_open = zstd.open

except ImportError:

    def zstd_open(*args: Any, **kwargs: Any) -> Never:
        """Open a Zstandard compressed file in binary or text mode."""
        raise RuntimeError("zstd is not available")

    zstd_available = False
else:
    zstd_available = True


__all__ = [
    "is_url",
    "open_inner_zipfile",
    "open_url",
    "safe_open",
    "safe_open_dict_reader",
    "safe_open_json",
    "safe_open_yaml",
    "safe_read_text",
    "safe_write_text",
    "write_json",
    "write_yaml",
    "zstd_open",
]

TextSource: TypeAlias = str | Path | IO[str]

COMPRESSION_EXTENSIONS = ["gz", "xz", "bz2"]
if zstd_available:
    COMPRESSION_EXTENSIONS.append("zst")


class OpenKwargs(TypedDict):
    """Keyword arguments for open-like functions."""

    encoding: NotRequired[str | None]
    newline: NotRequired[str | None]
    buffering: NotRequired[int | None]


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: str | Path | IO[str] | IO[bytes],
    *,
    operation: Operation = ...,
    representation: Literal["text"] = "text",
    **kwargs: Unpack[OpenKwargs],
) -> Generator[IO[str]]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: str | Path | IO[str] | IO[bytes],
    *,
    operation: Operation = ...,
    representation: Literal["binary"] = "binary",
    **kwargs: Unpack[OpenKwargs],
) -> Generator[IO[bytes]]: ...


@contextlib.contextmanager
def safe_open(  # noqa:C901
    path: str | Path | IO[str] | IO[bytes],
    *,
    operation: Operation = "read",
    representation: Representation = "text",
    **kwargs: Unpack[OpenKwargs],
) -> Generator[IO[str]] | Generator[IO[bytes]]:
    """Safely open a file for reading or writing text."""
    if operation not in OPERATION_VALUES:
        raise InvalidOperationError(operation)
    if representation not in REPRESENTATION_VALUES:
        raise InvalidRepresentationError(representation)

    if isinstance(path, (str, Path)):
        encoding = ensure_sensible_default_encoding(
            kwargs.get("encoding"), representation=representation
        )
        newline = ensure_sensible_newline(kwargs.get("newline"), representation=representation)
        buffering = kwargs.get("buffering") or -1

        if is_url(path):
            if operation != "read":
                raise ValueError('can only use operation="read" with URLs')
            with open_url(
                path,
                representation=representation,
                encoding=encoding,
                newline=newline,
                buffering=buffering,
            ) as file:
                yield file
        else:
            mode = MODE_MAP[operation, representation]
            premode = _OPERATION_TO_BINARY_MODE[operation]
            path = Path(path).expanduser().resolve()
            if path.suffix.endswith(".gz"):
                with (
                    open(path, buffering=buffering, mode=premode) as raw,
                    gzip.open(raw, mode=mode, encoding=encoding, newline=newline) as gzf,
                ):
                    yield gzf  # type:ignore
            elif path.suffix.endswith(".bz2"):
                with (
                    open(path, buffering=buffering, mode=premode) as raw,
                    bz2.open(raw, mode=mode, encoding=encoding, newline=newline) as bz2f,
                ):
                    yield bz2f
            elif path.suffix.endswith(".xz"):
                with (
                    open(path, buffering=buffering, mode=premode) as raw,
                    lzma.open(raw, mode=mode, encoding=encoding, newline=newline) as lzmaf,
                ):
                    yield lzmaf
            elif path.suffix.endswith(".zst"):
                with (
                    open(path, buffering=buffering, mode=premode) as raw,
                    zstd_open(raw, mode=mode, encoding=encoding, newline=newline) as zstdf,
                ):
                    yield zstdf  # type:ignore
            else:
                with open(
                    path, mode=mode, encoding=encoding, newline=newline, buffering=buffering
                ) as file:
                    yield file

    elif isinstance(path, typing.TextIO | io.TextIOWrapper | io.TextIOBase):
        if representation != "text":
            path = path.buffer
        yield path

    # io.BufferedIOBase covers the LZMA, BZ2, Gzip, and ZSTD file types
    # as well as io.BufferedReader
    elif isinstance(path, typing.BinaryIO | io.BufferedIOBase):
        with _wrap_binary_if_needed(
            path, representation, encoding=kwargs.get("encoding"), newline=kwargs.get("newline")
        ) as yp:
            yield yp
    else:
        raise TypeError(f"unsupported type for opening: {type(path)} - {path}")


@contextlib.contextmanager
def _open_read_text(path: TextSource, **kwargs: Unpack[OpenKwargs]) -> Generator[IO[str]]:
    with safe_open(path, representation="text", operation="read", **kwargs) as file:
        yield file


@contextlib.contextmanager
def _open_write_text(path: TextSource, **kwargs: Unpack[OpenKwargs]) -> Generator[IO[str]]:
    with safe_open(path, representation="text", operation="write", **kwargs) as file:
        yield file


def safe_open_json(path_or_url: TextSource, **kwargs: Unpack[OpenKwargs]) -> Any:
    """Safely open a file and parse as JSON."""
    with _open_read_text(path_or_url, **kwargs) as file:
        return json.load(file)


def safe_open_yaml(path_or_url: TextSource, **kwargs: Unpack[OpenKwargs]) -> Any:
    """Safely open a file and parse as YAML."""
    import yaml

    with _open_read_text(path_or_url, **kwargs) as file:
        return yaml.safe_load(file)


def safe_write_text(s: str, path: TextSource, **kwargs: Unpack[OpenKwargs]) -> int:
    """Write text to a file."""
    with _open_write_text(path, **kwargs) as file:
        return file.write(s)


def safe_read_text(path: TextSource, **kwargs: Unpack[OpenKwargs]) -> str:
    """Read text from a file."""
    with _open_read_text(path, **kwargs) as file:
        return file.read()


def write_yaml(
    data: Any,
    path: TextSource,
    *,
    encoding: str | None = None,
    newline: str | None = None,
    buffering: int | None = None,
    indent: int | None = None,
    allow_unicode: bool = True,
    **kwargs: Any,
) -> Any:
    """Write YAML to a file."""
    import yaml

    with _open_write_text(path, encoding=encoding, newline=newline, buffering=buffering) as file:
        yaml.safe_dump(data, file, indent=indent, allow_unicode=allow_unicode, **kwargs)


def write_json(
    data: Any,
    path: TextSource,
    *,
    encoding: str | None = None,
    newline: str | None = None,
    ensure_ascii: bool = False,
    indent: int | None = None,
    trailing_newline: bool = True,
    **kwargs: Any,
) -> Any:
    """Write JSON to a file."""
    with _open_write_text(path, encoding=encoding, newline=newline) as file:
        json.dump(data, file, ensure_ascii=ensure_ascii, indent=indent, **kwargs)
        if trailing_newline:
            file.write("\n")


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
) -> Generator[IO[str]]: ...


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
) -> Generator[IO[bytes]]: ...


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
) -> Generator[IO[str]] | Generator[IO[bytes]]:
    """Open a file inside an already opened zip archive."""
    mode = _OPERATION_TO_UNQUALIFIED_MODE[operation]
    encoding = ensure_sensible_default_encoding(encoding, representation=representation)
    newline = ensure_sensible_newline(newline, representation=representation)
    with (
        zip_file.open(inner_path, mode=mode, **(open_kwargs or {})) as binary_file,
        _wrap_binary_if_needed(
            binary_file, representation, encoding=encoding, newline=newline
        ) as yf,
    ):
        yield yf


ZZ = typing.TypeVar("ZZ", bound=IO[bytes])


@overload
@contextlib.contextmanager
def _wrap_binary_if_needed(
    file: ZZ,
    representation: Literal["text"],
    *,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[io.TextIOWrapper[ZZ]]: ...


@overload
@contextlib.contextmanager
def _wrap_binary_if_needed(
    file: ZZ,
    representation: Literal["binary"],
    *,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Generator[ZZ]: ...


@contextlib.contextmanager
def _wrap_binary_if_needed(
    file: ZZ,
    representation: Representation,
    *,
    encoding: str | None = None,
    newline: str | None = None,
) -> Generator[ZZ | io.TextIOWrapper[ZZ]]:
    if representation == "text":
        with io.TextIOWrapper(file, encoding=encoding, newline=newline) as text_file:
            yield text_file
    elif representation == "binary":
        yield file
    else:
        raise InvalidRepresentationError(representation)


@contextlib.contextmanager
def safe_open_dict_reader(
    f: TextSource, *, delimiter: str = "\t", **kwargs: Any
) -> Generator[csv.DictReader[str]]:
    """Open a CSV dictionary reader, wrapping :func:`csv.DictReader`.

    :param f: A path to a file, or an already open text-based IO object
    :param delimiter: The delimiter for writing to CSV
    :param kwargs: Keyword arguments to pass to :func:`csv.DictReader`

    :yields: A CSV reader object, constructed from :func:`csv.DictReader`
    """
    with _open_read_text(f) as file:
        yield csv.DictReader(file, delimiter=delimiter, **kwargs)


def is_url(obj: Any) -> TypeGuard[str]:
    """Check if the object is a URL."""
    return isinstance(obj, str) and obj.startswith(("http://", "https://"))


# docstr-coverage:excused `overload`
@overload
@contextlib.contextmanager
def open_url(
    url: str,
    *,
    representation: Literal["text"] = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
    buffering: int | None = None,
) -> Generator[IO[str]]: ...


# docstr-coverage:excused `overload`
@overload
@contextlib.contextmanager
def open_url(
    url: str,
    *,
    representation: Literal["binary"] = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
    buffering: int | None = None,
) -> Generator[IO[bytes]]: ...


@contextlib.contextmanager
def open_url(
    url: str,
    *,
    representation: Representation = "text",
    encoding: str | None = None,
    newline: str | None = None,
    buffering: int | None = None,
) -> Generator[IO[str]] | Generator[IO[bytes]]:
    """Get a file-like object from a URL."""
    if buffering is not None and buffering != -1:
        raise NotImplementedError("can not buffer when opening a URL")
    with urllib.request.urlopen(url) as response:  # noqa:S310
        match representation:
            case "text":
                yield io.TextIOWrapper(response, encoding=encoding, newline=newline)
            case "binary":
                yield io.BufferedReader(response)
