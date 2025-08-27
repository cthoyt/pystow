"""Utilities."""

from __future__ import annotations

import contextlib
import csv
import gzip
import hashlib
import io
import logging
import lzma
import os
import pickle
import shutil
import tarfile
import tempfile
import typing
import urllib.error
import zipfile
from collections.abc import Collection, Generator, Iterable, Iterator, Mapping
from functools import partial
from io import BytesIO, StringIO
from pathlib import Path, PurePosixPath
from subprocess import check_output
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    TextIO,
    cast,
)
from urllib.parse import urlparse
from urllib.request import urlretrieve
from uuid import uuid4

import requests
from tqdm.auto import tqdm
from typing_extensions import TypeAlias

from .constants import (
    PYSTOW_HOME_ENVVAR,
    PYSTOW_NAME_DEFAULT,
    PYSTOW_NAME_ENVVAR,
    PYSTOW_USE_APPDIRS,
    README_TEXT,
    TimeoutHint,
)

if TYPE_CHECKING:
    import _csv

    import botocore.client
    import bs4
    import lxml.etree
    import numpy.typing
    import pandas
    import rdflib

__all__ = [
    "DownloadBackend",
    "DownloadError",
    "Hash",
    "HexDigestError",
    "HexDigestMismatch",
    "UnexpectedDirectory",
    "UnexpectedDirectoryError",
    "download",
    "download_from_google",
    "download_from_s3",
    "get_base",
    "get_commit",
    "get_df_io",
    "get_hashes",
    "get_hexdigests_remote",
    "get_home",
    "get_name",
    "get_np_io",
    "get_offending_hexdigests",
    "get_soup",
    "getenv_path",
    "gunzip",
    "mkdir",
    "mock_envvar",
    "mock_home",
    "n",
    "name_from_s3_key",
    "name_from_url",
    "open_zip_reader",
    "open_zip_writer",
    "path_to_sqlite",
    "raise_on_digest_mismatch",
    "read_rdf",
    "read_tarfile_csv",
    "read_tarfile_xml",
    "read_zip_np",
    "read_zipfile_csv",
    "read_zipfile_rdf",
    "read_zipfile_xml",
    "safe_open",
    "safe_open_dict_reader",
    "safe_open_dict_writer",
    "safe_open_reader",
    "safe_open_writer",
    "write_lzma_csv",
    "write_pickle_gz",
    "write_tarfile_csv",
    "write_tarfile_xml",
    "write_zipfile_csv",
    "write_zipfile_np",
    "write_zipfile_rdf",
    "write_zipfile_xml",
]

logger = logging.getLogger(__name__)

#: Represents an available backend for downloading
DownloadBackend: TypeAlias = Literal["urllib", "requests"]

#: This type alias uses a stub-only constructor, meaning that
#: hashlib._Hash isn't actually part of the code, but MyPy injects it
#: so we can do type checking
Hash: TypeAlias = "hashlib._Hash"

Reader: TypeAlias = "_csv._reader"
Writer: TypeAlias = "_csv._writer"

#: A human-readable flag for how to open a file.
Operation: TypeAlias = Literal["read", "write"]
OPERATION_VALUES: set[str] = set(typing.get_args(Operation))

#: A human-readable flag for how to open a file.
Representation: TypeAlias = Literal["text", "binary"]
REPRESENTATION_VALUES: set[str] = set(typing.get_args(Representation))


class HexDigestMismatch(NamedTuple):
    """Contains information about a hexdigest mismatch."""

    #: the name of the algorithm
    name: str
    #: the observed/actual hexdigest, encoded as a string
    actual: str
    #: the expected hexdigest, encoded as a string
    expected: str


class HexDigestError(ValueError):
    """Thrown if the hashsums do not match expected hashsums."""

    def __init__(self, offending_hexdigests: Collection[HexDigestMismatch]):
        """Instantiate the exception.

        :param offending_hexdigests: The result from :func:`get_offending_hexdigests`
        """
        self.offending_hexdigests = offending_hexdigests

    def __str__(self) -> str:
        return "\n".join(
            (
                "Hexdigest of downloaded file does not match the expected ones!",
                *(
                    f"\t{name} actual: {actual} vs. expected: {expected}"
                    for name, actual, expected in self.offending_hexdigests
                ),
            )
        )


class UnexpectedDirectoryError(FileExistsError):
    """Thrown if a directory path is given where file path should have been."""

    def __init__(self, path: Path):
        """Instantiate the exception.

        :param path: The path to a directory that should have been a file.
        """
        self.path = path

    def __str__(self) -> str:
        return f"got directory instead of file: {self.path}"


#: Backwards compatible name
UnexpectedDirectory = UnexpectedDirectoryError


def get_hexdigests_remote(
    hexdigests_remote: Mapping[str, str] | None, hexdigests_strict: bool = False
) -> Mapping[str, str]:
    """Process hexdigests via URLs.

    :param hexdigests_remote: The expected hexdigests as (algorithm_name, url to file
        with expected hex digest) pairs.
    :param hexdigests_strict: Set this to `False` to stop automatically checking for the
        `algorithm(filename)=hash` format

    :returns: A mapping of algorithms to hexdigests
    """
    rv = {}
    for key, url in (hexdigests_remote or {}).items():
        text = requests.get(url, timeout=15).text
        if not hexdigests_strict and "=" in text:
            text = text.rsplit("=", 1)[-1].strip()
        rv[key] = text
    return rv


def get_offending_hexdigests(
    path: str | Path,
    chunk_size: int | None = None,
    hexdigests: Mapping[str, str] | None = None,
    hexdigests_remote: Mapping[str, str] | None = None,
    hexdigests_strict: bool = False,
) -> Collection[HexDigestMismatch]:
    """Check a file for hash sums.

    :param path: The file path.
    :param chunk_size: The chunk size for reading the file.
    :param hexdigests: The expected hexdigests as (algorithm_name, expected_hex_digest)
        pairs.
    :param hexdigests_remote: The expected hexdigests as (algorithm_name, url to file
        with expected hexdigest) pairs.
    :param hexdigests_strict: Set this to false to stop automatically checking for the
        `algorithm(filename)=hash` format

    :returns: A collection of observed / expected hexdigests where the digests do not
        match.
    """
    hexdigests = dict(
        **(hexdigests or {}),
        **get_hexdigests_remote(hexdigests_remote, hexdigests_strict=hexdigests_strict),
    )

    # If there aren't any keys in the combine dictionaries,
    # then there won't be any mismatches
    if not hexdigests:
        return []

    logger.info(f"Checking hash sums for file: {path}")

    # instantiate algorithms
    algorithms = get_hashes(path=path, names=set(hexdigests), chunk_size=chunk_size)

    # Compare digests
    mismatches = []
    for alg, expected_digest in hexdigests.items():
        observed_digest = algorithms[alg].hexdigest()
        if observed_digest != expected_digest:
            logger.error(f"{alg} expected {expected_digest} but got {observed_digest}.")
            mismatches.append(HexDigestMismatch(alg, observed_digest, expected_digest))
        else:
            logger.debug(f"Successfully checked with {alg}.")

    return mismatches


def get_hashes(
    path: str | Path,
    names: Iterable[str],
    *,
    chunk_size: int | None = None,
) -> Mapping[str, Hash]:
    """Calculate several hexdigests of hash algorithms for a file concurrently.

    :param path: The file path.
    :param names: Names of the hash algorithms in :mod:`hashlib`
    :param chunk_size: The chunk size for reading the file.

    :returns: A collection of observed hexdigests
    """
    path = Path(path).resolve()
    if chunk_size is None:
        chunk_size = 64 * 2**10

    # instantiate hash algorithms
    algorithms: Mapping[str, Hash] = {name: hashlib.new(name) for name in names}

    # calculate hash sums of file incrementally
    buffer = memoryview(bytearray(chunk_size))
    with path.open("rb", buffering=0) as file:
        for this_chunk_size in iter(lambda: file.readinto(buffer), 0):
            for alg in algorithms.values():
                alg.update(buffer[:this_chunk_size])

    return algorithms


def raise_on_digest_mismatch(
    *,
    path: Path,
    hexdigests: Mapping[str, str] | None = None,
    hexdigests_remote: Mapping[str, str] | None = None,
    hexdigests_strict: bool = False,
) -> None:
    """Raise a HexDigestError if the digests do not match.

    :param path: The file path.
    :param hexdigests: The expected hexdigests as (algorithm_name, expected_hex_digest)
        pairs.
    :param hexdigests_remote: The expected hexdigests as (algorithm_name, url to file
        with expected hexdigest) pairs.
    :param hexdigests_strict: Set this to false to stop automatically checking for the
        `algorithm(filename)=hash` format

    :raises HexDigestError: if there are any offending hex digests The expected
        hexdigests as (algorithm_name, url to file with expected hexdigest) pairs.
    """
    offending_hexdigests = get_offending_hexdigests(
        path=path,
        hexdigests=hexdigests,
        hexdigests_remote=hexdigests_remote,
        hexdigests_strict=hexdigests_strict,
    )
    if offending_hexdigests:
        raise HexDigestError(offending_hexdigests)


class TqdmReportHook(tqdm):  # type:ignore
    """A custom progress bar that can be used with urllib.

    Based on https://gist.github.com/leimao/37ff6e990b3226c2c9670a2cd1e4a6f5
    """

    def update_to(
        self,
        blocks: int = 1,
        block_size: int = 1,
        total_size: int | None = None,
    ) -> None:
        """Update the internal state based on a urllib report hook.

        :param blocks: Number of blocks transferred so far
        :param block_size: Size of each block (in tqdm units)
        :param total_size: Total size (in tqdm units). If [default: None] remains
            unchanged.
        """
        if total_size is not None:
            self.total = total_size
        self.update(blocks * block_size - self.n)  # will also set self.n = b * bsize


def download(
    url: str,
    path: str | Path,
    force: bool = True,
    clean_on_failure: bool = True,
    backend: DownloadBackend = "urllib",
    hexdigests: Mapping[str, str] | None = None,
    hexdigests_remote: Mapping[str, str] | None = None,
    hexdigests_strict: bool = False,
    progress_bar: bool = True,
    tqdm_kwargs: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> None:
    """Download a file from a given URL.

    :param url: URL to download
    :param path: Path to download the file to
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised
        during download
    :param backend: The downloader to use. Choose 'urllib' or 'requests'
    :param hexdigests: The expected hexdigests as (algorithm_name, expected_hex_digest)
        pairs.
    :param hexdigests_remote: The expected hexdigests as (algorithm_name, url to file
        with expected hexdigest) pairs.
    :param hexdigests_strict: Set this to false to stop automatically checking for the
        `algorithm(filename)=hash` format
    :param progress_bar: Set to true to show a progress bar while downloading
    :param tqdm_kwargs: Override the default arguments passed to :class:`tadm.tqdm` when
        progress_bar is True.
    :param kwargs: The keyword arguments to pass to :func:`urllib.request.urlretrieve`
        or to `requests.get` depending on the backend chosen. If using 'requests'
        backend, `stream` is set to True by default.

    :raises Exception: Thrown if an error besides a keyboard interrupt is thrown during
        download
    :raises KeyboardInterrupt: If a keyboard interrupt is thrown during download
    :raises UnexpectedDirectory: If a directory is given for the ``path`` argument
    :raises ValueError: If an invalid backend is chosen
    :raises DownloadError: If an error occurs during download
    """
    path = Path(path).resolve()

    if path.is_dir():
        raise UnexpectedDirectoryError(path)
    if path.is_file() and not force:
        raise_on_digest_mismatch(
            path=path,
            hexdigests=hexdigests,
            hexdigests_remote=hexdigests_remote,
            hexdigests_strict=hexdigests_strict,
        )
        logger.debug("did not re-download %s from %s", path, url)
        return

    _tqdm_kwargs = {
        "unit": "B",
        "unit_scale": True,
        "unit_divisor": 1024,
        "miniters": 1,
        "disable": not progress_bar,
        "desc": f"Downloading {path.name}",
        "leave": False,
    }
    if tqdm_kwargs:
        _tqdm_kwargs.update(tqdm_kwargs)

    try:
        if backend == "urllib":
            logger.info("downloading with urllib from %s to %s", url, path)
            with TqdmReportHook(**_tqdm_kwargs) as t:
                try:
                    urlretrieve(url, path, reporthook=t.update_to, **kwargs)  # noqa:S310
                except urllib.error.URLError as e:
                    raise DownloadError(backend, url, path, e) from e
        elif backend == "requests":
            kwargs.setdefault("stream", True)
            try:
                # see https://requests.readthedocs.io/en/master/user/quickstart/#raw-response-content
                # pattern from https://stackoverflow.com/a/39217788/5775947
                with requests.get(url, **kwargs) as response, path.open("wb") as file:  # noqa:S113
                    logger.info(
                        "downloading (stream=%s) with requests from %s to %s",
                        kwargs["stream"],
                        url,
                        path,
                    )
                    # Solution for progress bar from https://stackoverflow.com/a/63831344/5775947
                    total_size = int(response.headers.get("Content-Length", 0))
                    # Decompress if needed
                    response.raw.read = partial(  # type:ignore[method-assign]
                        response.raw.read, decode_content=True
                    )
                    with tqdm.wrapattr(
                        response.raw, "read", total=total_size, **_tqdm_kwargs
                    ) as fsrc:
                        shutil.copyfileobj(fsrc, file)
            except requests.exceptions.ConnectionError as e:
                raise DownloadError(backend, url, path, e) from e
        else:
            raise ValueError(f'Invalid backend: {backend}. Use "requests" or "urllib".')
    except (Exception, KeyboardInterrupt):
        if clean_on_failure:
            _unlink(path)
        raise

    raise_on_digest_mismatch(
        path=path,
        hexdigests=hexdigests,
        hexdigests_remote=hexdigests_remote,
        hexdigests_strict=hexdigests_strict,
    )


class DownloadError(OSError):
    """An error that wraps information from a requests or urllib download failure."""

    def __init__(
        self,
        backend: DownloadBackend,
        url: str,
        path: Path,
        exc: urllib.error.URLError | requests.exceptions.ConnectionError,
    ) -> None:
        """Initialize the error.

        :param backend: The backend used
        :param url: The url that failed to download
        :param path: The path that was supposed to be downloaded to
        :param exc: The exception raised
        """
        self.backend = backend
        self.url = url
        self.path = path
        self.exc = exc
        # TODO parse out HTTP error code, if possible

    def __str__(self) -> str:
        return f"Failed with {self.backend} to download {self.url} to {self.path}"


def name_from_url(url: str) -> str:
    """Get the filename from the end of the URL.

    :param url: A URL

    :returns: The name of the file at the end of the URL
    """
    parse_result = urlparse(url)
    path = PurePosixPath(parse_result.path)
    name = path.name
    return name


def base_from_gzip_name(name: str) -> str:
    """Get the base name for a file after stripping the gz ending.

    :param name: The name of the gz file

    :returns: The cleaned name of the file, with no gz ending

    :raises ValueError: if the file does not end with ".gz"
    """
    if not name.endswith(".gz"):
        raise ValueError(f"Name does not end with .gz: {name}")
    return name[: -len(".gz")]


def name_from_s3_key(key: str) -> str:
    """Get the filename from the S3 key.

    :param key: A S3 path

    :returns: The name of the file
    """
    return key.split("/")[-1]


def mkdir(path: Path, ensure_exists: bool = True) -> None:
    """Make a directory (or parent directory if a file is given) if flagged with ``ensure_exists``.

    :param path: The path to a directory
    :param ensure_exists: Should the directories leading to the path be created if they
        don't already exist?
    """
    if ensure_exists:
        path.mkdir(exist_ok=True, parents=True)


@contextlib.contextmanager
def mock_envvar(envvar: str, value: str) -> Iterator[None]:
    """Mock the environment variable then delete it after the test is over.

    :param envvar: The environment variable to mock
    :param value: The value to temporarily put in the environment variable during this
        mock.

    :yield: None, since this just mocks the environment variable for the time being.
    """
    original_value = os.environ.get(envvar)
    os.environ[envvar] = value
    yield
    if original_value is None:
        del os.environ[envvar]
    else:
        os.environ[envvar] = original_value


@contextlib.contextmanager
def mock_home() -> Iterator[Path]:
    """Mock the PyStow home environment variable, yields the directory name.

    :yield: The path to the temporary directory.
    """
    with tempfile.TemporaryDirectory() as directory:
        with mock_envvar(PYSTOW_HOME_ENVVAR, directory):
            yield Path(directory)


def getenv_path(envvar: str, default: Path, ensure_exists: bool = True) -> Path:
    """Get an environment variable representing a path, or use the default.

    :param envvar: The environmental variable name to check
    :param default: The default path to return if the environmental variable is not set
    :param ensure_exists: Should the directories leading to the path be created if they
        don't already exist?

    :returns: A path either specified by the environmental variable or by the default.
    """
    rv = Path(os.getenv(envvar, default=default)).expanduser()
    mkdir(rv, ensure_exists=ensure_exists)
    return rv


def n() -> str:
    """Get a random string for testing.

    :returns: A random string for testing purposes.
    """
    return str(uuid4())


def get_df_io(df: pandas.DataFrame, sep: str = "\t", index: bool = False, **kwargs: Any) -> BytesIO:
    """Get the dataframe as bytes.

    :param df: A dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index: Should the index be output? Overrides the Pandas default to be false.
    :param kwargs: Additional kwargs to pass to :func:`pandas.DataFrame.to_csv`.

    :returns: A bytes object that can be used as a file.
    """
    sio = StringIO()
    df.to_csv(sio, sep=sep, index=index, **kwargs)
    sio.seek(0)
    bio = BytesIO(sio.read().encode("utf-8"))
    return bio


def get_np_io(arr: numpy.typing.ArrayLike, **kwargs: Any) -> BytesIO:
    """Get the numpy object as bytes.

    :param arr: Array-like
    :param kwargs: Additional kwargs to pass to :func:`numpy.save`.

    :returns: A bytes object that can be used as a file.
    """
    import numpy as np

    bio = BytesIO()
    np.save(bio, arr, **kwargs)
    bio.seek(0)
    return bio


def write_pickle_gz(
    obj: Any,
    path: str | Path,
    **kwargs: Any,
) -> None:
    """Write an object to a gzipped pickle.

    :param obj: The object to write
    :param path: The path of the file to write to
    :param kwargs: Additional kwargs to pass to :func:`pickle.dump`
    """
    with gzip.open(path, mode="wb") as file:
        pickle.dump(obj, file, **kwargs)


def write_lzma_csv(
    df: pandas.DataFrame,
    path: str | Path,
    sep: str = "\t",
    index: bool = False,
    **kwargs: Any,
) -> None:
    """Write a dataframe as an lzma-compressed file.

    :param df: A dataframe
    :param path: The path to the resulting LZMA compressed dataframe file
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index: Should the index be output? Overrides the Pandas default to be false.
    :param kwargs: Additional kwargs to pass to :func:`get_df_io` and transitively to
        :func:`pandas.DataFrame.to_csv`.
    """
    bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    with lzma.open(path, "wb") as file:
        file.write(bytes_io.read())


def write_zipfile_csv(
    df: pandas.DataFrame,
    path: str | Path,
    inner_path: str,
    sep: str = "\t",
    index: bool = False,
    **kwargs: Any,
) -> None:
    """Write a dataframe to an inner CSV file to a zip archive.

    :param df: A dataframe
    :param path: The path to the resulting zip archive
    :param inner_path: The path inside the zip archive to write the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index: Should the index be output? Overrides the Pandas default to be false.
    :param kwargs: Additional kwargs to pass to :func:`get_df_io` and transitively to
        :func:`pandas.DataFrame.to_csv`.
    """
    bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    with open_zipfile(path, inner_path, operation="write", representation="binary") as file:
        file.write(bytes_io.read())


def read_zipfile_csv(
    path: str | Path, inner_path: str, sep: str = "\t", **kwargs: Any
) -> pandas.DataFrame:
    """Read an inner CSV file from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param kwargs: Additional kwargs to pass to :func:`pandas.read_csv`.

    :returns: A dataframe
    """
    import pandas as pd

    with open_zipfile(path, inner_path, representation="text", operation="read") as file:
        return pd.read_csv(file, sep=sep, **kwargs)


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def open_zipfile(
    path: str | Path,
    inner_path: str,
    *,
    operation: Operation = ...,
    representation: Literal["text"],
) -> Generator[typing.TextIO, None, None]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def open_zipfile(
    path: str | Path,
    inner_path: str,
    *,
    operation: Operation = ...,
    representation: Literal["binary"],
) -> Generator[typing.BinaryIO, None, None]: ...


@contextlib.contextmanager
def open_zipfile(
    path: str | Path,
    inner_path: str,
    *,
    operation: Operation = "read",
    representation: Representation,
) -> Generator[typing.TextIO, None, None] | Generator[typing.BinaryIO, None, None]:
    """Open a zipfile."""
    mode: Literal["r", "w"] = "r" if operation == "read" else "w"
    # there might be a better way to deal with the mode here
    with zipfile.ZipFile(file=path, mode=mode) as zip_file:
        with zip_file.open(inner_path, mode=mode) as binary_file:
            if representation == "text":
                with io.TextIOWrapper(binary_file, encoding="utf-8") as text_file:
                    yield text_file
            elif representation == "binary":
                yield cast(typing.BinaryIO, binary_file)
            else:
                raise ValueError


@contextlib.contextmanager
def open_tarfile(
    path: str | Path,
    inner_path: str,
    *,
    operation: Operation = "read",
    representation: Representation = "binary",
) -> Generator[typing.IO[bytes], None, None]:
    """Open a tar file."""
    if representation != "binary":
        raise NotImplementedError

    if operation == "read":
        with tarfile.open(path, "r") as tar:
            member = tar.getmember(inner_path)
            file = tar.extractfile(member)
            if file is None:
                raise FileNotFoundError(f"could not find {inner_path} in tarfile {path}")
            yield file
    elif operation == "write":
        file = BytesIO()
        yield file
        file.seek(0)
        tarinfo = tarfile.TarInfo(name=inner_path)
        tarinfo.size = len(file.getbuffer())
        with tarfile.TarFile(path, mode="w") as tar_file:
            tar_file.addfile(tarinfo, file)
    else:
        raise ValueError


@contextlib.contextmanager
def open_zip_reader(
    path: str | Path, inner_path: str, delimiter: str = "\t", **kwargs: Any
) -> Generator[Reader, None, None]:
    """Read an inner CSV file from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the CSV
    :param delimiter: The separator in the CSV. Defaults to tab.
    :param kwargs: Additional kwargs to pass to :func:`csv.reader`.

    :returns: A reader over the file
    """
    with open_zipfile(path, inner_path, representation="text") as file:
        yield csv.reader(file, delimiter=delimiter, **kwargs)


@contextlib.contextmanager
def open_zip_writer(
    path: str | Path, inner_path: str, delimiter: str = "\t", **kwargs: Any
) -> Generator[Writer, None, None]:
    """Open a writer for an inner CSV file from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the CSV
    :param delimiter: The separator in the CSV. Defaults to tab.
    :param kwargs: Additional kwargs to pass to :func:`csv.writer`.

    :returns: A writer over the file
    """
    with open_zipfile(path, inner_path, operation="write", representation="text") as file:
        yield csv.writer(file, delimiter=delimiter, **kwargs)


def write_zipfile_xml(
    element_tree: lxml.etree.ElementTree,
    path: str | Path,
    inner_path: str,
    **kwargs: Any,
) -> None:
    """Write an XML element tree to an inner XML file to a zip archive.

    :param element_tree: An XML element tree
    :param path: The path to the resulting zip archive
    :param inner_path: The path inside the zip archive to write the XML element
    :param kwargs: Additional kwargs to pass to :func:`lxml.etree.tostring`
    """
    from lxml import etree

    kwargs.setdefault("pretty_print", True)
    with open_zipfile(path, inner_path, operation="write", representation="binary") as file:
        file.write(etree.tostring(element_tree, **kwargs))


def read_zipfile_xml(path: str | Path, inner_path: str, **kwargs: Any) -> lxml.etree.ElementTree:
    """Read an inner XML file from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the xml file
    :param kwargs: Additional kwargs to pass to :func:`lxml.etree.parse`

    :returns: An XML element tree
    """
    from lxml import etree

    with open_zipfile(path, inner_path, operation="read", representation="binary") as file:
        return etree.parse(file, **kwargs)


def write_zipfile_np(
    arr: numpy.typing.ArrayLike,
    path: str | Path,
    inner_path: str,
    **kwargs: Any,
) -> None:
    """Write a dataframe to an inner CSV file to a zip archive.

    :param arr: Array-like
    :param path: The path to the resulting zip archive
    :param inner_path: The path inside the zip archive to write the dataframe
    :param kwargs: Additional kwargs to pass to :func:`get_np_io` and transitively to
        :func:`numpy.save`.
    """
    import numpy as np

    with open_zipfile(path, inner_path, operation="write", representation="binary") as file:
        np.save(file, arr, **kwargs)


def read_zip_np(path: str | Path, inner_path: str, **kwargs: Any) -> numpy.typing.ArrayLike:
    """Read an inner numpy array-like from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the dataframe
    :param kwargs: Additional kwargs to pass to :func:`numpy.load`.

    :returns: A numpy array or other object
    """
    import numpy as np

    with open_zipfile(path, inner_path, operation="read", representation="binary") as file:
        return cast(np.typing.ArrayLike, np.load(file, **kwargs))


def read_zipfile_rdf(path: str | Path, inner_path: str, **kwargs: Any) -> rdflib.Graph:
    """Read an inner RDF file from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the dataframe
    :param kwargs: Additional kwargs to pass to :meth:`rdflib.Graph.parse`.

    :returns: A graph
    """
    import rdflib

    graph = rdflib.Graph()
    with open_zipfile(path, inner_path, operation="read", representation="binary") as file:
        graph.parse(file, **kwargs)
    return graph


def write_zipfile_rdf(
    graph: rdflib.Graph, path: str | Path, inner_path: str, **kwargs: Any
) -> None:
    """Read an inner RDF file from a zip archive.

    :param graph: The graph to write
    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the dataframe
    :param kwargs: Additional kwargs to pass to :meth:`rdflib.Graph.parse`.
    """
    with open_zipfile(path, inner_path, operation="write", representation="binary") as file:
        graph.serialize(file, **kwargs)


def write_tarfile_csv(
    df: pandas.DataFrame,
    path: str | Path,
    inner_path: str,
    sep: str = "\t",
    index: bool = False,
    **kwargs: Any,
) -> None:
    """Write a dataframe to an inner CSV file from a tar archive.

    :param df: A dataframe
    :param path: The path to the resulting tar archive
    :param inner_path: The path inside the tar archive to write the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index: Should the index be output? Overrides the Pandas default to be false.
    :param kwargs: Additional kwargs to pass to :func:`get_df_io` and transitively to
        :func:`pandas.DataFrame.to_csv`.
    """
    with open_tarfile(path, inner_path, operation="write") as file:
        df.to_csv(file, sep=sep, index=index, **kwargs)


def write_tarfile_xml(
    element_tree: lxml.etree.ElementTree,
    path: str | Path,
    inner_path: str,
    **kwargs: Any,
) -> None:
    """Write an XML document a tar archive.

    :param element_tree: An element
    :param path: The path to the resulting tar archive
    :param inner_path: The path inside the tar archive to write the dataframe
    :param kwargs: Additional kwargs to pass to :func:`lxml.etree.tostring`
    """
    from lxml import etree

    kwargs.setdefault("pretty_print", True)

    with open_tarfile(path, inner_path, operation="write") as file:
        file.write(etree.tostring(element_tree, **kwargs))


def read_tarfile_csv(
    path: str | Path, inner_path: str, sep: str = "\t", **kwargs: Any
) -> pandas.DataFrame:
    """Read an inner CSV file from a tar archive.

    :param path: The path to the tar archive
    :param inner_path: The path inside the tar archive to the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param kwargs: Additional kwargs to pass to :func:`pandas.read_csv`.

    :returns: A dataframe
    """
    import pandas as pd

    with open_tarfile(path, inner_path) as file:
        return pd.read_csv(file, sep=sep, **kwargs)


def read_tarfile_xml(path: str | Path, inner_path: str, **kwargs: Any) -> lxml.etree.ElementTree:
    """Read an inner XML file from a tar archive.

    :param path: The path to the tar archive
    :param inner_path: The path inside the tar archive to the xml file
    :param kwargs: Additional kwargs to pass to :func:`lxml.etree.parse`

    :returns: An XML element tree
    """
    from lxml import etree

    with open_tarfile(path, inner_path) as file:
        return etree.parse(file, **kwargs)


def read_rdf(path: str | Path, **kwargs: Any) -> rdflib.Graph:
    """Read an RDF file with :mod:`rdflib`.

    :param path: The path to the RDF file
    :param kwargs: Additional kwargs to pass to :func:`rdflib.Graph.parse`

    :returns: A parsed RDF graph
    """
    import rdflib

    graph = rdflib.Graph()
    with safe_open(path, representation="binary", operation="read") as file:
        graph.parse(file, **kwargs)
    return graph


def write_sql(df: pandas.DataFrame, name: str, path: str | Path, **kwargs: Any) -> None:
    """Write a dataframe as a SQL table.

    :param df: A dataframe
    :param name: The table the database to write to
    :param path: The path to the resulting tar archive
    :param kwargs: Additional keyword arguments to pass to
        :meth:`pandas.DataFrame.to_sql`
    """
    import sqlite3

    with contextlib.closing(sqlite3.connect(path)) as conn:
        df.to_sql(name, conn, **kwargs)


def get_commit(org: str, repo: str, provider: str = "git") -> str:
    """Get last commit hash for the given repo.

    :param org: The GitHub organization or owner
    :param repo: The GitHub repository name
    :param provider: The method for getting the most recent commit

    :returns: A commit hash's hex digest as a string

    :raises ValueError: if an invalid provider is given
    """
    if provider == "git":
        output = check_output(["git", "ls-remote", f"https://github.com/{org}/{repo}"])  # noqa
        lines = (line.strip().split("\t") for line in output.decode("utf8").splitlines())
        rv = next(line[0] for line in lines if line[1] == "HEAD")
    elif provider == "github":
        res = requests.get(f"https://api.github.com/repos/{org}/{repo}/branches/master", timeout=15)
        res_json = res.json()
        rv = res_json["commit"]["sha"]
    else:
        raise ValueError(f"invalid implementation: {provider}")
    return rv


CHUNK_SIZE = 32768
DOWNLOAD_URL = "https://docs.google.com/uc?export=download"
TOKEN_KEY = "download_warning"  # noqa:S105


def download_from_google(
    file_id: str,
    path: str | Path,
    force: bool = True,
    clean_on_failure: bool = True,
    hexdigests: Mapping[str, str] | None = None,
) -> None:
    """Download a file from google drive.

    Implementation inspired by https://github.com/ndrplz/google-drive-downloader.

    :param file_id: The google file identifier
    :param path: The place to write the file
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised
        during download
    :param hexdigests: The expected hexdigests as (algorithm_name, expected_hex_digest)
        pairs.

    :raises Exception: Thrown if an error besides a keyboard interrupt is thrown during
        download
    :raises KeyboardInterrupt: If a keyboard interrupt is thrown during download
    :raises UnexpectedDirectory: If a directory is given for the ``path`` argument
    """
    path = Path(path).resolve()

    if path.is_dir():
        raise UnexpectedDirectoryError(path)
    if path.is_file() and not force:
        raise_on_digest_mismatch(path=path, hexdigests=hexdigests)
        logger.debug("did not re-download %s from Google ID %s", path, file_id)
        return

    try:
        with requests.Session() as sess:
            res = sess.get(DOWNLOAD_URL, params={"id": file_id}, stream=True)
            token = _get_confirm_token(res)
            res = sess.get(DOWNLOAD_URL, params={"id": file_id, "confirm": token}, stream=True)
            with path.open("wb") as file:
                for chunk in tqdm(res.iter_content(CHUNK_SIZE), desc="writing", unit="chunk"):
                    if chunk:  # filter out keep-alive new chunks
                        file.write(chunk)
    except (Exception, KeyboardInterrupt):
        if clean_on_failure:
            _unlink(path)
        raise

    raise_on_digest_mismatch(path=path, hexdigests=hexdigests)


def _get_confirm_token(res: requests.Response) -> str:
    for key, value in res.cookies.items():
        if key.startswith(TOKEN_KEY):
            return value
    raise ValueError(f"no token found with key {TOKEN_KEY} in cookies: {res.cookies}")


def download_from_s3(
    s3_bucket: str,
    s3_key: str,
    path: str | Path,
    client: None | botocore.client.BaseClient = None,
    client_kwargs: Mapping[str, Any] | None = None,
    download_file_kwargs: Mapping[str, Any] | None = None,
    force: bool = True,
    clean_on_failure: bool = True,
) -> None:
    """Download a file from S3.

    :param s3_bucket: The key inside the S3 bucket name
    :param s3_key: The key inside the S3 bucket
    :param path: The place to write the file
    :param client: A botocore client. If none given, one will be created automatically
    :param client_kwargs: Keyword arguments to be passed to the client on instantiation.
    :param download_file_kwargs: Keyword arguments to be passed to
        :func:`boto3.s3.transfer.S3Transfer.download_file`
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised
        during download

    :raises Exception: Thrown if an error besides a keyboard interrupt is thrown during
        download
    :raises KeyboardInterrupt: If a keyboard interrupt is thrown during download
    :raises UnexpectedDirectory: If a directory is given for the ``path`` argument
    """
    path = Path(path).resolve()

    if path.is_dir():
        raise UnexpectedDirectoryError(path)
    if path.is_file() and not force:
        logger.debug("did not re-download %s from %s %s", path, s3_bucket, s3_key)
        return

    try:
        import boto3.s3.transfer

        if client is None:
            import boto3
            import botocore.client

            client_kwargs = {} if client_kwargs is None else dict(client_kwargs)
            client_kwargs.setdefault(
                "config", botocore.client.Config(signature_version=botocore.UNSIGNED)
            )
            client = boto3.client("s3", **client_kwargs)

        download_file_kwargs = {} if download_file_kwargs is None else dict(download_file_kwargs)
        download_file_kwargs.setdefault(
            "Config", boto3.s3.transfer.TransferConfig(use_threads=False)
        )
        client.download_file(s3_bucket, s3_key, path.as_posix(), **download_file_kwargs)
    except (Exception, KeyboardInterrupt):
        if clean_on_failure:
            _unlink(path)
        raise


def _unlink(path: str | Path) -> None:
    # python 3.6 does not have pathlib.Path.unlink, smh
    try:
        os.remove(path)
    except OSError:
        pass  # if the file can't be deleted then no problem


def get_name() -> str:
    """Get the PyStow home directory name.

    :returns: The name of the pystow home directory, either loaded from the
        :data:`PYSTOW_NAME_ENVVAR`` environment variable or given by the default value
        :data:`PYSTOW_NAME_DEFAULT`.
    """
    return os.getenv(PYSTOW_NAME_ENVVAR, default=PYSTOW_NAME_DEFAULT)


def use_appdirs() -> bool:
    """Check if X Desktop Group (XDG) compatibility is requested.

    :returns: If the :data:`PYSTOW_USE_APPDIRS` is set to ``true`` in the environment.
    """
    return os.getenv(PYSTOW_USE_APPDIRS) in {"true", "True"}


def get_home(ensure_exists: bool = True) -> Path:
    """Get the PyStow home directory.

    :param ensure_exists: If true, ensures the directory is created

    :returns: A path object representing the pystow home directory, as one of:

        1. :data:`PYSTOW_HOME_ENVVAR` environment variable or
        2. The user data directory defined by :mod:`appdirs` if the
           :data:`PYSTOW_USE_APPDIRS` environment variable is set to ``true`` or
        3. The default directory constructed in the user's home directory plus what's
           returned by :func:`get_name`.
    """
    if use_appdirs():
        from appdirs import user_data_dir

        default = Path(user_data_dir())
    else:
        default = Path.home() / get_name()
    return getenv_path(PYSTOW_HOME_ENVVAR, default, ensure_exists=ensure_exists)


def get_base(key: str, ensure_exists: bool = True) -> Path:
    """Get the base directory for a module.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param ensure_exists: Should all directories be created automatically? Defaults to
        true.

    :returns: The path to the given

    :raises ValueError: if the key is invalid (e.g., has a dot in it)
    """
    if "." in key:
        raise ValueError(f"The module should not have a dot in it: {key}")
    envvar = f"{key.upper()}_HOME"
    if use_appdirs():
        from appdirs import user_data_dir

        default = Path(user_data_dir(appname=key))
    else:
        default = get_home(ensure_exists=False) / key
    return getenv_path(envvar, default, ensure_exists=ensure_exists)


def ensure_readme() -> None:
    """Ensure there's a README in the PyStow data directory.

    :raises PermissionError: If the script calling this function does not have adequate
        permissions to write a file into the PyStow home directory.
    """
    try:
        readme_path = get_home(ensure_exists=True).joinpath("README.md")
    except PermissionError as e:
        raise PermissionError(
            "PyStow was not able to create its home directory in due to a lack of "
            "permissions. This can happen, e.g., if you're working on a server where you don't "
            "have full rights. See https://pystow.readthedocs.io/en/latest/installation.html#"
            "configuration for instructions on choosing a different home folder location for "
            "PyStow to somewhere where you have write permissions."
        ) from e
    if readme_path.is_file():
        return
    with readme_path.open("w", encoding="utf8") as file:
        print(README_TEXT, file=file)


def path_to_sqlite(path: str | Path) -> str:
    """Convert a path to a SQLite connection string.

    :param path: A path to a SQLite database file

    :returns: A standard connection string to the database
    """
    path = Path(path).expanduser().resolve()
    return f"sqlite:///{path.as_posix()}"


def gunzip(source: str | Path, target: str | Path) -> None:
    """Unzip a file in the source to the target.

    :param source: The path to an input file
    :param target: The path to an output file
    """
    with gzip.open(source, "rb") as in_file, open(target, "wb") as out_file:
        shutil.copyfileobj(in_file, out_file)


MODE_MAP: dict[tuple[Operation, Representation], Literal["rt", "wt", "rb", "wb"]] = {
    ("read", "text"): "rt",
    ("read", "binary"): "rb",
    ("write", "text"): "wt",
    ("write", "binary"): "wb",
}


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: str | Path, *, operation: Operation = ..., representation: Literal["text"] = "text"
) -> Generator[typing.TextIO, None, None]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def safe_open(
    path: str | Path, *, operation: Operation = ..., representation: Literal["binary"] = "binary"
) -> Generator[typing.BinaryIO, None, None]: ...


@contextlib.contextmanager
def safe_open(
    path: str | Path, *, operation: Operation = "read", representation: Representation = "text"
) -> Generator[typing.TextIO, None, None] | Generator[typing.BinaryIO, None, None]:
    """Safely open a file for reading or writing text."""
    if operation not in OPERATION_VALUES:
        raise ValueError(
            f"Invalid operation given: {operation}. Should be one of {OPERATION_VALUES}."
        )
    if representation not in REPRESENTATION_VALUES:
        raise ValueError(
            f"Invalid representation given: {representation}. "
            f"Should be one of {REPRESENTATION_VALUES}."
        )

    mode = MODE_MAP[operation, representation]
    path = Path(path).expanduser().resolve()
    if path.suffix.endswith(".gz"):
        with gzip.open(path, mode=mode) as file:
            yield file  # type:ignore
    else:
        with open(path, mode=mode) as file:
            yield file  # type:ignore


@contextlib.contextmanager
def safe_open_writer(
    f: str | Path | TextIO, *, delimiter: str = "\t", **kwargs: Any
) -> Generator[Writer, None, None]:
    """Open a CSV writer, wrapping :func:`csv.writer`.

    :param f: A path to a file, or an already open text-based IO object
    :param delimiter: The delimiter for writing to CSV
    :param kwargs: Keyword arguments to pass to :func:`csv.writer`

    :yields: A CSV writer object, constructed from :func:`csv.writer`
    """
    if isinstance(f, (str, Path)):
        with safe_open(f, operation="write", representation="text") as file:
            yield csv.writer(file, delimiter=delimiter, **kwargs)
    else:
        yield csv.writer(f, delimiter=delimiter, **kwargs)


@contextlib.contextmanager
def safe_open_dict_writer(
    f: str | Path | TextIO,
    fieldnames: typing.Sequence[str],
    *,
    delimiter: str = "\t",
    **kwargs: Any,
) -> Generator[csv.DictWriter[str], None, None]:
    """Open a CSV dictionary writer, wrapping :func:`csv.DictWriter`.

    :param f: A path to a file, or an already open text-based IO object
    :param fieldnames: A path to a file, or an already open text-based IO object
    :param delimiter: The delimiter for writing to CSV
    :param kwargs: Keyword arguments to pass to :func:`csv.DictWriter`

    :yields: A CSV dictionary writer object, constructed from :func:`csv.DictWriter`
    """
    if isinstance(f, (str, Path)):
        with safe_open(f, operation="write", representation="text") as file:
            yield csv.DictWriter(file, fieldnames, delimiter=delimiter, **kwargs)
    else:
        yield csv.DictWriter(f, fieldnames, delimiter=delimiter, **kwargs)


@contextlib.contextmanager
def safe_open_reader(
    f: str | Path | TextIO, *, delimiter: str = "\t", **kwargs: Any
) -> Generator[Reader, None, None]:
    """Open a CSV reader, wrapping :func:`csv.reader`.

    :param f: A path to a file, or an already open text-based IO object
    :param delimiter: The delimiter for writing to CSV
    :param kwargs: Keyword arguments to pass to :func:`csv.reader`

    :yields: A CSV reader object, constructed from :func:`csv.reader`
    """
    if isinstance(f, (str, Path)):
        with safe_open(f, operation="read", representation="text") as file:
            yield csv.reader(file, delimiter=delimiter, **kwargs)
    else:
        yield csv.reader(f, delimiter=delimiter, **kwargs)


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
    if isinstance(f, (str, Path)):
        with safe_open(f, operation="read", representation="text") as file:
            yield csv.DictReader(file, delimiter=delimiter, **kwargs)
    else:
        yield csv.DictReader(f, delimiter=delimiter, **kwargs)


def get_soup(
    url: str,
    *,
    verify: bool = True,
    timeout: TimeoutHint | None = None,
    user_agent: str | None = None,
) -> bs4.BeautifulSoup:
    """Get a beautiful soup parsed version of the given web page.

    :param url: The URL to download and parse with BeautifulSoup
    :param verify: Should SSL be used? This is almost always true,
        except for Ensembl, which makes a big pain
    :param timeout: How many integer seconds to wait for a response?
        Defaults to 15 if none given.
    :param user_agent: A custom user-agent to set, e.g., to avoid anti-crawling mechanisms
    :returns: A BeautifulSoup object
    """
    from bs4 import BeautifulSoup

    headers = {}
    if user_agent:
        headers["User-Agent"] = user_agent
    res = requests.get(url, verify=verify, timeout=timeout or 15, headers=headers)
    soup = BeautifulSoup(res.text, features="html.parser")
    return soup
