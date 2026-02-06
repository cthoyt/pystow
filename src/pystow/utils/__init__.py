"""Utilities."""

from __future__ import annotations

import contextlib
import csv
import gzip
import io
import logging
import lzma
import pickle
import shutil
import tarfile
import typing
import zipfile
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from io import BytesIO
from pathlib import Path, PurePosixPath
from subprocess import check_output
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Literal,
    Protocol,
    TextIO,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)
from urllib.parse import urlparse
from uuid import uuid4

import requests
from tqdm.auto import tqdm

from .download import (
    DownloadBackend,
    DownloadError,
    UnexpectedDirectoryError,
    download,
    download_from_google,
    download_from_s3,
)
from .env import (
    get_base,
    get_home,
    get_name,
    getenv_path,
    mkdir,
    mock_envvar,
    mock_home,
    use_appdirs,
)
from .hashing import (
    Hash,
    HexDigestError,
    HexDigestMismatch,
    get_hash_hexdigest,
    get_hashes,
    get_hexdigests_remote,
    get_offending_hexdigests,
    raise_on_digest_mismatch,
)
from .io_typing import (
    _MODE_TO_SIMPLE,
    MODE_MAP,
    OPERATION_VALUES,
    REPRESENTATION_VALUES,
    REVERSE_MODE_MAP,
    InvalidOperationError,
    InvalidRepresentationError,
    Operation,
    Reader,
    Representation,
    Writer,
    ensure_sensible_default_encoding,
    ensure_sensible_newline,
    get_mode_pair,
)
from .pydantic_utils import iter_pydantic_jsonl, read_pydantic_jsonl, write_pydantic_jsonl
from .safe_open import open_inner_zipfile, safe_open
from ..constants import README_TEXT, TimeoutHint

if TYPE_CHECKING:
    import bs4
    import lxml.etree
    import numpy.typing
    import pandas
    import rdflib

__all__ = [
    "MODE_MAP",
    "OPERATION_VALUES",
    "REPRESENTATION_VALUES",
    "REVERSE_MODE_MAP",
    "DownloadBackend",
    "DownloadError",
    "Hash",
    "HeaderMismatchError",
    "HexDigestError",
    "HexDigestMismatch",
    "InvalidOperationError",
    "InvalidRepresentationError",
    "Operation",
    "Representation",
    "UnexpectedDirectory",
    "UnexpectedDirectoryError",
    "download",
    "download_from_google",
    "download_from_s3",
    "get_base",
    "get_commit",
    "get_df_io",
    "get_hash_hexdigest",
    "get_hashes",
    "get_hexdigests_remote",
    "get_home",
    "get_mode_pair",
    "get_name",
    "get_np_io",
    "get_offending_hexdigests",
    "get_soup",
    "getenv_path",
    "gunzip",
    "iter_pydantic_jsonl",
    "iter_tarred_csvs",
    "iter_tarred_files",
    "iter_zipped_csvs",
    "iter_zipped_files",
    "mkdir",
    "mock_envvar",
    "mock_home",
    "n",
    "name_from_s3_key",
    "name_from_url",
    "open_inner_zipfile",
    "open_tarfile",
    "open_zip_reader",
    "open_zip_writer",
    "open_zipfile",
    "path_to_sqlite",
    "raise_on_digest_mismatch",
    "read_lzma_csv",
    "read_pydantic_jsonl",
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
    "safe_tarfile_open",
    "safe_zipfile_open",
    "tarfile_writestr",
    "use_appdirs",
    "write_lzma_csv",
    "write_pickle_gz",
    "write_pydantic_jsonl",
    "write_tarfile_csv",
    "write_tarfile_xml",
    "write_zipfile_csv",
    "write_zipfile_np",
    "write_zipfile_rdf",
    "write_zipfile_xml",
]

logger = logging.getLogger(__name__)


#: Backwards compatible name
UnexpectedDirectory = UnexpectedDirectoryError


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
    return io.BytesIO(df.to_csv(sep=sep, index=index, **kwargs).encode("utf-8"))


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
    with safe_open(path, representation="binary", operation="write") as file:
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
    with lzma.open(path, "wb") as file:
        df.to_csv(file, sep=sep, index=index, **kwargs)


def read_lzma_csv(
    path: str | Path,
    sep: str = "\t",
    **kwargs: Any,
) -> pandas.DataFrame:
    """Read a dataframe from a lzma-compressed file.

    :param path: The path to the resulting LZMA compressed dataframe file
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param kwargs: Additional kwargs to pass to :func:`get_df_io` and transitively to
        :func:`pandas.DataFrame.to_csv`.
    """
    import pandas as pd

    with lzma.open(path, "rb") as file:
        return pd.read_csv(file, sep=sep, **kwargs)


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
    with open_zipfile(path, inner_path, operation="write", representation="binary") as file:
        df.to_csv(file, sep=sep, index=index, **kwargs)


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
    representation: Literal["text"] = ...,
    zipfile_kwargs: Mapping[str, Any] | None = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
) -> Generator[typing.TextIO, None, None]: ...


# docstr-coverage:excused `overload`
@typing.overload
@contextlib.contextmanager
def open_zipfile(
    path: str | Path,
    inner_path: str,
    *,
    operation: Operation = ...,
    representation: Literal["binary"] = ...,
    zipfile_kwargs: Mapping[str, Any] | None = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
) -> Generator[typing.BinaryIO, None, None]: ...


@contextlib.contextmanager
def open_zipfile(
    path: str | Path,
    inner_path: str,
    *,
    operation: Operation = "read",
    representation: Representation = "text",
    zipfile_kwargs: Mapping[str, Any] | None = None,
    open_kwargs: Mapping[str, Any] | None = None,
    encoding: str | None = None,
) -> Generator[typing.TextIO, None, None] | Generator[typing.BinaryIO, None, None]:
    """Open a zipfile."""
    mode = _MODE_TO_SIMPLE[operation]
    with (
        zipfile.ZipFile(file=path, mode=mode, **(zipfile_kwargs or {})) as zip_file,
        open_inner_zipfile(
            zip_file,
            inner_path,
            operation=operation,
            representation=representation,
            open_kwargs=open_kwargs,
            encoding=encoding,
        ) as file,
    ):
        yield file


@contextlib.contextmanager
def open_tarfile(
    path: str | Path,
    inner_path: str,
    *,
    operation: Operation = "read",
    representation: Representation = "binary",
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[typing.IO[bytes], None, None]:
    """Open a tar file."""
    if representation != "binary":
        raise NotImplementedError("tarfile must use binary representation")

    if operation == "read":
        with tarfile.open(path, "r", **(open_kwargs or {})) as tar:
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
        raise InvalidOperationError(operation)


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
    with safe_open(f, operation="write", representation="text") as file:
        yield csv.writer(file, delimiter=delimiter, **kwargs)


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
    with safe_open(f, operation="write", representation="text", newline="") as file:
        yield csv.DictWriter(file, fieldnames, delimiter=delimiter, **kwargs)


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
    with safe_open(f, operation="read", representation="text", newline="") as file:
        yield csv.reader(file, delimiter=delimiter, **kwargs)


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


def get_soup(
    url: str,
    *,
    verify: bool = True,
    timeout: TimeoutHint | None = None,
    user_agent: str | None = None,
) -> bs4.BeautifulSoup:
    """Get a beautiful soup parsed version of the given web page.

    :param url: The URL to download and parse with BeautifulSoup
    :param verify: Should SSL be used? This is almost always true, except for Ensembl,
        which makes a big pain
    :param timeout: How many integer seconds to wait for a response? Defaults to 15 if
        none given.
    :param user_agent: A custom user-agent to set, e.g., to avoid anti-crawling
        mechanisms

    :returns: A BeautifulSoup object
    """
    from bs4 import BeautifulSoup

    headers = {}
    if user_agent:
        headers["User-Agent"] = user_agent
    res = requests.get(url, verify=verify, timeout=timeout or 15, headers=headers)
    soup = BeautifulSoup(res.text, features="html.parser")
    return soup


ArchiveType = TypeVar("ArchiveType", contravariant=True)
ArchiveInfo = TypeVar("ArchiveInfo", covariant=True)
Predicate: TypeAlias = Callable[[ArchiveInfo], bool]


class ArchivedFileIterator(Protocol[ArchiveType, ArchiveInfo]):
    """A protocol for opening files in an archive."""

    # docstr-coverage:excused `overload`
    @overload
    def __call__(
        self,
        path: str | Path | ArchiveType,
        *,
        representation: Literal["binary"] = ...,
        progress: bool = ...,
        tqdm_kwargs: Mapping[str, Any] | None = ...,
        keep: Predicate[ArchiveInfo] | None = ...,
        open_kwargs: Mapping[str, Any] | None = ...,
        encoding: str | None = ...,
        newline: str | None = ...,
    ) -> Iterable[BinaryIO]: ...

    # docstr-coverage:excused `overload`
    @overload
    def __call__(
        self,
        path: str | Path | ArchiveType,
        *,
        representation: Literal["text"] = ...,
        progress: bool = ...,
        tqdm_kwargs: Mapping[str, Any] | None = ...,
        keep: Predicate[ArchiveInfo] | None = ...,
        open_kwargs: Mapping[str, Any] | None = ...,
        encoding: str | None = ...,
        newline: str | None = ...,
    ) -> Iterable[TextIO]: ...

    def __call__(
        self,
        path: str | Path | ArchiveType,
        *,
        representation: Representation = ...,
        progress: bool = True,
        tqdm_kwargs: Mapping[str, Any] | None = ...,
        keep: Predicate[ArchiveInfo] | None = ...,
        open_kwargs: Mapping[str, Any] | None = None,
        encoding: str | None = ...,
        newline: str | None = ...,
    ) -> Iterable[TextIO] | Iterable[BinaryIO]: ...


# docstr-coverage:excused `overload`
@overload
def iter_tarred_files(
    path: str | Path | tarfile.TarFile,
    *,
    representation: Literal["binary"] = ...,
    progress: bool = ...,
    tqdm_kwargs: Mapping[str, Any] | None = ...,
    keep: Predicate[tarfile.TarInfo] | None = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Iterable[BinaryIO]: ...


# docstr-coverage:excused `overload`
@overload
def iter_tarred_files(
    path: str | Path | tarfile.TarFile,
    *,
    representation: Literal["text"] = ...,
    progress: bool = ...,
    tqdm_kwargs: Mapping[str, Any] | None = ...,
    keep: Predicate[tarfile.TarInfo] | None = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Iterable[TextIO]: ...


def iter_tarred_files(
    path: str | Path | tarfile.TarFile,
    *,
    representation: Representation = "text",
    progress: bool = True,
    tqdm_kwargs: Mapping[str, Any] | None = None,
    keep: Predicate[tarfile.TarInfo] | None = None,
    open_kwargs: Mapping[str, Any] | None = None,
    encoding: str | None = None,
    newline: str | None = None,
) -> Iterable[TextIO] | Iterable[BinaryIO]:
    """Iterate over opened files in a tar archive in read mode."""
    encoding = ensure_sensible_default_encoding(encoding, representation=representation)
    newline = ensure_sensible_newline(newline, representation=representation)
    with safe_tarfile_open(path) as tar_file:
        _tqdm_kwargs: dict[str, Any] = {
            "unit": "file",
            "unit_scale": True,
        }
        if isinstance(tar_file.name, str | Path):
            _tqdm_kwargs["desc"] = f"reading {Path(tar_file.name).name}"
        if tqdm_kwargs is not None:
            _tqdm_kwargs.update(tqdm_kwargs)
        for member in tqdm(tar_file.getmembers(), disable=not progress, **_tqdm_kwargs):
            if keep is not None and not keep(member):
                continue
            file = tar_file.extractfile(member, **(open_kwargs or {}))
            if file is None:
                continue
            if representation == "text":
                yield io.TextIOWrapper(file, encoding=encoding, newline=newline)
            else:
                yield cast(BinaryIO, file)  # FIXME


@contextlib.contextmanager
def safe_tarfile_open(
    tar_file: str | Path | tarfile.TarFile,
) -> Generator[tarfile.TarFile, None, None]:
    """Open a tar archive safely."""
    if isinstance(tar_file, str | Path):
        with tarfile.open(Path(tar_file).expanduser().resolve(), mode="r") as tar_file:
            yield tar_file
    else:
        yield tar_file


ReturnType: TypeAlias = Literal["sequence", "record"]


# docstr-coverage:excused `overload`
@overload
def iter_tarred_csvs(
    path: str | Path | tarfile.TarFile,
    *,
    progress: bool = ...,
    return_type: Literal["sequence"] = ...,
    max_line_length: int | None = ...,
) -> Iterable[Sequence[str]]: ...


# docstr-coverage:excused `overload`
@overload
def iter_tarred_csvs(
    path: str | Path | tarfile.TarFile,
    *,
    progress: bool = ...,
    return_type: Literal["record"] = ...,
    max_line_length: int | None = ...,
) -> Iterable[dict[str, Any]]: ...


def iter_tarred_csvs(
    path: str | Path | tarfile.TarFile,
    *,
    progress: bool = True,
    return_type: ReturnType = "sequence",
    tqdm_kwargs: Mapping[str, Any] | None = None,
    max_line_length: int | None = None,
    encoding: str | None = None,
) -> Iterable[Sequence[str]] | Iterable[dict[str, Any]]:
    """Iterate over the lines from tarred CSV files."""
    yield from _iter_archived_csvs(
        path,
        progress=progress,
        return_type=return_type,
        iter_files=iter_tarred_files,
        keep=_keep_tar_info_csv,
        tqdm_kwargs=tqdm_kwargs,
        max_line_length=max_line_length,
        encoding=encoding,
    )


def _keep_tar_info_csv(tar_info: tarfile.TarInfo) -> bool:
    return tar_info.name.endswith(".csv")


# docstr-coverage:excused `overload`
@overload
def iter_zipped_files(
    path: str | Path | zipfile.ZipFile,
    *,
    representation: Literal["binary"] = ...,
    progress: bool = ...,
    tqdm_kwargs: Mapping[str, Any] | None = ...,
    keep: Predicate[zipfile.ZipInfo] | None = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Iterable[typing.BinaryIO]: ...


# docstr-coverage:excused `overload`
@overload
def iter_zipped_files(
    path: str | Path | zipfile.ZipFile,
    *,
    representation: Literal["text"] = ...,
    progress: bool = ...,
    tqdm_kwargs: Mapping[str, Any] | None = ...,
    keep: Predicate[zipfile.ZipInfo] | None = ...,
    open_kwargs: Mapping[str, Any] | None = ...,
    encoding: str | None = ...,
    newline: str | None = ...,
) -> Iterable[typing.TextIO]: ...


def iter_zipped_files(
    path: str | Path | zipfile.ZipFile,
    *,
    representation: Representation = "text",
    progress: bool = True,
    tqdm_kwargs: Mapping[str, Any] | None = None,
    keep: Predicate[zipfile.ZipInfo] | None = None,
    open_kwargs: Mapping[str, Any] | None = None,
    encoding: str | None = None,
    newline: str | None = None,
) -> Iterable[typing.TextIO] | Iterable[typing.BinaryIO]:
    """Iterate over opened files in a zip file in read mode."""
    with safe_zipfile_open(path) as zip_file:
        _tqdm_kwargs = {
            "desc": f"reading {zip_file.filename}",
            "unit": "file",
            "unit_scale": True,
        }
        if tqdm_kwargs is not None:
            _tqdm_kwargs.update(tqdm_kwargs)
        for info in tqdm(zip_file.infolist(), disable=not progress, **_tqdm_kwargs):
            if keep is not None and not keep(info):
                continue
            with open_inner_zipfile(
                zip_file,
                info.filename,
                operation="read",
                representation=representation,
                open_kwargs=open_kwargs,
                encoding=encoding,
                newline=newline,
            ) as file:
                yield file


@contextlib.contextmanager
def safe_zipfile_open(
    zip_file: str | Path | zipfile.ZipFile,
) -> Generator[zipfile.ZipFile, None, None]:
    """Open a zip archive safely."""
    if isinstance(zip_file, str | Path):
        with zipfile.ZipFile(Path(zip_file).expanduser().resolve(), mode="r") as zip_file:
            yield zip_file
    else:
        yield zip_file


# docstr-coverage:excused `overload`
@overload
def iter_zipped_csvs(
    path: str | Path | zipfile.ZipFile,
    *,
    progress: bool = ...,
    return_type: Literal["sequence"] = ...,
    tqdm_kwargs: Mapping[str, Any] | None = ...,
    max_line_length: int | None = ...,
) -> Iterable[Sequence[str]]: ...


# docstr-coverage:excused `overload`
@overload
def iter_zipped_csvs(
    path: str | Path | zipfile.ZipFile,
    *,
    progress: bool = ...,
    return_type: Literal["record"] = ...,
    tqdm_kwargs: Mapping[str, Any] | None = ...,
    max_line_length: int | None = ...,
) -> Iterable[dict[str, Any]]: ...


def iter_zipped_csvs(
    path: str | Path | zipfile.ZipFile,
    *,
    progress: bool = True,
    return_type: ReturnType = "sequence",
    tqdm_kwargs: Mapping[str, Any] | None = None,
    max_line_length: int | None = None,
    encoding: str | None = None,
) -> Iterable[Sequence[str]] | Iterable[dict[str, Any]]:
    """Iterate over the lines from zipped CSV files."""
    yield from _iter_archived_csvs(
        path,
        progress=progress,
        return_type=return_type,
        iter_files=iter_zipped_files,
        keep=_keep_zip_info_csv,
        tqdm_kwargs=tqdm_kwargs,
        max_line_length=max_line_length,
        encoding=encoding,
    )


def _keep_zip_info_csv(zip_info: zipfile.ZipInfo) -> bool:
    return zip_info.filename.endswith(".csv")


def _iter_archived_csvs(
    path: str | Path | ArchiveType,
    *,
    progress: bool = True,
    tqdm_kwargs: Mapping[str, Any] | None = None,
    keep: Predicate[ArchiveInfo] | None = None,
    return_type: ReturnType = "sequence",
    iter_files: ArchivedFileIterator[ArchiveType, ArchiveInfo],
    max_line_length: int | None = None,
    encoding: str | None = None,
) -> Iterable[Sequence[str]] | Iterable[dict[str, Any]]:
    """Iterate over the lines from zipped CSV files."""
    header: Sequence[str] | None = None
    for file in iter_files(
        path,
        representation="text",
        progress=progress,
        tqdm_kwargs=tqdm_kwargs,
        keep=keep,
        encoding=encoding,
        newline="",
    ):
        filename = file.name
        if max_line_length is not None:
            # this will break everything if there's an issue in the
            # header, but we aren't going to consider that case
            it = _cut_long_lines(file, max_line_length, filename)
        else:
            it = file

        reader: csv.DictReader[str] | Reader
        match return_type:
            case "sequence":
                reader = csv.reader(it)
            case "record":
                reader = csv.DictReader(it)
            case _:
                raise ValueError(f"unrecognized return type {return_type}")
        if header is None:
            header = _get_header(reader)
        elif (current_header := _get_header(reader)) != header:
            raise HeaderMismatchError(header, current_header)
        rv = tqdm(
            reader,
            disable=not progress,
            leave=False,
            desc=f"reading {filename}",
            unit="row",
            unit_scale=True,
        )
        yield from rv


def _cut_long_lines(it: Iterable[str], max_length: int, name: str) -> Iterable[str]:
    for i, line in enumerate(it):
        if len(line) > max_length:
            tqdm.write(f"[{name}:{i:,}] line of length {len(line):,} is too long: {line[:100]}")
            continue
        yield line


def _get_header(reader: csv.DictReader[str] | Reader) -> Sequence[str]:
    if isinstance(reader, csv.DictReader):
        return cast(Sequence[str], reader.fieldnames)
    else:
        return next(reader)


class HeaderMismatchError(ValueError):
    """Raised when the current header in an archive of CSVs is different than the original."""

    def __init__(self, original: Sequence[str], current: Sequence[str]) -> None:
        """Instantiate the error."""
        self.original = original
        self.current = current

    def __str__(self) -> str:
        return (
            f"header mismatch. first header was {self.original} "
            f"and current header is {self.current}"
        )


def tarfile_writestr(tar_file: tarfile.TarFile, filename: str, data: str) -> None:
    """Write to a tarfile."""
    # TODO later, combine with other tarfile writing
    data_bytes = data.encode("utf-8")
    tar_info = tarfile.TarInfo(name=filename)
    tar_info.size = len(data_bytes)
    tar_file.addfile(tar_info, io.BytesIO(data_bytes))
