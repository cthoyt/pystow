# -*- coding: utf-8 -*-

"""Utilities."""

import contextlib
import gzip
import hashlib
import logging
import lzma
import os
import pickle
import shutil
import tarfile
import tempfile
import zipfile
from collections import namedtuple
from io import BytesIO, StringIO
from pathlib import Path, PurePosixPath
from subprocess import check_output  # noqa: S404
from typing import Any, Collection, Iterable, Iterator, Mapping, Optional, Union
from urllib.parse import urlparse
from urllib.request import urlretrieve
from uuid import uuid4

import requests
from tqdm import tqdm

from .constants import (
    PYSTOW_HOME_ENVVAR,
    PYSTOW_NAME_DEFAULT,
    PYSTOW_NAME_ENVVAR,
    PYSTOW_USE_APPDIRS,
    README_TEXT,
)

__all__ = [
    # Data Structures
    "HexDigestMismatch",
    # Exceptions
    "HexDigestError",
    "UnexpectedDirectory",
    # Functions
    "get_offending_hexdigests",
    "get_hashes",
    "raise_on_digest_mismatch",
    "get_hexdigests_remote",
    "download",
    "name_from_url",
    "name_from_s3_key",
    "mkdir",
    "mock_envvar",
    "mock_home",
    "getenv_path",
    "n",
    # Bytes generators
    "get_df_io",
    "get_np_io",
    # LZMA utilities
    "write_lzma_csv",
    "gunzip",
    # Zipfile utilities
    "write_zipfile_csv",
    "read_zipfile_csv",
    "write_zipfile_np",
    "read_zip_np",
    "read_zipfile_rdf",
    # Tarfile utilities
    "write_tarfile_csv",
    "read_tarfile_csv",
    "read_tarfile_xml",
    # GZ utilities
    "write_pickle_gz",
    # Standard readers
    "read_rdf",
    # Downloaders
    "download_from_google",
    "download_from_s3",
    # Misc
    "get_commit",
    "get_home",
    "get_name",
    "get_base",
    "path_to_sqlite",
]

logger = logging.getLogger(__name__)

# Since we're python 3.6 compatible, we can't do from __future__ import annotations and use hashlib._Hash
Hash = Any

HexDigestMismatch = namedtuple("HexDigestMismatch", "name actual expected")


class HexDigestError(ValueError):
    """Thrown if the hashsums do not match expected hashsums."""

    def __init__(self, offending_hexdigests: Collection[HexDigestMismatch]):
        """Instantiate the exception.

        :param offending_hexdigests: The result from :func:`get_offending_hexdigests`
        """
        self.offending_hexdigests = offending_hexdigests

    def __str__(self):  # noqa:D105
        return "\n".join(
            (
                "Hexdigest of downloaded file does not match the expected ones!",
                *(
                    f"\t{name} actual: {actual} vs. expected: {expected}"
                    for name, actual, expected in self.offending_hexdigests
                ),
            )
        )


class UnexpectedDirectory(FileExistsError):
    """Thrown if a directory path is given where file path should have been."""

    def __init__(self, path: Path):
        """Instantiate the exception.

        :param path: The path to a directory that should have been a file.
        """
        self.path = path

    def __str__(self) -> str:  # noqa:D105
        return f"got directory instead of file: {self.path}"


def get_hexdigests_remote(
    hexdigests_remote: Optional[Mapping[str, str]], hexdigests_strict: bool = False
) -> Mapping[str, str]:
    """Process hexdigests via URLs.

    :param hexdigests_remote:
        The expected hexdigests as (algorithm_name, url to file with expected hex digest) pairs.
    :param hexdigests_strict:
        Set this to false to stop automatically checking for the `algorithm(filename)=hash` format
    :returns:
        A mapping of algorithms to hexdigests
    """
    rv = {}
    for key, url in (hexdigests_remote or {}).items():
        text = requests.get(url).text
        if not hexdigests_strict and "=" in text:
            text = text.rsplit("=", 1)[-1].strip()
        rv[key] = text
    return rv


def get_offending_hexdigests(
    path: Union[str, Path],
    chunk_size: Optional[int] = None,
    hexdigests: Optional[Mapping[str, str]] = None,
    hexdigests_remote: Optional[Mapping[str, str]] = None,
    hexdigests_strict: bool = False,
) -> Collection[HexDigestMismatch]:
    """
    Check a file for hash sums.

    :param path:
        The file path.
    :param chunk_size:
        The chunk size for reading the file.
    :param hexdigests:
        The expected hexdigests as (algorithm_name, expected_hex_digest) pairs.
    :param hexdigests_remote:
        The expected hexdigests as (algorithm_name, url to file with expected hexdigest) pairs.
    :param hexdigests_strict:
        Set this to false to stop automatically checking for the `algorithm(filename)=hash` format

    :return:
        A collection of observed / expected hexdigests where the digests do not match.
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
    path: Union[str, Path],
    names: Iterable[str],
    *,
    chunk_size: Optional[int] = None,
) -> Mapping[str, Hash]:
    """Calculate several hexdigests of hash algorithms for a file concurrently.

    :param path: The file path.
    :param names: Names of the hash algorithms in :mod:`hashlib`
    :param chunk_size: The chunk size for reading the file.

    :return:
        A collection of observed hexdigests
    """
    path = Path(path).resolve()
    if chunk_size is None:
        chunk_size = 64 * 2**10

    # instantiate hash algorithms
    algorithms: Mapping[str, Hash] = {name: hashlib.new(name) for name in names}

    # calculate hash sums of file incrementally
    buffer = memoryview(bytearray(chunk_size))
    with path.open("rb", buffering=0) as file:
        for this_chunk_size in iter(lambda: file.readinto(buffer), 0):  # type: ignore
            for alg in algorithms.values():
                alg.update(buffer[:this_chunk_size])

    return algorithms


def raise_on_digest_mismatch(
    *,
    path: Path,
    hexdigests: Optional[Mapping[str, str]] = None,
    hexdigests_remote: Optional[Mapping[str, str]] = None,
    hexdigests_strict: bool = False,
) -> None:
    """Raise a HexDigestError if the digests do not match.

    :param path:
        The file path.
    :param hexdigests:
        The expected hexdigests as (algorithm_name, expected_hex_digest) pairs.
    :param hexdigests_remote:
        The expected hexdigests as (algorithm_name, url to file with expected hexdigest) pairs.
    :param hexdigests_strict:
        Set this to false to stop automatically checking for the `algorithm(filename)=hash` format

    :raises HexDigestError: if there are any offending hex digests
        The expected hexdigests as (algorithm_name, url to file with expected hexdigest) pairs.
    """
    offending_hexdigests = get_offending_hexdigests(
        path=path,
        hexdigests=hexdigests,
        hexdigests_remote=hexdigests_remote,
        hexdigests_strict=hexdigests_strict,
    )
    if offending_hexdigests:
        raise HexDigestError(offending_hexdigests)


def download(
    url: str,
    path: Union[str, Path],
    force: bool = True,
    clean_on_failure: bool = True,
    backend: str = "urllib",
    hexdigests: Optional[Mapping[str, str]] = None,
    hexdigests_remote: Optional[Mapping[str, str]] = None,
    hexdigests_strict: bool = False,
    **kwargs: Any,
) -> None:
    """Download a file from a given URL.

    :param url: URL to download
    :param path: Path to download the file to
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised during download
    :param backend: The downloader to use. Choose 'urllib' or 'requests'
    :param hexdigests:
        The expected hexdigests as (algorithm_name, expected_hex_digest) pairs.
    :param hexdigests_remote:
        The expected hexdigests as (algorithm_name, url to file with expected hexdigest) pairs.
    :param hexdigests_strict:
        Set this to false to stop automatically checking for the `algorithm(filename)=hash` format
    :param kwargs: The keyword arguments to pass to :func:`urllib.request.urlretrieve` or to `requests.get`
        depending on the backend chosen. If using 'requests' backend, `stream` is set to True by default.

    :raises Exception: Thrown if an error besides a keyboard interrupt is thrown during download
    :raises KeyboardInterrupt: If a keyboard interrupt is thrown during download
    :raises UnexpectedDirectory: If a directory is given for the ``path`` argument
    :raises ValueError: If an invalid backend is chosen
    """
    path = Path(path).resolve()

    if path.is_dir():
        raise UnexpectedDirectory(path)
    if path.is_file() and not force:
        raise_on_digest_mismatch(
            path=path,
            hexdigests=hexdigests,
            hexdigests_remote=hexdigests_remote,
            hexdigests_strict=hexdigests_strict,
        )
        logger.debug("did not re-download %s from %s", path, url)
        return

    try:
        if backend == "urllib":
            logger.info("downloading with urllib from %s to %s", url, path)
            urlretrieve(url, path, **kwargs)  # noqa:S310
        elif backend == "requests":
            kwargs.setdefault("stream", True)
            # see https://requests.readthedocs.io/en/master/user/quickstart/#raw-response-content
            # pattern from https://stackoverflow.com/a/39217788/5775947
            with requests.get(url, **kwargs) as response, path.open("wb") as file:
                logger.info(
                    "downloading (stream=%s) with requests from %s to %s",
                    kwargs["stream"],
                    url,
                    path,
                )
                shutil.copyfileobj(response.raw, file)
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


def name_from_url(url: str) -> str:
    """Get the filename from the end of the URL.

    :param url: A URL
    :return: The name of the file at the end of the URL
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
    :param ensure_exists: Should the directories leading to the path be created if they don't already exist?
    """
    if ensure_exists:
        path.mkdir(exist_ok=True, parents=True)


@contextlib.contextmanager
def mock_envvar(envvar: str, value: str) -> Iterator[None]:
    """Mock the environment variable then delete it after the test is over.

    :param envvar: The environment variable to mock
    :param value: The value to temporarily put in the environment variable
        during this mock.
    :yield: None, since this just mocks the environment variable for the
        time being.
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
    :param ensure_exists: Should the directories leading to the path be created if they don't already exist?
    :return: A path either specified by the environmental variable or by the default.
    """
    rv = Path(os.getenv(envvar, default=default))
    mkdir(rv, ensure_exists=ensure_exists)
    return rv


def n() -> str:
    """Get a random string for testing.

    :returns: A random string for testing purposes.
    """
    return str(uuid4())


def get_df_io(df, sep: str = "\t", index: bool = False, **kwargs) -> BytesIO:
    """Get the dataframe as bytes.

    :param df: A dataframe
    :type df: pandas.DataFrame
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index:  Should the index be output? Overrides the Pandas default to be false.
    :param kwargs: Additional kwargs to pass to :func:`pandas.DataFrame.to_csv`.
    :return: A bytes object that can be used as a file.
    """
    sio = StringIO()
    df.to_csv(sio, sep=sep, index=index, **kwargs)
    sio.seek(0)
    bio = BytesIO(sio.read().encode("utf-8"))
    return bio


def get_np_io(arr, **kwargs) -> BytesIO:
    """Get the numpy object as bytes.

    :param arr: Array-like
    :param kwargs: Additional kwargs to pass to :func:`numpy.save`.
    :return: A bytes object that can be used as a file.
    """
    import numpy as np

    bio = BytesIO()
    np.save(bio, arr, **kwargs)
    bio.seek(0)
    return bio


def write_pickle_gz(
    obj,
    path: Union[str, Path],
    **kwargs,
) -> None:
    """Write an object to a gzipped pickle.

    :param obj: The object to write
    :param path: The path of the file to write to
    :param kwargs:
        Additional kwargs to pass to :func:`pickle.dump`
    """
    with gzip.open(path, mode="wb") as file:
        pickle.dump(obj, file, **kwargs)


def write_lzma_csv(
    df,
    path: Union[str, Path],
    sep="\t",
    index: bool = False,
    **kwargs,
):
    """Write a dataframe as an lzma-compressed file.

    :param df: A dataframe
    :type df: pandas.DataFrame
    :param path: The path to the resulting LZMA compressed dataframe file
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index:  Should the index be output? Overrides the Pandas default to be false.
    :param kwargs:
        Additional kwargs to pass to :func:`get_df_io` and transitively
        to :func:`pandas.DataFrame.to_csv`.
    """
    bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    with lzma.open(path, "wb") as file:
        file.write(bytes_io.read())


def write_zipfile_csv(
    df,
    path: Union[str, Path],
    inner_path: str,
    sep="\t",
    index: bool = False,
    **kwargs,
) -> None:
    """Write a dataframe to an inner CSV file to a zip archive.

    :param df: A dataframe
    :type df: pandas.DataFrame
    :param path: The path to the resulting zip archive
    :param inner_path: The path inside the zip archive to write the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index:  Should the index be output? Overrides the Pandas default to be false.
    :param kwargs:
        Additional kwargs to pass to :func:`get_df_io` and transitively
        to :func:`pandas.DataFrame.to_csv`.
    """
    bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    with zipfile.ZipFile(file=path, mode="w") as zip_file:
        with zip_file.open(inner_path, mode="w") as file:
            file.write(bytes_io.read())


def read_zipfile_csv(path: Union[str, Path], inner_path: str, sep: str = "\t", **kwargs):
    """Read an inner CSV file from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param kwargs: Additional kwargs to pass to :func:`pandas.read_csv`.
    :return: A dataframe
    :rtype: pandas.DataFrame
    """
    import pandas as pd

    with zipfile.ZipFile(file=path) as zip_file:
        with zip_file.open(inner_path) as file:
            return pd.read_csv(file, sep=sep, **kwargs)


def write_zipfile_np(
    arr,
    path: Union[str, Path],
    inner_path: str,
    **kwargs,
) -> None:
    """Write a dataframe to an inner CSV file to a zip archive.

    :param arr: Array-like
    :param path: The path to the resulting zip archive
    :param inner_path: The path inside the zip archive to write the dataframe
    :param kwargs:
        Additional kwargs to pass to :func:`get_np_io` and transitively
        to :func:`numpy.save`.
    """
    bytes_io = get_np_io(arr, **kwargs)
    with zipfile.ZipFile(file=path, mode="w") as zip_file:
        with zip_file.open(inner_path, mode="w") as file:
            file.write(bytes_io.read())


def read_zip_np(path: Union[str, Path], inner_path: str, **kwargs):
    """Read an inner numpy array-like from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the dataframe
    :param kwargs: Additional kwargs to pass to :func:`numpy.load`.
    :return: A numpy array or other object
    :rtype: numpy.typing.ArrayLike
    """
    import numpy as np

    with zipfile.ZipFile(file=path) as zip_file:
        with zip_file.open(inner_path) as file:
            return np.load(file, **kwargs)


def read_zipfile_rdf(path: Union[str, Path], inner_path: str, **kwargs):
    """Read an inner RDF file from a zip archive.

    :param path: The path to the zip archive
    :param inner_path: The path inside the zip archive to the dataframe
    :param kwargs: Additional kwargs to pass to :func:`pandas.read_csv`.
    :return: A dataframe
    :rtype: rdflib.Graph
    """
    import rdflib

    graph = rdflib.Graph()
    with zipfile.ZipFile(file=path) as zip_file:
        with zip_file.open(inner_path) as file:
            graph.load(file, **kwargs)
    return graph


def write_tarfile_csv(
    df,
    path: Union[str, Path],
    inner_path: str,
    sep: str = "\t",
    index: bool = False,
    **kwargs,
) -> None:
    """Write a dataframe to an inner CSV file from a tar archive.

    :param df: A dataframe
    :type df: pandas.DataFrame
    :param path: The path to the resulting tar archive
    :param inner_path: The path inside the tar archive to write the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param index:  Should the index be output? Overrides the Pandas default to be false.
    :param kwargs:
        Additional kwargs to pass to :func:`get_df_io` and transitively
        to :func:`pandas.DataFrame.to_csv`.
    """
    s = df.to_csv(sep=sep, index=index, **kwargs)
    tarinfo = tarfile.TarInfo(name=inner_path)
    tarinfo.size = len(s)
    with tarfile.TarFile(path, mode="w") as tar_file:
        tar_file.addfile(tarinfo, BytesIO(s.encode("utf-8")))


def read_tarfile_csv(path: Union[str, Path], inner_path: str, sep: str = "\t", **kwargs):
    """Read an inner CSV file from a tar archive.

    :param path: The path to the tar archive
    :param inner_path: The path inside the tar archive to the dataframe
    :param sep: The separator in the dataframe. Overrides Pandas default to use a tab.
    :param kwargs: Additional kwargs to pass to :func:`pandas.read_csv`.
    :return: A dataframe
    :rtype: pandas.DataFrame
    """
    import pandas as pd

    with tarfile.open(path) as tar_file:
        with tar_file.extractfile(inner_path) as file:  # type: ignore
            return pd.read_csv(file, sep=sep, **kwargs)


def read_tarfile_xml(path: Union[str, Path], inner_path: str, **kwargs):
    """Read an inner XML file from a tar archive.

    :param path: The path to the tar archive
    :param inner_path: The path inside the tar archive to the xml file
    :param kwargs: Additional kwargs to pass to :func:`lxml.etree.parse`
    :return: An XML element tree
    :rtype: lxml.etree.ElementTree
    """
    from lxml import etree

    with tarfile.open(path) as tar_file:
        with tar_file.extractfile(inner_path) as file:  # type: ignore
            return etree.parse(file, **kwargs)


def read_rdf(path: Union[str, Path], **kwargs):
    """Read an RDF file with :mod:`rdflib`.

    :param path: The path to the RDF file
    :param kwargs: Additional kwargs to pass to :func:`rdflib.Graph.parse`
    :return: A parsed RDF graph
    :rtype: rdflib.Graph
    """
    import rdflib

    if isinstance(path, str):
        path = Path(path)
    graph = rdflib.Graph()
    with (
        gzip.open(path, "rb")  # type: ignore
        if isinstance(path, Path) and path.suffix == ".gz"
        else open(path)
    ) as file:
        graph.parse(file, **kwargs)
    return graph


def write_sql(df, name: str, path: Union[str, Path], **kwargs) -> None:
    """Write a dataframe as a SQL table.

    :param df: A dataframe
    :type df: pandas.DataFrame
    :param name: The table the database to write to
    :param path: The path to the resulting tar archive
    :param kwargs: Additional keyword arguments to pass to :meth:`pandas.DataFrame.to_sql`
    """
    import sqlite3

    with contextlib.closing(sqlite3.connect(path)) as conn:
        df.to_sql(name, conn, **kwargs)


def get_commit(org: str, repo: str, provider: str = "git") -> str:
    """Get last commit hash for the given repo.

    :param org: The GitHub organization or owner
    :param repo: The GitHub repository name
    :param provider: The method for getting the most recent commit
    :raises ValueError: if an invalid provider is given
    :returns: A commit hash's hex digest as a string
    """
    if provider == "git":
        output = check_output(["git", "ls-remote", f"https://github.com/{org}/{repo}"])  # noqa
        lines = (line.strip().split("\t") for line in output.decode("utf8").splitlines())
        rv = next(line[0] for line in lines if line[1] == "HEAD")
    elif provider == "github":
        res = requests.get(f"https://api.github.com/repos/{org}/{repo}/branches/master")
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
    path: Union[str, Path],
    force: bool = True,
    clean_on_failure: bool = True,
    hexdigests: Optional[Mapping[str, str]] = None,
) -> None:
    """Download a file from google drive.

    Implementation inspired by https://github.com/ndrplz/google-drive-downloader.

    :param file_id: The google file identifier
    :param path: The place to write the file
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised during download
    :param hexdigests:
        The expected hexdigests as (algorithm_name, expected_hex_digest) pairs.

    :raises Exception: Thrown if an error besides a keyboard interrupt is thrown during download
    :raises KeyboardInterrupt: If a keyboard interrupt is thrown during download
    :raises UnexpectedDirectory: If a directory is given for the ``path`` argument
    """
    path = Path(path).resolve()

    if path.is_dir():
        raise UnexpectedDirectory(path)
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
    path: Union[str, Path],
    client=None,
    client_kwargs: Optional[Mapping[str, Any]] = None,
    download_file_kwargs: Optional[Mapping[str, Any]] = None,
    force: bool = True,
    clean_on_failure: bool = True,
) -> None:
    """Download a file from S3.

    :param s3_bucket: The key inside the S3 bucket name
    :param s3_key: The key inside the S3 bucket
    :param path: The place to write the file
    :param client:
        A botocore client. If none given, one will be created automatically
    :type client: Optional[botocore.client.BaseClient]
    :param client_kwargs:
        Keyword arguments to be passed to the client on instantiation.
    :param download_file_kwargs:
        Keyword arguments to be passed to :func:`boto3.s3.transfer.S3Transfer.download_file`
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised during download

    :raises Exception: Thrown if an error besides a keyboard interrupt is thrown during download
    :raises KeyboardInterrupt: If a keyboard interrupt is thrown during download
    :raises UnexpectedDirectory: If a directory is given for the ``path`` argument
    """
    path = Path(path).resolve()

    if path.is_dir():
        raise UnexpectedDirectory(path)
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


def _unlink(path: Union[str, Path]) -> None:
    # python 3.6 does not have pathlib.Path.unlink, smh
    try:
        os.remove(path)
    except OSError:
        pass  # if the file can't be deleted then no problem


def get_name() -> str:
    """Get the PyStow home directory name.

    :returns: The name of the pystow home directory, either loaded from
        the :data:`PYSTOW_NAME_ENVVAR`` environment variable or given by the default
        value :data:`PYSTOW_NAME_DEFAULT`.
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
        2. The user data directory defined by :mod:`appdirs` if the :data:`PYSTOW_USE_APPDIRS`
           environment variable is set to ``true`` or
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

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param ensure_exists:
        Should all directories be created automatically?
        Defaults to true.
    :returns:
        The path to the given

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
    """Ensure there's a README in the PyStow data directory."""
    readme_path = get_home(ensure_exists=True).joinpath("README.md")
    if readme_path.is_file():
        return
    with readme_path.open("w", encoding="utf8") as file:
        print(README_TEXT, file=file)  # noqa:T001,T201


def path_to_sqlite(path: Union[str, Path]) -> str:
    """Convert a path to a SQLite connection string.

    :param path: A path to a SQLite database file
    :returns: A standard connection string to the database
    """
    path = Path(path).expanduser().resolve()
    return f"sqlite:///{path.as_posix()}"


def gunzip(source: Union[str, Path], target: Union[str, Path]) -> None:
    """Unzip a file in the source to the target.

    :param source: The path to an input file
    :param target: The path to an output file
    """
    with gzip.open(source, "rb") as in_file, open(target, "wb") as out_file:
        shutil.copyfileobj(in_file, out_file)
