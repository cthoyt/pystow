# -*- coding: utf-8 -*-

"""Utilities."""

import contextlib
import gzip
import hashlib
import logging
import lzma
import os
import shutil
import tarfile
import tempfile
import zipfile
from collections import namedtuple
from io import BytesIO, StringIO
from pathlib import Path, PurePosixPath
from subprocess import check_output  # noqa: S404
from typing import Any, Collection, Iterable, Mapping, Optional, Union
from urllib.parse import urlparse
from urllib.request import urlretrieve
from uuid import uuid4

import requests
from tqdm import tqdm

from .constants import PYSTOW_HOME_ENVVAR

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
    "download",
    "name_from_url",
    "name_from_s3_key",
    "mkdir",
    "mock_envvar",
    "mock_home",
    "getenv_path",
    "n",
    "get_df_io",
    "write_lzma_csv",
    "write_zipfile_csv",
    "read_zipfile_csv",
    "read_zipfile_rdf",
    "write_tarfile_csv",
    "read_tarfile_csv",
    "read_tarfile_xml",
    "read_rdf",
    "get_commit",
    "download_from_google",
    "download_from_s3",
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

    def __str__(self):  # noqa:D105
        return f"got directory instead of file: {self.path}"


def get_offending_hexdigests(
    path: Union[str, Path],
    chunk_size: Optional[int] = None,
    hexdigests: Optional[Mapping[str, str]] = None,
) -> Collection[HexDigestMismatch]:
    """
    Check a file for hash sums.

    :param path:
        The file path.
    :param chunk_size:
        The chunk size for reading the file.
    :param hexdigests:
        The expected hexdigests as (algorithm_name, expected_hex_digest) pairs.

    :return:
        A collection of observed / expected hexdigests where the digests do not match.
    """
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
        chunk_size = 64 * 2 ** 10

    # instantiate hash algorithms
    algorithms: Mapping[str, Hash] = {name: hashlib.new(name) for name in names}

    # calculate hash sums of file incrementally
    buffer = memoryview(bytearray(chunk_size))
    with path.open("rb", buffering=0) as file:
        for this_chunk_size in iter(lambda: file.readinto(buffer), 0):  # type: ignore
            for alg in algorithms.values():
                alg.update(buffer[:this_chunk_size])

    return algorithms


def raise_on_digest_mismatch(*, path: Path, hexdigests: Optional[Mapping[str, str]] = None) -> None:
    """Raise a HexDigestError if the digests do not match."""
    offending_hexdigests = get_offending_hexdigests(path=path, hexdigests=hexdigests)
    if offending_hexdigests:
        raise HexDigestError(offending_hexdigests)


def download(
    url: str,
    path: Union[str, Path],
    force: bool = True,
    clean_on_failure: bool = True,
    backend: str = "urllib",
    hexdigests: Optional[Mapping[str, str]] = None,
    **kwargs,
) -> None:
    """Download a file from a given URL.

    :param url: URL to download
    :param path: Path to download the file to
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised during download
    :param backend: The downloader to use. Choose 'urllib' or 'requests'
    :param hexdigests:
        The expected hexdigests as (algorithm_name, expected_hex_digest) pairs.
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
        raise_on_digest_mismatch(path=path, hexdigests=hexdigests)
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

    raise_on_digest_mismatch(path=path, hexdigests=hexdigests)


def name_from_url(url: str) -> str:
    """Get the filename from the end of the URL."""
    parse_result = urlparse(url)
    path = PurePosixPath(parse_result.path)
    name = path.name
    return name


def name_from_s3_key(key: str) -> str:
    """Get the filename from the S3 key."""
    return key.split("/")[-1]


def mkdir(path: Path, ensure_exists: bool = True) -> None:
    """Make a directory (or parent directory if a file is given) if flagged with ``ensure_exists``."""
    if ensure_exists:
        path.mkdir(exist_ok=True, parents=True)


@contextlib.contextmanager
def mock_envvar(envvar: str, value: str):
    """Mock the environment variable then delete it after the test is over."""
    os.environ[envvar] = value
    yield
    del os.environ[envvar]


@contextlib.contextmanager
def mock_home():
    """Mock the PyStow home environment variable, yields the directory name."""
    with tempfile.TemporaryDirectory() as directory:
        with mock_envvar(PYSTOW_HOME_ENVVAR, directory):
            yield directory


def getenv_path(envvar: str, default: Path, ensure_exists: bool = True) -> Path:
    """Get an environment variable representing a path, or use the default."""
    rv = Path(os.getenv(envvar, default=default))
    mkdir(rv, ensure_exists=ensure_exists)
    return rv


def n() -> str:
    """Get a random string for testing."""
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


def get_commit(org: str, repo: str, provider: str = "git") -> str:
    """Get last commit hash for the given repo."""
    if provider == "git":
        output = check_output(["git", "ls-remote", f"https://github.com/{org}/{repo}"])  # noqa
        lines = (line.strip().split("\t") for line in output.decode("utf8").splitlines())
        rv = next(line[0] for line in lines if line[1] == "HEAD")
    elif provider == "github":
        res = requests.get(f"https://api.github.com/repos/{org}/{repo}/branches/master")
        res_json = res.json()
        rv = res_json["commit"]["sha"]
    else:
        raise NotImplementedError(f"invalid implementation: {provider}")
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
