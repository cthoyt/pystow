# -*- coding: utf-8 -*-

"""Utilities."""

import contextlib
import gzip
import hashlib
import logging
import os
import shutil
import tarfile
import zipfile
from io import BytesIO, StringIO
from pathlib import Path, PurePosixPath
from subprocess import check_output  # noqa: S404
from typing import Collection, Mapping, Optional, TYPE_CHECKING, Tuple, Union
from urllib.parse import urlparse
from urllib.request import urlretrieve
from uuid import uuid4

import requests
from tqdm import tqdm

if TYPE_CHECKING:
    import rdflib
    import pandas as pd

logger = logging.getLogger(__name__)


class HexDigestError(ValueError):
    """Thrown if the hashsums do not match expected hashsums."""

    def __init__(self, offending_hexdigests: Collection[Tuple[str, str]]):
        self.offending_hexdigests = offending_hexdigests

    def __str__(self):
        return "\n".join((
            "Hexdigest of downloaded file does not match the expected ones!",
            *(
                f"\tactual: {actual} vs. expected: {expected}"
                for actual, expected in self.offending_hexdigests
            )
        ))


def get_offending_hexdigests(
    destination: Path,
    chunk_size: int = 64 * 2 ** 10,
    verbose: bool = True,
    hexdigests: Optional[Mapping[str, str]] = None,
) -> Collection[Tuple[str, str]]:
    """
    Check a file for hash sums.

    :param destination:
        The file path.
    :param chunk_size:
        The chunk size for reading the file.
    :param hexdigests:
        The expected hexdigests as (algorithm_name, expected_hex_digest) pairs.
    :param verbose:
        Whether to be verbose.

    :return:
        A collection of observed / expected hexdigests where the digests do not match.
    """
    if hexdigests is None:
        hexdigests = {}

    if verbose:
        logger.info(f"Checking hash sums for file: {destination.as_uri()}")
    if len(hexdigests) == 0 and verbose:
        logger.warning("There are no hash sums to check for.")
        return []

    # instantiate algorithms
    hash_algorithms = {
        alg: hashlib.new(alg)
        for alg in hexdigests
    }

    # calculate hash sums of file incrementally
    buffer = memoryview(bytearray(chunk_size))
    with destination.open('rb', buffering=0) as f:
        for this_chunk_size in iter(lambda: f.readinto(buffer), 0):
            for alg in hash_algorithms.values():
                alg.update(buffer[:this_chunk_size])

    # Compare digests
    mismatches = []
    for alg, digest in hexdigests.items():
        digest_ = hash_algorithms[alg].hexdigest()
        if digest_ != digest:
            logger.fatal(f"Hashsum does not match! expected {alg}={digest}, but got {digest_}.")
            mismatches.append((digest_, digest))
        elif verbose:
            logger.info(f"Successfully checked with {alg}.")

    return mismatches


def download(
    url: str,
    path: Union[str, Path],
    force: bool = True,
    clean_on_failure: bool = True,
    backend: str = 'urllib',
    hexdigests: Optional[Mapping[str, str]] = None,
    **kwargs,
) -> None:
    """Download a file from a given URL.

    :param url: URL to download
    :param path: Path to download the file to
    :param force: If false and the file already exists, will not re-download.
    :param clean_on_failure: If true, will delete the file on any exception raised during download
    :param backend: The downloader to use. Choose 'urllib' or 'requests'
    :param kwargs: The keyword arguments to pass to :func:`urllib.request.urlretrieve` or to `requests.get`
        depending on the backend chosen. If using 'requests' backend, `stream` is set to True by default.

    :raises Exception: Thrown if an error besides a keyboard interrupt is thrown during download
    :raises KeyboardInterrupt: If a keyboard interrupt is thrown during download
    :raises ValueError: If an invalid backend is chosen
    :raises HexDigestError: If the downloaded file's hexdigest does not match
    """
    # input normalization
    path = Path(path).resolve()

    skip_download = False
    if os.path.exists(path) and not force:
        skip_download = not get_offending_hexdigests(destination=path, hexdigests=hexdigests)
    if skip_download:
        logger.debug('did not re-download %s from %s', path, url)
        return

    try:
        if backend == 'urllib':
            logger.info('downloading from %s to %s', url, path)
            urlretrieve(url, path, **kwargs)  # noqa:S310
        elif backend == 'requests':
            kwargs.setdefault('stream', True)
            # see https://requests.readthedocs.io/en/master/user/quickstart/#raw-response-content
            # pattern from https://stackoverflow.com/a/39217788/5775947
            with requests.get(url, **kwargs) as response, open(path, 'wb') as file:
                logger.info('downloading (stream=%s) from %s to %s', kwargs['stream'], url, path)
                shutil.copyfileobj(response.raw, file)
        else:
            raise ValueError(f'Invalid backend: {backend}. Use "requests" or "urllib".')
    except (Exception, KeyboardInterrupt):
        if clean_on_failure:
            try:
                os.remove(path)
            except FileExistsError:
                pass  # if the file can't be deleted then no problem
        raise

    offending_hexdigests = get_offending_hexdigests(destination=path, hexdigests=hexdigests)
    if offending_hexdigests:
        raise HexDigestError(offending_hexdigests)


def name_from_url(url: str) -> str:
    """Get the filename from the end of the URL."""
    parse_result = urlparse(url)
    path = PurePosixPath(parse_result.path)
    name = path.name
    return name


def name_from_s3_key(key: str) -> str:
    """Get the filename from the S3 key."""
    return key.split('/')[-1]


def mkdir(path: Path, ensure_exists: bool = True) -> None:
    """Make a directory (or parent directory if a file is given) if flagged with ``ensure_exists``."""
    if ensure_exists:
        path.mkdir(exist_ok=True, parents=True)


@contextlib.contextmanager
def mock_envvar(k: str, v: str):
    """Mock the environment variable then delete it after the test is over."""
    os.environ[k] = v
    yield
    del os.environ[k]


def getenv_path(envvar: str, default: Path, ensure_exists: bool = True) -> Path:
    """Get an environment variable representing a path, or use the default."""
    rv = Path(os.getenv(envvar, default=default))
    mkdir(rv, ensure_exists=ensure_exists)
    return rv


def n() -> str:
    """Get a random string for testing."""
    return str(uuid4())


def get_df_io(df: 'pd.DataFrame', sep: str = '\t', index: bool = False, **kwargs) -> BytesIO:
    """Get the dataframe as bytes."""
    sio = StringIO()
    df.to_csv(sio, sep=sep, index=index, **kwargs)
    sio.seek(0)
    bio = BytesIO(sio.read().encode('utf-8'))
    return bio


def write_zipfile_csv(
    df: 'pd.DataFrame',
    path: Union[str, Path],
    inner_path: str, sep='\t',
    index: bool = False,
    **kwargs,
) -> None:
    """Write a dataframe to an inner CSV file to a zip archive."""
    bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    with zipfile.ZipFile(file=path, mode='w') as zip_file:
        with zip_file.open(inner_path, mode='w') as file:
            file.write(bytes_io.read())


def read_zipfile_csv(path: Union[str, Path], inner_path: str, sep='\t', **kwargs) -> 'pd.DataFrame':
    """Read an inner CSV file from a zip archive."""
    import pandas as pd

    with zipfile.ZipFile(file=path) as zip_file:
        with zip_file.open(inner_path) as file:
            return pd.read_csv(file, sep=sep, **kwargs)


def write_tarfile_csv(
    df: 'pd.DataFrame',
    path: Union[str, Path],
    inner_path: str,
    sep='\t',
    index: bool = False,
    **kwargs,
) -> None:
    """Write a dataframe to an inner CSV file from a tar archive."""
    raise NotImplementedError
    # bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    # with tarfile.open(path, mode='w') as tar_file:
    #    with tar_file.open(inner_path, mode='w') as file:  # type: ignore
    #        file.write(bytes_io.read())


def read_tarfile_csv(path: Union[str, Path], inner_path: str, sep='\t', **kwargs) -> 'pd.DataFrame':
    """Read an inner CSV file from a tar archive."""
    import pandas as pd

    with tarfile.open(path) as tar_file:
        with tar_file.extractfile(inner_path) as file:  # type: ignore
            return pd.read_csv(file, sep=sep, **kwargs)


def read_rdf(path: Union[str, Path], **kwargs) -> 'rdflib.Graph':
    """Read an RDF file with :mod:`rdflib`."""
    import rdflib
    if isinstance(path, str):
        path = Path(path)
    graph = rdflib.Graph()
    with (
        gzip.open(path, 'rb')  # type: ignore
        if isinstance(path, Path) and path.suffix == '.gz' else
        open(path)
    ) as file:
        graph.parse(file, **kwargs)
    return graph


def get_commit(org: str, repo: str, provider: str = 'git') -> str:
    """Get last commit hash for the given repo."""
    if provider == 'git':
        output = check_output(['git', 'ls-remote', f'https://github.com/{org}/{repo}'])  # noqa
        lines = (line.strip().split('\t') for line in output.decode('utf8').splitlines())
        rv = next(line[0] for line in lines if line[1] == 'HEAD')
    elif provider == 'github':
        res = requests.get(f'https://api.github.com/repos/{org}/{repo}/branches/master')
        res_json = res.json()
        rv = res_json['commit']['sha']
    else:
        raise NotImplementedError(f'invalid implementation: {provider}')
    return rv


CHUNK_SIZE = 32768
DOWNLOAD_URL = 'https://docs.google.com/uc?export=download'
TOKEN_KEY = 'download_warning'  # noqa:S105


def download_from_google(file_id: str, path: Union[str, os.PathLike]):
    """Download a file from google drive.

    Implementation inspired by https://github.com/ndrplz/google-drive-downloader.

    :param file_id: The google file identifier
    :param path: The place to write the file
    """
    with requests.Session() as sess:
        res = sess.get(DOWNLOAD_URL, params={'id': file_id}, stream=True)
        token = _get_confirm_token(res)
        res = sess.get(DOWNLOAD_URL, params={'id': file_id, 'confirm': token}, stream=True)
        with open(path, 'wb') as file:
            for chunk in tqdm(res.iter_content(CHUNK_SIZE), desc='writing', unit='chunk'):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)


def _get_confirm_token(res: requests.Response) -> str:
    for key, value in res.cookies.items():
        if key.startswith(TOKEN_KEY):
            return value
    raise ValueError(f'no token found with key {TOKEN_KEY} in cookies: {res.cookies}')
