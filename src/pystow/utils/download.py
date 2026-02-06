"""Download utilities."""

from __future__ import annotations

import logging
import shutil
import urllib.error
from collections.abc import Mapping
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeAlias
from urllib.request import urlretrieve

import requests
from tqdm import tqdm

from .hashing import raise_on_digest_mismatch

if TYPE_CHECKING:
    import botocore.client

__all__ = [
    "DownloadBackend",
    "DownloadError",
    "UnexpectedDirectoryError",
    "download",
    "download_from_google",
    "download_from_s3",
]

logger = logging.getLogger(__file__)

#: Represents an available backend for downloading
DownloadBackend: TypeAlias = Literal["urllib", "requests"]


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
            path.unlink(missing_ok=True)
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


class UnexpectedDirectoryError(FileExistsError):
    """Thrown if a directory path is given where file path should have been."""

    def __init__(self, path: Path):
        """Instantiate the exception.

        :param path: The path to a directory that should have been a file.
        """
        self.path = path

    def __str__(self) -> str:
        return f"got directory instead of file: {self.path}"


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
            path.unlink(missing_ok=True)
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
            path.unlink(missing_ok=True)
        raise
