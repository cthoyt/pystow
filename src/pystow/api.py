"""API functions for PyStow."""

from __future__ import annotations

import bz2
import io
import lzma
import sqlite3
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from functools import lru_cache
from io import BytesIO, StringIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    overload,
)

from .constants import JSON, BytesOpener, Provider
from .impl import Module, VersionHint

if TYPE_CHECKING:
    import lxml.etree
    import numpy.typing
    import pandas as pd
    import rdflib

__all__ = [
    "dump_df",
    "dump_json",
    "dump_pickle",
    "dump_rdf",
    "dump_xml",
    "ensure",
    "ensure_csv",
    "ensure_custom",
    "ensure_excel",
    "ensure_from_google",
    "ensure_from_s3",
    "ensure_gunzip",
    "ensure_json",
    "ensure_json_bz2",
    "ensure_nltk",
    "ensure_open",
    "ensure_open_bz2",
    "ensure_open_gz",
    "ensure_open_lzma",
    "ensure_open_sqlite",
    "ensure_open_sqlite_gz",
    "ensure_open_tarfile",
    "ensure_open_zip",
    "ensure_pickle",
    "ensure_pickle_gz",
    "ensure_rdf",
    "ensure_tar_df",
    "ensure_tar_xml",
    "ensure_untar",
    "ensure_xml",
    "ensure_zip_df",
    "ensure_zip_np",
    "join",
    "joinpath_sqlite",
    "load_df",
    "load_json",
    "load_pickle",
    "load_pickle_gz",
    "load_rdf",
    "load_xml",
    "module",
    "open",
    "open_gz",
]


def module(key: str, *subkeys: str, ensure_exists: bool = True) -> Module:
    """Return a module for the application.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param ensure_exists: Should all directories be created automatically? Defaults to
        true.

    :returns: The module object that manages getting and ensuring
    """
    return Module.from_key(key, *subkeys, ensure_exists=ensure_exists)


def join(
    key: str,
    *subkeys: str,
    name: str | None = None,
    ensure_exists: bool = True,
    version: VersionHint = None,
) -> Path:
    """Return the home data directory for the given module.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join
    :param name: The name of the file (optional) inside the folder
    :param ensure_exists: Should all directories be created automatically? Defaults to
        true.
    :param version: The optional version, or no-argument callable that returns an
        optional version. This is prepended before the subkeys.

        The following example describes how to store the versioned data from the Rhea
        database for biologically relevant chemical reactions.

        .. code-block::

            import pystow
            import requests

            def get_rhea_version() -> str:
                res = requests.get("https://ftp.expasy.org/databases/rhea/rhea-release.properties")
                _, _, version = res.text.splitlines()[0].partition("=")
                return version

            # Assume you want to download the data from
            # ftp://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz, make a path
            # with the same name
            path = pystow.join("rhea", name="rhea.rdf.gz", version=get_rhea_version)


    :returns: The path of the directory or subdirectory for the given module.
    """
    _module = Module.from_key(key, ensure_exists=ensure_exists)
    return _module.join(*subkeys, name=name, ensure_exists=ensure_exists, version=version)


# docstr-coverage:excused `overload`
@overload
@contextmanager
def open(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["r", "rt", "w", "wt"] = "r",
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[StringIO, None, None]: ...


# docstr-coverage:excused `overload`
@overload
@contextmanager
def open(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["rb", "wb"],
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[BytesIO, None, None]: ...


@contextmanager
def open(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["r", "rb", "rt", "w", "wb", "wt"] = "r",
    open_kwargs: Mapping[str, Any] | None = None,
    ensure_exists: bool = False,
) -> Generator[StringIO | BytesIO, None, None]:
    """Open a file.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param mode: The read or write mode, passed to :func:`open`
    :param open_kwargs: Additional keyword arguments passed to :func:`open`
    :param ensure_exists: Should the directory the file is in be made? Set to true on
        write operations.

    :yields: An open file object

    This function should be called inside a context manager like in the following

    .. code-block:: python

        import pystow

        with pystow.open("test", name="test.tsv", mode="w") as file:
            print("Test text!", file=file)
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.open(
        *subkeys, name=name, mode=mode, open_kwargs=open_kwargs, ensure_exists=ensure_exists
    ) as file:
        yield file


# docstr-coverage:excused `overload`
@overload
@contextmanager
def open_gz(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["r", "w", "rt", "wt"] = ...,
    open_kwargs: Mapping[str, Any] | None,
) -> Generator[StringIO, None, None]: ...


# docstr-coverage:excused `overload`
@overload
@contextmanager
def open_gz(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["rb", "wb"] = ...,
    open_kwargs: Mapping[str, Any] | None,
) -> Generator[BytesIO, None, None]: ...


@contextmanager
def open_gz(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["r", "w", "rt", "wt", "rb", "wb"] = "rb",
    open_kwargs: Mapping[str, Any] | None = None,
    ensure_exists: bool = False,
) -> Generator[StringIO | BytesIO, None, None]:
    """Open a gzipped file that exists already.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param mode: The read mode, passed to :func:`gzip.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`
    :param ensure_exists: Should the file be made? Set to true on write operations.

    :yields: An open file object
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.open_gz(
        *subkeys, name=name, mode=mode, open_kwargs=open_kwargs, ensure_exists=ensure_exists
    ) as file:
        yield file


def ensure(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    version: VersionHint = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
) -> Path:
    """Ensure a file is downloaded.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param version: The optional version, or no-argument callable that returns an
        optional version. This is prepended before the subkeys.

        The following example describes how to store the versioned data from the Rhea
        database for biologically relevant chemical reactions.

        .. code-block::

            import pystow
            import requests

            def get_rhea_version() -> str:
                res = requests.get("https://ftp.expasy.org/databases/rhea/rhea-release.properties")
                _, _, version = res.text.splitlines()[0].partition("=")
                return version

            path = pystow.ensure(
                "rhea",
                url="ftp://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz",
                version=get_rhea_version,
            )

    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.

    :returns: The path of the file that has been downloaded (or already exists)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure(
        *subkeys, url=url, name=name, version=version, force=force, download_kwargs=download_kwargs
    )


def ensure_custom(
    key: str,
    *subkeys: str,
    name: str,
    force: bool = False,
    provider: Provider,
    **kwargs: Any,
) -> Path:
    """Ensure a file is present, and run a custom create function otherwise.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The file name.
    :param force: Should the file be re-created, even if the path already exists?
    :param provider: The file provider. Will be run with the path as the first
        positional argument, if the file needs to be generated.
    :param kwargs: Additional keyword-based parameters passed to the provider.

    :returns: The path of the file that has been created (or already exists)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_custom(*subkeys, name=name, force=force, provider=provider, **kwargs)


def ensure_untar(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    directory: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    extract_kwargs: Mapping[str, Any] | None = None,
) -> Path:
    """Ensure a file is downloaded and untarred.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param directory: Overrides the name of the directory into which the tar archive is
        extracted. If none given, will use the stem of the file name that gets
        downloaded.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param extract_kwargs: Keyword arguments to pass to
        :meth:`tarfile.TarFile.extract_all`.

    :returns: The path of the directory where the file that has been downloaded gets
        extracted to
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_untar(
        *subkeys,
        url=url,
        name=name,
        directory=directory,
        force=force,
        download_kwargs=download_kwargs,
        extract_kwargs=extract_kwargs,
    )


def ensure_gunzip(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    autoclean: bool = True,
    download_kwargs: Mapping[str, Any] | None = None,
) -> Path:
    """Ensure a file is downloaded and gunzipped.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param autoclean: Should the zipped file be deleted?
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.

    :returns: The path of the directory where the file that has been downloaded gets
        extracted to
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_gunzip(
        *subkeys,
        url=url,
        name=name,
        force=force,
        autoclean=autoclean,
        download_kwargs=download_kwargs,
    )


# docstr-coverage:excused `overload`
@overload
@contextmanager
def ensure_open(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None,
    force: bool,
    download_kwargs: Mapping[str, Any] | None,
    mode: Literal["r", "rt", "w", "wt"] = ...,
    open_kwargs: Mapping[str, Any] | None,
) -> Generator[StringIO, None, None]: ...


# docstr-coverage:excused `overload`
@overload
@contextmanager
def ensure_open(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None,
    force: bool,
    download_kwargs: Mapping[str, Any] | None,
    mode: Literal["rb", "wb"] = ...,
    open_kwargs: Mapping[str, Any] | None,
) -> Generator[BytesIO, None, None]: ...


@contextmanager
def ensure_open(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: Literal["r", "rt", "w", "wt"] | Literal["rb", "wb"] = "r",
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[StringIO | BytesIO, None, None]:
    """Ensure a file is downloaded and open it.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`lzma.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`lzma.open`

    :yields: An open file object
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
    ) as yv:
        yield yv


@contextmanager
def ensure_open_zip(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: str = "r",
    open_kwargs: Mapping[str, Any] | None = None,
) -> BytesOpener:
    """Ensure a file is downloaded then open it with :mod:`zipfile`.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param inner_path: The relative path to the file inside the archive
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`zipfile.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`zipfile.open`

    :yields: An open file object
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open_zip(
        *subkeys,
        url=url,
        inner_path=inner_path,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
    ) as yv:
        yield yv


# docstr-coverage:excused `overload`
@overload
@contextmanager
def ensure_open_lzma(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None,
    force: bool,
    download_kwargs: Mapping[str, Any] | None,
    mode: Literal["r", "w", "rt", "wt"] = "rt",
    open_kwargs: Mapping[str, Any] | None,
) -> Generator[io.TextIOWrapper[lzma.LZMAFile], None, None]: ...


# docstr-coverage:excused `overload`
@overload
@contextmanager
def ensure_open_lzma(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None,
    force: bool,
    download_kwargs: Mapping[str, Any] | None,
    mode: Literal["rb", "wb"] = ...,
    open_kwargs: Mapping[str, Any] | None,
) -> Generator[lzma.LZMAFile, None, None]: ...


@contextmanager
def ensure_open_lzma(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: Literal["r", "rb", "w", "wb", "rt", "wt"] = "rt",
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[lzma.LZMAFile | io.TextIOWrapper[lzma.LZMAFile], None, None]:
    """Ensure a LZMA-compressed file is downloaded and open a file inside it.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`lzma.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`lzma.open`

    :yields: An open file object
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open_lzma(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
    ) as yv:
        yield yv


@contextmanager
def ensure_open_tarfile(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: str = "r",
    open_kwargs: Mapping[str, Any] | None = None,
) -> BytesOpener:
    """Ensure a tar file is downloaded and open a file inside it.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param inner_path: The relative path to the file inside the archive
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`tarfile.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`tarfile.open`

    :yields: An open file object
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open_tarfile(
        *subkeys,
        url=url,
        inner_path=inner_path,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
    ) as yv:
        yield yv


# docstr-coverage:excused `overload`
@overload
@contextmanager
def ensure_open_gz(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None,
    mode: Literal["r", "w", "rt", "wt"] = ...,
    open_kwargs: Mapping[str, Any] | None,
) -> Generator[StringIO, None, None]: ...


# docstr-coverage:excused `overload`
@overload
@contextmanager
def ensure_open_gz(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: Literal["rb", "wb"] = ...,
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[BytesIO, None, None]: ...


@contextmanager
def ensure_open_gz(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: Literal["r", "rb", "w", "wb", "rt", "wt"] = "rb",
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[StringIO | BytesIO, None, None]:
    """Ensure a gzipped file is downloaded and open a file inside it.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`gzip.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`

    :yields: An open file object
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open_gz(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
    ) as yv:
        yield yv


@contextmanager
def ensure_open_bz2(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: Literal["rb"] = "rb",
    open_kwargs: Mapping[str, Any] | None = None,
) -> Generator[bz2.BZ2File, None, None]:
    """Ensure a BZ2-compressed file is downloaded and open a file inside it.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`bz2.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`bz2.open`

    :yields: An open file object
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open_bz2(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
    ) as yv:
        yield yv


def ensure_csv(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    read_csv_kwargs: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Download a CSV and open as a dataframe with :mod:`pandas`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param read_csv_kwargs: Keyword arguments to pass through to
        :func:`pandas.read_csv`.

        .. note::

            It is assumed that the CSV uses tab separators, as this is the only safe
            option. For more information, see `Wikipedia
            <https://en.wikipedia.org/wiki/Comma-separated_values>`_ and `Issue #51
            <https://github.com/cthoyt/pystow/issues/51>`_. To override this behavior
            and load using the comma separator, specify
            ``read_csv_kwargs=dict(sep=",")``.


    :returns: A pandas DataFrame

    Example usage:

    ::

        >>> import pystow
        >>> import pandas as pd
        >>> url = "https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt"
        >>> df: pd.DataFrame = pystow.ensure_csv("pykeen", "datasets", "nations", url=url)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_csv(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        read_csv_kwargs=read_csv_kwargs,
    )


def load_df(
    key: str,
    *subkeys: str,
    name: str,
    read_csv_kwargs: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Open a pre-existing CSV as a dataframe with :mod:`pandas`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param read_csv_kwargs: Keyword arguments to pass through to
        :func:`pandas.read_csv`.

    :returns: A pandas DataFrame

    Example usage:

    ::

        >>> import pystow
        >>> import pandas as pd
        >>> url = "https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt"
        >>> pystow.ensure_csv("pykeen", "datasets", "nations", url=url)
        >>> df: pd.DataFrame = pystow.load_df("pykeen", "datasets", "nations", name="test.txt")
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.load_df(
        *subkeys,
        name=name,
        read_csv_kwargs=read_csv_kwargs,
    )


def dump_df(
    key: str,
    *subkeys: str,
    name: str,
    obj: pd.DataFrame,
    sep: str = "\t",
    index: bool = False,
    to_csv_kwargs: Mapping[str, Any] | None = None,
) -> None:
    """Dump a dataframe to a TSV file with :mod:`pandas`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param obj: The dataframe to dump
    :param sep: The separator to use, defaults to a tab
    :param index: Should the index be dumped? Defaults to false.
    :param to_csv_kwargs: Keyword arguments to pass through to
        :meth:`pandas.DataFrame.to_csv`.
    """
    _module = Module.from_key(key, ensure_exists=True)
    _module.dump_df(
        *subkeys,
        name=name,
        obj=obj,
        sep=sep,
        index=index,
        to_csv_kwargs=to_csv_kwargs,
    )


def ensure_json(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    open_kwargs: Mapping[str, Any] | None = None,
    json_load_kwargs: Mapping[str, Any] | None = None,
) -> JSON:
    """Download JSON and open with :mod:`json`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param open_kwargs: Additional keyword arguments passed to :func:`open`
    :param json_load_kwargs: Keyword arguments to pass through to :func:`json.load`.

    :returns: A JSON object (list, dict, etc.)

    Example usage:

    ::

        >>> import pystow
        >>> url = "https://maayanlab.cloud/CREEDS/download/single_gene_perturbations-v1.0.json"
        >>> perturbations = pystow.ensure_json("bio", "creeds", "1.0", url=url)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_json(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        open_kwargs=open_kwargs,
        json_load_kwargs=json_load_kwargs,
    )


def ensure_json_bz2(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    open_kwargs: Mapping[str, Any] | None = None,
    json_load_kwargs: Mapping[str, Any] | None = None,
) -> JSON:
    """Download BZ2-compressed JSON and open with :mod:`json`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param open_kwargs: Additional keyword arguments passed to :func:`bz2.open`
    :param json_load_kwargs: Keyword arguments to pass through to :func:`json.load`.

    :returns: A JSON object (list, dict, etc.)

    Example usage:

    ::

        >>> import pystow
        >>> url = "https://github.com/hetio/hetionet/raw/master/hetnet/json/hetionet-v1.0.json.bz2"
        >>> hetionet = pystow.ensure_json_bz2("bio", "hetionet", "1.0", url=url)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_json_bz2(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        open_kwargs=open_kwargs,
        json_load_kwargs=json_load_kwargs,
    )


def load_json(
    key: str,
    *subkeys: str,
    name: str,
    json_load_kwargs: Mapping[str, Any] | None = None,
) -> JSON:
    """Open a JSON file :mod:`json`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param json_load_kwargs: Keyword arguments to pass through to :func:`json.load`.

    :returns: A JSON object (list, dict, etc.)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.load_json(*subkeys, name=name, json_load_kwargs=json_load_kwargs)


def dump_json(
    key: str,
    *subkeys: str,
    name: str,
    obj: JSON,
    open_kwargs: Mapping[str, Any] | None = None,
    json_dump_kwargs: Mapping[str, Any] | None = None,
) -> None:
    """Dump an object to a file with :mod:`json`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param obj: The object to dump
    :param open_kwargs: Additional keyword arguments passed to :func:`open`
    :param json_dump_kwargs: Keyword arguments to pass through to :func:`json.dump`.
    """
    _module = Module.from_key(key, ensure_exists=True)
    _module.dump_json(
        *subkeys, name=name, obj=obj, open_kwargs=open_kwargs, json_dump_kwargs=json_dump_kwargs
    )


def ensure_pickle(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: Literal["rb"] = "rb",
    open_kwargs: Mapping[str, Any] | None = None,
    pickle_load_kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Download a pickle file and open with :mod:`pickle`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`open`
    :param open_kwargs: Additional keyword arguments passed to :func:`open`
    :param pickle_load_kwargs: Keyword arguments to pass through to :func:`pickle.load`.

    :returns: Any object
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_pickle(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
        pickle_load_kwargs=pickle_load_kwargs,
    )


def load_pickle(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["rb"] = "rb",
    open_kwargs: Mapping[str, Any] | None = None,
    pickle_load_kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Open a pickle file with :mod:`pickle`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param mode: The read mode, passed to :func:`open`
    :param open_kwargs: Additional keyword arguments passed to :func:`open`
    :param pickle_load_kwargs: Keyword arguments to pass through to :func:`pickle.load`.

    :returns: Any object
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.load_pickle(
        *subkeys,
        name=name,
        mode=mode,
        open_kwargs=open_kwargs,
        pickle_load_kwargs=pickle_load_kwargs,
    )


def dump_pickle(
    key: str,
    *subkeys: str,
    name: str,
    obj: Any,
    mode: Literal["wb"] = "wb",
    open_kwargs: Mapping[str, Any] | None = None,
    pickle_dump_kwargs: Mapping[str, Any] | None = None,
) -> None:
    """Dump an object to a file with :mod:`pickle`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param obj: The object to dump
    :param mode: The read mode, passed to :func:`open`
    :param open_kwargs: Additional keyword arguments passed to :func:`open`
    :param pickle_dump_kwargs: Keyword arguments to pass through to :func:`pickle.dump`.
    """
    _module = Module.from_key(key, ensure_exists=True)
    _module.dump_pickle(
        *subkeys,
        name=name,
        obj=obj,
        mode=mode,
        open_kwargs=open_kwargs,
        pickle_dump_kwargs=pickle_dump_kwargs,
    )


def ensure_pickle_gz(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    mode: Literal["rb"] = "rb",
    open_kwargs: Mapping[str, Any] | None = None,
    pickle_load_kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Download a gzipped pickle file and open with :mod:`pickle`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param mode: The read mode, passed to :func:`gzip.open`
    :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`
    :param pickle_load_kwargs: Keyword arguments to pass through to :func:`pickle.load`.

    :returns: Any object
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_pickle_gz(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        mode=mode,
        open_kwargs=open_kwargs,
        pickle_load_kwargs=pickle_load_kwargs,
    )


def load_pickle_gz(
    key: str,
    *subkeys: str,
    name: str,
    mode: Literal["rb"] = "rb",
    open_kwargs: Mapping[str, Any] | None = None,
    pickle_load_kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Open a gzipped pickle file with :mod:`pickle`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param mode: The read mode, passed to :func:`open`
    :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`
    :param pickle_load_kwargs: Keyword arguments to pass through to :func:`pickle.load`.

    :returns: Any object
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.load_pickle_gz(
        *subkeys,
        name=name,
        mode=mode,
        open_kwargs=open_kwargs,
        pickle_load_kwargs=pickle_load_kwargs,
    )


def ensure_xml(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    parse_kwargs: Mapping[str, Any] | None = None,
) -> lxml.etree.ElementTree:
    """Download an XML file and open it with :mod:`lxml`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param parse_kwargs: Keyword arguments to pass through to :func:`lxml.etree.parse`.

    :returns: An ElementTree object

    .. warning::

        If you have lots of files to read in the same archive, it's better just to unzip
        first.
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_xml(
        *subkeys,
        name=name,
        url=url,
        force=force,
        download_kwargs=download_kwargs,
        parse_kwargs=parse_kwargs,
    )


def load_xml(
    key: str,
    *subkeys: str,
    name: str,
    parse_kwargs: Mapping[str, Any] | None = None,
) -> lxml.etree.ElementTree:
    """Load an XML file with :mod:`lxml`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param parse_kwargs: Keyword arguments to pass through to :func:`lxml.etree.parse`.

    :returns: An ElementTree object

    .. warning::

        If you have lots of files to read in the same archive, it's better just to unzip
        first.
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.load_xml(
        *subkeys,
        name=name,
        parse_kwargs=parse_kwargs,
    )


def dump_xml(
    key: str,
    *subkeys: str,
    name: str,
    obj: lxml.etree.ElementTree,
    open_kwargs: Mapping[str, Any] | None = None,
    write_kwargs: Mapping[str, Any] | None = None,
) -> None:
    """Dump an XML element tree to a file with :mod:`lxml`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param obj: The object to dump
    :param open_kwargs: Additional keyword arguments passed to :func:`open`
    :param write_kwargs: Keyword arguments to pass through to
        :func:`lxml.etree.ElementTree.write`.
    """
    _module = Module.from_key(key, ensure_exists=True)
    _module.dump_xml(
        *subkeys,
        name=name,
        obj=obj,
        open_kwargs=open_kwargs,
        write_kwargs=write_kwargs,
    )


def ensure_excel(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    read_excel_kwargs: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Download an excel file and open as a dataframe with :mod:`pandas`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param read_excel_kwargs: Keyword arguments to pass through to
        :func:`pandas.read_excel`.

    :returns: A pandas DataFrame
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_excel(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        read_excel_kwargs=read_excel_kwargs,
    )


def ensure_tar_df(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    read_csv_kwargs: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Download a tar file and open an inner file as a dataframe with :mod:`pandas`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param inner_path: The relative path to the file inside the archive
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param read_csv_kwargs: Keyword arguments to pass through to
        :func:`pandas.read_csv`.

    :returns: A dataframe

    .. warning::

        If you have lots of files to read in the same archive, it's better just to unzip
        first.
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_tar_df(
        *subkeys,
        url=url,
        name=name,
        force=force,
        inner_path=inner_path,
        download_kwargs=download_kwargs,
        read_csv_kwargs=read_csv_kwargs,
    )


def ensure_tar_xml(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    parse_kwargs: Mapping[str, Any] | None = None,
) -> lxml.etree.ElementTree:
    """Download a tar file and open an inner file as an XML with :mod:`lxml`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param inner_path: The relative path to the file inside the archive
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param parse_kwargs: Keyword arguments to pass through to :func:`lxml.etree.parse`.

    :returns: An ElementTree object

    .. warning::

        If you have lots of files to read in the same archive, it's better just to unzip
        first.
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_tar_xml(
        *subkeys,
        url=url,
        name=name,
        force=force,
        inner_path=inner_path,
        download_kwargs=download_kwargs,
        parse_kwargs=parse_kwargs,
    )


def ensure_zip_df(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    read_csv_kwargs: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Download a zip file and open an inner file as a dataframe with :mod:`pandas`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param inner_path: The relative path to the file inside the archive
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param read_csv_kwargs: Keyword arguments to pass through to
        :func:`pandas.read_csv`.

    :returns: A pandas DataFrame
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_zip_df(
        *subkeys,
        url=url,
        name=name,
        force=force,
        inner_path=inner_path,
        download_kwargs=download_kwargs,
        read_csv_kwargs=read_csv_kwargs,
    )


def ensure_zip_np(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    load_kwargs: Mapping[str, Any] | None = None,
) -> numpy.typing.ArrayLike:
    """Download a zip file and open an inner file as an array-like with :mod:`numpy`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param inner_path: The relative path to the file inside the archive
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param load_kwargs: Additional keyword arguments that are passed through to
        :func:`read_zip_np` and transitively to :func:`numpy.load`.

    :returns: An array-like object
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_zip_np(
        *subkeys,
        url=url,
        name=name,
        force=force,
        inner_path=inner_path,
        download_kwargs=download_kwargs,
        load_kwargs=load_kwargs,
    )


def ensure_rdf(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
    precache: bool = True,
    parse_kwargs: Mapping[str, Any] | None = None,
) -> rdflib.Graph:
    """Download a RDF file and open with :mod:`rdflib`.

    :param key: The module name
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.
    :param precache: Should the parsed :class:`rdflib.Graph` be stored as a pickle for
        fast loading?
    :param parse_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.read_rdf` and transitively to :func:`rdflib.Graph.parse`.

    :returns: An RDF graph

    Example usage

    .. code-block:: python

        import pystow
        import rdflib

        url = "https://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz"
        rdf_graph: rdflib.Graph = pystow.ensure_rdf("rhea", url=url, parse_kwargs={"format": "xml"})

    .. note::

        Sometimes, :mod:`rdflib` is able to guess the format, and you can omit the
        "format" from the `parse_kwargs` argument.

    Here's another example

    .. code-block:: python

        import pystow
        import rdflib

        url = "http://oaei.webdatacommons.org/tdrs/testdata/persistent/knowledgegraph/v3/suite/memoryalpha-stexpanded/component/reference.xml"
        rdf_graph: rdflib.Graph = pystow.ensure_rdf(
            "memoryalpha-stexpanded", url=url, parse_kwargs={"format": "xml"}
        )
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_rdf(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        precache=precache,
        parse_kwargs=parse_kwargs,
    )


def load_rdf(
    key: str,
    *subkeys: str,
    name: str | None = None,
    parse_kwargs: Mapping[str, Any] | None = None,
) -> rdflib.Graph:
    """Open an RDF file with :mod:`rdflib`.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param parse_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.read_rdf` and transitively to :func:`rdflib.Graph.parse`.

    :returns: An RDF graph
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.load_rdf(*subkeys, name=name, parse_kwargs=parse_kwargs)


def dump_rdf(
    key: str,
    *subkeys: str,
    name: str,
    obj: rdflib.Graph,
    format: str = "turtle",
    serialize_kwargs: Mapping[str, Any] | None = None,
) -> None:
    """Dump an RDF graph to a file with :mod:`rdflib`.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file to open
    :param obj: The object to dump
    :param format: The format to dump in
    :param serialize_kwargs: Keyword arguments to through to
        :func:`rdflib.Graph.serialize`.
    """
    _module = Module.from_key(key, ensure_exists=True)
    _module.dump_rdf(*subkeys, name=name, obj=obj, format=format, serialize_kwargs=serialize_kwargs)


def ensure_from_s3(
    key: str,
    *subkeys: str,
    s3_bucket: str,
    s3_key: str | Sequence[str],
    name: str | None = None,
    force: bool = False,
    **kwargs: Any,
) -> Path:
    """Ensure a file is downloaded.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param s3_bucket: The S3 bucket name
    :param s3_key: The S3 key name
    :param name: Overrides the name of the file at the end of the S3 key, if given.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param kwargs: Remaining kwargs to forwrad to :class:`Module.ensure_from_s3`.

    :returns: The path of the file that has been downloaded (or already exists)

    Example downloading ProtMapper 0.0.21:

    .. code-block:: python

        import pystow

        version = "0.0.21"
        pystow.ensure_from_s3(
            "test",
            version,
            s3_bucket="bigmech",
            s3_key=f"protmapper/{version}/refseq_uniprot.csv",
        )
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_from_s3(
        *subkeys, s3_bucket=s3_bucket, s3_key=s3_key, name=name, force=force, **kwargs
    )


def ensure_from_google(
    key: str,
    *subkeys: str,
    name: str,
    file_id: str,
    force: bool = False,
) -> Path:
    """Ensure a file is downloaded from google drive.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the file
    :param file_id: The file identifier of the google file. If your share link is
        https://drive.google.com/file/d/1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z/view, then
        your file id is ``1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z``.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.

    :returns: The path of the file that has been downloaded (or already exists)

    Example downloading the WK3l-15k dataset as motivated by
    https://github.com/pykeen/pykeen/pull/403:

    .. code-block:: python

        import pystow

        path = pystow.ensure_from_google(
            "test", name="wk3l15k.zip", file_id="1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z"
        )
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_from_google(*subkeys, name=name, file_id=file_id, force=force)


def joinpath_sqlite(key: str, *subkeys: str, name: str) -> str:
    """Get an SQLite database connection string.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param name: The name of the database file.

    :returns: A SQLite path string.
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.joinpath_sqlite(*subkeys, name=name)


@contextmanager
def ensure_open_sqlite(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
) -> Generator[sqlite3.Connection, None, None]:
    """Ensure and connect to a SQLite database.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.

    :yields: An instance of :class:`sqlite3.Connection` from :func:`sqlite3.connect`

    Example usage:

    .. code-block:: python

        import pystow
        import pandas as pd

        url = "https://s3.amazonaws.com/bbop-sqlite/obi.db"
        sql = "SELECT * FROM entailed_edge LIMIT 10"
        with pystow.ensure_open_sqlite("test", url=url) as conn:
            df = pd.read_sql(sql, conn)
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open_sqlite(
        *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
    ) as yv:
        yield yv


@contextmanager
def ensure_open_sqlite_gz(
    key: str,
    *subkeys: str,
    url: str,
    name: str | None = None,
    force: bool = False,
    download_kwargs: Mapping[str, Any] | None = None,
) -> Generator[sqlite3.Connection, None, None]:
    """Ensure and connect to a gzipped SQLite database.

    :param key: The name of the module. No funny characters. The envvar `<key>_HOME`
        where key is uppercased is checked first before using the default home
        directory.
    :param subkeys: A sequence of additional strings to join. If none are given, returns
        the directory for this module.
    :param url: The URL to download.
    :param name: Overrides the name of the file at the end of the URL, if given. Also
        useful for URLs that don't have proper filenames with extensions.
    :param force: Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to
        :func:`pystow.utils.download`.

    :yields: An instance of :class:`sqlite3.Connection` from :func:`sqlite3.connect`

    Example usage: >>> import pystow >>> import pandas as pd >>> url =
    "https://s3.amazonaws.com/bbop-sqlite/hp.db.gz" >>> sql = "SELECT * FROM
    entailed_edge LIMIT 10" >>> with pystow.ensure_open_sqlite_gz("test", url=url) as
    conn: >>> df = pd.read_sql(sql, conn)
    """
    _module = Module.from_key(key, ensure_exists=True)
    with _module.ensure_open_sqlite_gz(
        *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
    ) as yv:
        yield yv


@lru_cache
def ensure_nltk(resource: str = "stopwords") -> tuple[Path, bool]:
    """Ensure NLTK data is downloaded in a standard way.

    :param resource: Name of the resource to download, e.g., ``stopwords``
    :returns:
        A pair of the NLTK cache directory and a boolean that says if download was successful

    This function also appends the standard PyStow location for NLTK data to the
    :data:`nltk.data.path` list so any downstream users of NLTK will know how to
    find it automatically.
    """
    import nltk.data

    directory = join("nltk")

    result = nltk.download(resource, download_dir=directory, quiet=True)
    if directory not in nltk.data.path:
        nltk.data.path.append(directory)

    # this is cached so you don't have to keep checking
    # if the package was downloaded

    return directory, result
