"""Module implementation."""

from __future__ import annotations

import bz2
import gzip
import io
import json
import logging
import lzma
import os
import pickle
import sqlite3
import tarfile
import zipfile
from collections.abc import Generator, Mapping, Sequence
from contextlib import closing, contextmanager
from io import BytesIO, StringIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)

from typing_extensions import TypeAlias

from . import utils
from .constants import JSON, BytesOpener, Provider
from .utils import (
    base_from_gzip_name,
    download_from_google,
    download_from_s3,
    get_base,
    gunzip,
    mkdir,
    name_from_s3_key,
    name_from_url,
    path_to_sqlite,
    read_rdf,
    read_tarfile_csv,
    read_tarfile_xml,
    read_zip_np,
    read_zipfile_csv,
)

if TYPE_CHECKING:
    import botocore.client
    import bs4
    import lxml.etree
    import numpy
    import pandas as pd
    import rdflib

__all__ = [
    "Module",
    "VersionHint",
]

logger = logging.getLogger(__name__)

#: A type hint for something that can be passed to the
#: `version` argument of Module.join, Module.ensure, etc.
VersionHint: TypeAlias = Union[None, str, Callable[[], Optional[str]]]


class Module:
    """The class wrapping the directory lookup implementation."""

    def __init__(self, base: str | Path, ensure_exists: bool = True) -> None:
        """Initialize the module.

        :param base: The base directory for the module
        :param ensure_exists: Should the base directory be created automatically?
            Defaults to true.
        """
        self.base = Path(base)
        mkdir(self.base, ensure_exists=ensure_exists)

    @classmethod
    def from_key(cls, key: str, *subkeys: str, ensure_exists: bool = True) -> Module:
        """Get a module for the given directory or one of its subdirectories.

        :param key: The name of the module. No funny characters. The envvar <key>_HOME
            where key is uppercased is checked first before using the default home
            directory.
        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param ensure_exists: Should all directories be created automatically? Defaults
            to true.

        :returns: A module
        """
        base = get_base(key, ensure_exists=False)
        rv = cls(base=base, ensure_exists=ensure_exists)
        if subkeys:
            rv = rv.module(*subkeys, ensure_exists=ensure_exists)
        return rv

    def module(self, *subkeys: str, ensure_exists: bool = True) -> Module:
        """Get a module for a subdirectory of the current module.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param ensure_exists: Should all directories be created automatically? Defaults
            to true.

        :returns: A module representing the subdirectory based on the given ``subkeys``.
        """
        base = self.join(*subkeys, ensure_exists=False)
        return Module(base=base, ensure_exists=ensure_exists)

    def join(
        self,
        *subkeys: str,
        name: str | None = None,
        ensure_exists: bool = True,
        version: VersionHint = None,
    ) -> Path:
        """Get a subdirectory of the current module.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param ensure_exists: Should all directories be created automatically? Defaults
            to true.
        :param name: The name of the file (optional) inside the folder
        :param version: The optional version, or no-argument callable that returns an
            optional version. This is prepended before the subkeys.

            The following example describes how to store the versioned data from the
            Rhea database for biologically relevant chemical reactions.

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
                module = pystow.module("rhea")
                path = module.join(name="rhea.rdf.gz", version=get_rhea_version)


        :returns: The path of the directory or subdirectory for the given module.
        """
        rv = self.base

        # if the version is given as a no-argument callable,
        # then it should be called and a version is returned
        if callable(version):
            version = version()
        if version:
            self._raise_for_invalid_version(version)
            subkeys = (version, *subkeys)

        if subkeys:
            rv = rv.joinpath(*subkeys)
            mkdir(rv, ensure_exists=ensure_exists)
        if name:
            rv = rv.joinpath(name)
        return rv

    @staticmethod
    def _raise_for_invalid_version(version: str) -> None:
        if "/" in version or os.sep in version:
            raise ValueError(
                f"slashes and `{os.sep}` not allowed in versions because of "
                f"conflicts with file path construction: {version}"
            )

    def joinpath_sqlite(self, *subkeys: str, name: str) -> str:
        """Get an SQLite database connection string.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the database file.

        :returns: A SQLite path string.
        """
        path = self.join(*subkeys, name=name, ensure_exists=True)
        return path_to_sqlite(path)

    def ensure(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        version: VersionHint = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
    ) -> Path:
        """Ensure a file is downloaded.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param version: The optional version, or no-argument callable that returns an
            optional version. This is prepended before the subkeys.

            The following example describes how to store the versioned data from the
            Rhea database for biologically relevant chemical reactions.

            .. code-block::

                import pystow
                import requests

                def get_rhea_version() -> str:
                    res = requests.get("https://ftp.expasy.org/databases/rhea/rhea-release.properties")
                    _, _, version = res.text.splitlines()[0].partition("=")
                    return version

                module = pystow.module("rhea")
                path = module.ensure(
                    url="ftp://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz",
                    version=get_rhea_version,
                )

        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.

        :returns: The path of the file that has been downloaded (or already exists)
        """
        if name is None:
            name = name_from_url(url)
        path = self.join(*subkeys, name=name, version=version, ensure_exists=True)
        utils.download(
            url=url,
            path=path,
            force=force,
            **(download_kwargs or {}),
        )
        return path

    def ensure_soup(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        version: VersionHint = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        mode: Literal["r", "rt", "w", "wt"] | Literal["rb", "wb"] = "r",
        open_kwargs: Mapping[str, Any] | None = None,
        beautiful_soup_kwargs: Mapping[str, Any] | None = None,
    ) -> bs4.BeautifulSoup:
        """Ensure a webpage is downloaded and parsed with :mod:`BeautifulSoup`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param version: The optional version, or no-argument callable that returns an
            optional version. This is prepended before the subkeys.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`open`
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param beautiful_soup_kwargs: Additional keyword arguments passed to :class:`BeautifulSoup`

        :returns: An BeautifulSoup object

        .. note:: If you don't need to cache, consider using :func:`pystow.utils.get_soup` instead.
        """
        from bs4 import BeautifulSoup

        beautiful_soup_kwargs = dict(beautiful_soup_kwargs or {})
        beautiful_soup_kwargs.setdefault("features", "html.parser")

        with self.ensure_open(
            *subkeys,
            url=url,
            name=name,
            version=version,
            force=force,
            download_kwargs=download_kwargs,
            mode=mode,
            open_kwargs=open_kwargs,
        ) as file:
            soup = BeautifulSoup(file, **beautiful_soup_kwargs)
        return soup

    def ensure_custom(
        self,
        *subkeys: str,
        name: str,
        force: bool = False,
        provider: Provider,
        **kwargs: Any,
    ) -> Path:
        """Ensure a file is present, and run a custom create function otherwise.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The file name.
        :param force: Should the file be re-created, even if the path already exists?
        :param provider: The file provider. Will be run with the path as the first
            positional argument, if the file needs to be generated.
        :param kwargs: Additional keyword-based parameters passed to the provider.

        :returns: The path of the file that has been created (or already exists)

        :raises ValueError: If the provider was called but the file was not created by
            it.
        """
        path = self.join(*subkeys, name=name, ensure_exists=True)
        if path.is_file() and not force:
            return path
        provider(path, **kwargs)
        if not path.is_file():
            raise ValueError(f"Provider {provider} did not create the file at {path}!")
        return path

    def ensure_untar(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        directory: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        extract_kwargs: Mapping[str, Any] | None = None,
    ) -> Path:
        """Ensure a tar file is downloaded and unarchived.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param directory: Overrides the name of the directory into which the tar archive
            is extracted. If none given, will use the stem of the file name that gets
            downloaded.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param extract_kwargs: Keyword arguments to pass to
            :meth:`tarfile.TarFile.extract_all`.

        :returns: The path of the directory where the file that has been downloaded gets
            extracted to
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        if directory is None:
            # rhea-rxn.tar.gz -> rhea-rxn
            suffixes_len = sum(len(suffix) for suffix in path.suffixes)
            directory = path.name[:-suffixes_len]
        unzipped_path = path.parent.joinpath(directory)
        if unzipped_path.is_dir() and not force:
            return unzipped_path
        unzipped_path.mkdir(exist_ok=True, parents=True)
        with tarfile.open(path) as tar_file:
            tar_file.extractall(unzipped_path, **(extract_kwargs or {}))  # noqa:S202
        return unzipped_path

    def ensure_gunzip(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        autoclean: bool = True,
        download_kwargs: Mapping[str, Any] | None = None,
    ) -> Path:
        """Ensure a tar.gz file is downloaded and unarchived.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param autoclean: Should the zipped file be deleted?
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.

        :returns: The path of the directory where the file that has been downloaded gets
            extracted to
        """
        if name is None:
            name = name_from_url(url)
        gunzipped_name = base_from_gzip_name(name)
        gunzipped_path = self.join(*subkeys, name=gunzipped_name, ensure_exists=True)
        if gunzipped_path.is_file() and not force:
            return gunzipped_path
        path = self.ensure(
            *subkeys,
            url=url,
            name=name,
            force=force,
            download_kwargs=download_kwargs,
        )
        gunzip(path, gunzipped_path)
        if autoclean:
            logger.info("removing original gzipped file %s", path)
            path.unlink()
        return gunzipped_path

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def ensure_open(
        self,
        *subkeys: str,
        url: str,
        name: str | None,
        version: VersionHint = ...,
        force: bool,
        download_kwargs: Mapping[str, Any] | None,
        mode: Literal["r", "rt", "w", "wt"] = ...,
        open_kwargs: Mapping[str, Any] | None,
    ) -> Generator[StringIO, None, None]: ...

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def ensure_open(
        self,
        *subkeys: str,
        url: str,
        name: str | None,
        version: VersionHint = ...,
        force: bool,
        download_kwargs: Mapping[str, Any] | None,
        mode: Literal["rb", "wb"] = ...,
        open_kwargs: Mapping[str, Any] | None,
    ) -> Generator[BytesIO, None, None]: ...

    @contextmanager
    def ensure_open(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        version: VersionHint = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        mode: Literal["r", "rt", "w", "wt"] | Literal["rb", "wb"] = "r",
        open_kwargs: Mapping[str, Any] | None = None,
    ) -> Generator[StringIO | BytesIO, None, None]:
        """Ensure a file is downloaded and open it.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param version: The optional version, or no-argument callable that returns an
            optional version. This is prepended before the subkeys.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`open`
        :param open_kwargs: Additional keyword arguments passed to :func:`open`

        :yields: An open file object
        """
        path = self.ensure(
            *subkeys,
            url=url,
            name=name,
            version=version,
            force=force,
            download_kwargs=download_kwargs,
        )
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with path.open(**open_kwargs) as file:
            yield file

    @staticmethod
    def _raise_for_mode_ensure_mismatch(
        mode: Literal["r", "rt", "w", "wt", "wb", "rb"], ensure_exists: bool
    ) -> None:
        """Raise an exception for a mismatch between the mode and the ensure status.

        :param mode: The file opening mode
        :param ensure_exists: The value for ensuring a file

        :raises ValueError: In the following situations:

            1. If the file should be opened in write mode, and it is not ensured to
               exist
            2. If the file should be opened in read mode, and it is ensured to exist.
               This is bad because it will create a file when there previously wasn't
               one
        """
        if "w" in mode and not ensure_exists:
            raise ValueError
        if "r" in mode and ensure_exists:
            raise ValueError

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def open(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["r", "rt", "w", "wt"] = ...,
        open_kwargs: Mapping[str, Any] | None = None,
        ensure_exists: bool,
    ) -> Generator[StringIO, None, None]: ...

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def open(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["rb", "wb"] = ...,
        open_kwargs: Mapping[str, Any] | None = None,
        ensure_exists: bool,
    ) -> Generator[BytesIO, None, None]: ...

    @contextmanager
    def open(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["r", "rt", "w", "wt"] | Literal["rb", "wb"] = "r",
        open_kwargs: Mapping[str, Any] | None = None,
        ensure_exists: bool = False,
    ) -> Generator[StringIO | BytesIO, None, None]:
        """Open a file.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param mode: The read mode, passed to :func:`open`
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param ensure_exists: Should the directory the file is in be made? Set to true
            on write operations.

        :yields: An open file object.

        This function should be called inside a context manager like in the following

        .. code-block:: python

            import pystow

            with pystow.module("test").open(name="test.tsv", mode="w") as file:
                print("Test text!", file=file)
        """
        self._raise_for_mode_ensure_mismatch(mode, ensure_exists)
        path = self.join(*subkeys, name=name, ensure_exists=ensure_exists)
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with path.open(**open_kwargs) as file:
            yield file

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def open_gz(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["r", "w", "rt", "wt"] = ...,
        open_kwargs: Mapping[str, Any] | None,
        ensure_exists: bool,
    ) -> Generator[StringIO, None, None]: ...

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def open_gz(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["rb", "wb"] = ...,
        open_kwargs: Mapping[str, Any] | None,
        ensure_exists: bool,
    ) -> Generator[BytesIO, None, None]: ...

    @contextmanager
    def open_gz(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["r", "w", "rt", "wt", "rb", "wb"] = "rb",
        open_kwargs: Mapping[str, Any] | None = None,
        ensure_exists: bool = False,
    ) -> Generator[StringIO | BytesIO, None, None]:
        """Open a gzipped file that exists already.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param mode: The read mode, passed to :func:`gzip.open`
        :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`
        :param ensure_exists: Should the file be made? Set to true on write operations.

        :yields: An open file object
        """
        self._raise_for_mode_ensure_mismatch(mode, ensure_exists)
        path = self.join(*subkeys, name=name, ensure_exists=ensure_exists)
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with gzip.open(path, **open_kwargs) as file:
            yield file

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def ensure_open_lzma(
        self,
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
        self,
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
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        mode: Literal["r", "rb", "w", "wb", "rt", "wt"] = "rt",
        open_kwargs: Mapping[str, Any] | None = None,
    ) -> Generator[lzma.LZMAFile | io.TextIOWrapper[lzma.LZMAFile], None, None]:
        """Ensure a LZMA-compressed file is downloaded and open a file inside it.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`lzma.open`
        :param open_kwargs: Additional keyword arguments passed to :func:`lzma.open`

        :yields: An open file object
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with lzma.open(path, **open_kwargs) as file:
            yield file

    @contextmanager
    def ensure_open_tarfile(
        self,
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

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param inner_path: The relative path to the file inside the archive
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`tarfile.open`
        :param open_kwargs: Additional keyword arguments passed to :func:`tarfile.open`

        :yields: An open file object
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with tarfile.open(path, **open_kwargs) as tar_file:
            with tar_file.extractfile(inner_path) as file:  # type:ignore
                yield file

    @contextmanager
    def ensure_open_zip(
        self,
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

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param inner_path: The relative path to the file inside the archive
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`zipfile.open`
        :param open_kwargs: Additional keyword arguments passed to :func:`zipfile.open`

        :yields: An open file object
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with zipfile.ZipFile(file=path) as zip_file:
            with zip_file.open(inner_path) as file:
                yield file

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def ensure_open_gz(
        self,
        *subkeys: str,
        url: str,
        name: str | None,
        force: bool,
        download_kwargs: Mapping[str, Any] | None,
        mode: Literal["r", "w", "rt", "wt"] = ...,
        open_kwargs: Mapping[str, Any] | None,
    ) -> Generator[StringIO, None, None]: ...

    # docstr-coverage:excused `overload`
    @overload
    @contextmanager
    def ensure_open_gz(
        self,
        *subkeys: str,
        url: str,
        name: str | None,
        force: bool,
        download_kwargs: Mapping[str, Any] | None,
        mode: Literal[
            "rb",
            "wb",
        ] = ...,
        open_kwargs: Mapping[str, Any] | None,
    ) -> Generator[BytesIO, None, None]: ...

    @contextmanager
    def ensure_open_gz(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        mode: Literal["r", "rb", "w", "wb", "rt", "wt"] = "rb",
        open_kwargs: Mapping[str, Any] | None = None,
    ) -> Generator[StringIO | BytesIO, None, None]:
        """Ensure a gzipped file is downloaded and open a file inside it.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`gzip.open`
        :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`

        :yields: An open file object
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with gzip.open(path, **open_kwargs) as file:
            yield file

    @contextmanager
    def ensure_open_bz2(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        mode: Literal["rb"] = "rb",
        open_kwargs: Mapping[str, Any] | None = None,
    ) -> Generator[bz2.BZ2File, None, None]:
        """Ensure a BZ2-compressed file is downloaded and open a file inside it.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`bz2.open`
        :param open_kwargs: Additional keyword arguments passed to :func:`bz2.open`

        :yields: An open file object
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        open_kwargs = {} if open_kwargs is None else dict(open_kwargs)
        open_kwargs.setdefault("mode", mode)
        with bz2.open(path, **open_kwargs) as file:
            yield file

    def ensure_csv(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        read_csv_kwargs: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Download a CSV and open as a dataframe with :mod:`pandas`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param read_csv_kwargs: Keyword arguments to pass through to
            :func:`pandas.read_csv`.

        :returns: A pandas DataFrame
        """
        import pandas as pd

        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        return pd.read_csv(path, **_clean_csv_kwargs(read_csv_kwargs))

    def load_df(
        self,
        *subkeys: str,
        name: str,
        read_csv_kwargs: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Open a pre-existing CSV as a dataframe with :mod:`pandas`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param read_csv_kwargs: Keyword arguments to pass through to
            :func:`pandas.read_csv`.

        :returns: A pandas DataFrame
        """
        import pandas as pd

        with self.open(*subkeys, name=name, mode="r", ensure_exists=False) as file:
            return pd.read_csv(file, **_clean_csv_kwargs(read_csv_kwargs))

    def dump_df(
        self,
        *subkeys: str,
        name: str,
        obj: pd.DataFrame,
        sep: str = "\t",
        index: bool = False,
        to_csv_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Dump a dataframe to a TSV file with :mod:`pandas`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param obj: The dataframe to dump
        :param sep: The separator to use, defaults to a tab
        :param index: Should the index be dumped? Defaults to false.
        :param to_csv_kwargs: Keyword arguments to pass through to
            :meth:`pandas.DataFrame.to_csv`.
        """
        to_csv_kwargs = {} if to_csv_kwargs is None else dict(to_csv_kwargs)
        to_csv_kwargs.setdefault("sep", sep)
        to_csv_kwargs.setdefault("index", index)
        # should this use unified opener instead? Pandas is pretty smart...
        path = self.join(*subkeys, name=name)
        obj.to_csv(path, **to_csv_kwargs)

    def ensure_json(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        open_kwargs: Mapping[str, Any] | None = None,
        json_load_kwargs: Mapping[str, Any] | None = None,
    ) -> JSON:
        """Download JSON and open with :mod:`json`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param json_load_kwargs: Keyword arguments to pass through to :func:`json.load`.

        :returns: A JSON object (list, dict, etc.)
        """
        with self.ensure_open(
            *subkeys,
            url=url,
            name=name,
            force=force,
            download_kwargs=download_kwargs,
            open_kwargs=open_kwargs,
        ) as file:
            return json.load(file, **(json_load_kwargs or {}))

    def ensure_json_bz2(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        open_kwargs: Mapping[str, Any] | None = None,
        json_load_kwargs: Mapping[str, Any] | None = None,
    ) -> JSON:
        """Download BZ2-compressed JSON and open with :mod:`json`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param open_kwargs: Additional keyword arguments passed to :func:`bz2.open`
        :param json_load_kwargs: Keyword arguments to pass through to :func:`json.load`.

        :returns: A JSON object (list, dict, etc.)
        """
        with self.ensure_open_bz2(
            *subkeys,
            url=url,
            name=name,
            force=force,
            download_kwargs=download_kwargs,
            open_kwargs=open_kwargs,
        ) as file:
            return json.load(file, **(json_load_kwargs or {}))

    def load_json(
        self,
        *subkeys: str,
        name: str,
        open_kwargs: Mapping[str, Any] | None = None,
        json_load_kwargs: Mapping[str, Any] | None = None,
    ) -> JSON:
        """Open a JSON file :mod:`json`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param json_load_kwargs: Keyword arguments to pass through to :func:`json.load`.

        :returns: A JSON object (list, dict, etc.)
        """
        with self.open(
            *subkeys, name=name, mode="r", open_kwargs=open_kwargs, ensure_exists=False
        ) as file:
            return json.load(file, **(json_load_kwargs or {}))

    def dump_json(
        self,
        *subkeys: str,
        name: str,
        obj: JSON,
        open_kwargs: Mapping[str, Any] | None = None,
        json_dump_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Dump an object to a file with :mod:`json`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param obj: The object to dump
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param json_dump_kwargs: Keyword arguments to pass through to :func:`json.dump`.
        """
        with self.open(
            *subkeys, name=name, mode="w", open_kwargs=open_kwargs, ensure_exists=True
        ) as file:
            json.dump(obj, file, **(json_dump_kwargs or {}))

    def ensure_pickle(
        self,
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

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`open`
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param pickle_load_kwargs: Keyword arguments to pass through to
            :func:`pickle.load`.

        :returns: Any object
        """
        with self.ensure_open(
            *subkeys,
            url=url,
            name=name,
            force=force,
            download_kwargs=download_kwargs,
            mode=mode,
            open_kwargs=open_kwargs,
        ) as file:
            return pickle.load(file, **(pickle_load_kwargs or {}))

    def load_pickle(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["rb"] = "rb",
        open_kwargs: Mapping[str, Any] | None = None,
        pickle_load_kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        """Open a pickle file with :mod:`pickle`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param mode: The read mode, passed to :func:`open`
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param pickle_load_kwargs: Keyword arguments to pass through to
            :func:`pickle.load`.

        :returns: Any object
        """
        with self.open(
            *subkeys,
            name=name,
            mode=mode,
            open_kwargs=open_kwargs,
            ensure_exists=False,
        ) as file:
            return pickle.load(file, **(pickle_load_kwargs or {}))

    def dump_pickle(
        self,
        *subkeys: str,
        name: str,
        obj: Any,
        mode: Literal["wb"] = "wb",
        open_kwargs: Mapping[str, Any] | None = None,
        pickle_dump_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Dump an object to a file with :mod:`pickle`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param obj: The object to dump
        :param mode: The read mode, passed to :func:`open`
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param pickle_dump_kwargs: Keyword arguments to pass through to
            :func:`pickle.dump`.
        """
        with self.open(
            *subkeys,
            name=name,
            mode=mode,
            open_kwargs=open_kwargs,
            ensure_exists=True,
        ) as file:
            pickle.dump(obj, file, **(pickle_dump_kwargs or {}))

    def ensure_pickle_gz(
        self,
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

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param mode: The read mode, passed to :func:`gzip.open`
        :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`
        :param pickle_load_kwargs: Keyword arguments to pass through to
            :func:`pickle.load`.

        :returns: Any object
        """
        with self.ensure_open_gz(
            *subkeys,
            url=url,
            name=name,
            force=force,
            download_kwargs=download_kwargs,
            mode=mode,
            open_kwargs=open_kwargs,
        ) as file:
            return pickle.load(file, **(pickle_load_kwargs or {}))

    def load_pickle_gz(
        self,
        *subkeys: str,
        name: str,
        mode: Literal["rb"] = "rb",
        open_kwargs: Mapping[str, Any] | None = None,
        pickle_load_kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        """Open a gzipped pickle file with :mod:`pickle`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param mode: The read mode, passed to :func:`open`
        :param open_kwargs: Additional keyword arguments passed to :func:`gzip.open`
        :param pickle_load_kwargs: Keyword arguments to pass through to
            :func:`pickle.load`.

        :returns: Any object
        """
        with self.open_gz(
            *subkeys,
            name=name,
            mode=mode,
            open_kwargs=open_kwargs,
            ensure_exists=False,
        ) as file:
            return pickle.load(file, **(pickle_load_kwargs or {}))

    def ensure_excel(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        read_excel_kwargs: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Download an excel file and open as a dataframe with :mod:`pandas`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param read_excel_kwargs: Keyword arguments to pass through to
            :func:`pandas.read_excel`.

        :returns: A pandas DataFrame
        """
        import pandas as pd

        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        return pd.read_excel(path, **(read_excel_kwargs or {}))

    def ensure_tar_df(
        self,
        *subkeys: str,
        url: str,
        inner_path: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        read_csv_kwargs: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Download a tar file and open an inner file as a dataframe with :mod:`pandas`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param inner_path: The relative path to the file inside the archive
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param read_csv_kwargs: Keyword arguments to pass through to
            :func:`pandas.read_csv`.

        :returns: A dataframe

        .. warning::

            If you have lots of files to read in the same archive, it's better just to
            unzip first.
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        return read_tarfile_csv(
            path=path, inner_path=inner_path, **_clean_csv_kwargs(read_csv_kwargs)
        )

    def ensure_xml(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        parse_kwargs: Mapping[str, Any] | None = None,
    ) -> lxml.etree.ElementTree:
        """Download an XML file and open it with :mod:`lxml`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param parse_kwargs: Keyword arguments to pass through to
            :func:`lxml.etree.parse`.

        :returns: An ElementTree object

        .. warning::

            If you have lots of files to read in the same archive, it's better just to
            unzip first.
        """
        from lxml import etree

        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        return etree.parse(path, **(parse_kwargs or {}))

    def load_xml(
        self,
        *subkeys: str,
        name: str,
        parse_kwargs: Mapping[str, Any] | None = None,
    ) -> lxml.etree.ElementTree:
        """Load an XML file with :mod:`lxml`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param parse_kwargs: Keyword arguments to pass through to
            :func:`lxml.etree.parse`.

        :returns: An ElementTree object

        .. warning::

            If you have lots of files to read in the same archive, it's better just to
            unzip first.
        """
        from lxml import etree

        with self.open(*subkeys, mode="r", name=name, ensure_exists=False) as file:
            return etree.parse(file, **(parse_kwargs or {}))

    def dump_xml(
        self,
        *subkeys: str,
        name: str,
        obj: lxml.etree.ElementTree,
        open_kwargs: Mapping[str, Any] | None = None,
        write_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Dump an XML element tree to a file with :mod:`lxml`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param obj: The object to dump
        :param open_kwargs: Additional keyword arguments passed to :func:`open`
        :param write_kwargs: Keyword arguments to pass through to
            :func:`lxml.etree.ElementTree.write`.
        """
        with self.open(
            *subkeys, name=name, mode="wb", open_kwargs=open_kwargs, ensure_exists=True
        ) as file:
            obj.write(file, **(write_kwargs or {}))

    def ensure_tar_xml(
        self,
        *subkeys: str,
        url: str,
        inner_path: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        parse_kwargs: Mapping[str, Any] | None = None,
    ) -> lxml.etree.ElementTree:
        """Download a tar file and open an inner file as an XML with :mod:`lxml`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param inner_path: The relative path to the file inside the archive
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param parse_kwargs: Keyword arguments to pass through to
            :func:`lxml.etree.parse`.

        :returns: An ElementTree object

        .. warning::

            If you have lots of files to read in the same archive, it's better just to
            unzip first.
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        return read_tarfile_xml(path=path, inner_path=inner_path, **(parse_kwargs or {}))

    def ensure_zip_df(
        self,
        *subkeys: str,
        url: str,
        inner_path: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        read_csv_kwargs: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Download a zip file and open an inner file as a dataframe with :mod:`pandas`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param inner_path: The relative path to the file inside the archive
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param read_csv_kwargs: Keyword arguments to pass through to
            :func:`pandas.read_csv`.

        :returns: A pandas DataFrame
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        return read_zipfile_csv(
            path=path, inner_path=inner_path, **_clean_csv_kwargs(read_csv_kwargs)
        )

    def ensure_zip_np(
        self,
        *subkeys: str,
        url: str,
        inner_path: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        load_kwargs: Mapping[str, Any] | None = None,
    ) -> numpy.typing.ArrayLike:
        """Download a zip file and open an inner file as an array-like with :mod:`numpy`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param inner_path: The relative path to the file inside the archive
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param load_kwargs: Additional keyword arguments that are passed through to
            :func:`read_zip_np` and transitively to :func:`numpy.load`.

        :returns: An array-like object
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        return read_zip_np(path=path, inner_path=inner_path, **(load_kwargs or {}))

    def ensure_rdf(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
        precache: bool = True,
        parse_kwargs: Mapping[str, Any] | None = None,
    ) -> rdflib.Graph:
        """Download a RDF file and open with :mod:`rdflib`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.
        :param precache: Should the parsed :class:`rdflib.Graph` be stored as a pickle
            for fast loading?
        :param parse_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.read_rdf` and transitively to
            :func:`rdflib.Graph.parse`.

        :returns: An RDF graph
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        if not precache:
            return read_rdf(path=path, **(parse_kwargs or {}))

        cache_path = path.with_suffix(path.suffix + ".pickle.gz")
        if cache_path.exists() and not force:
            import rdflib

            with gzip.open(cache_path, "rb") as file:
                return cast(rdflib.Graph, pickle.load(file))

        rv = read_rdf(path=path, **(parse_kwargs or {}))
        with gzip.open(cache_path, "wb") as file:
            pickle.dump(rv, file, protocol=pickle.HIGHEST_PROTOCOL)
        return rv

    def load_rdf(
        self,
        *subkeys: str,
        name: str | None = None,
        parse_kwargs: Mapping[str, Any] | None = None,
    ) -> rdflib.Graph:
        """Open an RDF file with :mod:`rdflib`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param parse_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.read_rdf` and transitively to
            :func:`rdflib.Graph.parse`.

        :returns: An RDF graph
        """
        path = self.join(*subkeys, name=name, ensure_exists=False)
        return read_rdf(path=path, **(parse_kwargs or {}))

    def dump_rdf(
        self,
        *subkeys: str,
        name: str,
        obj: rdflib.Graph,
        format: str = "turtle",
        serialize_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Dump an RDF graph to a file with :mod:`rdflib`.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file to open
        :param obj: The object to dump
        :param format: The format to dump in
        :param serialize_kwargs: Keyword arguments to through to
            :func:`rdflib.Graph.serialize`.
        """
        path = self.join(*subkeys, name=name, ensure_exists=False)
        serialize_kwargs = {} if serialize_kwargs is None else dict(serialize_kwargs)
        serialize_kwargs.setdefault("format", format)
        obj.serialize(path, **serialize_kwargs)

    def ensure_from_s3(
        self,
        *subkeys: str,
        s3_bucket: str,
        s3_key: str | Sequence[str],
        name: str | None = None,
        client: botocore.client.BaseClient | None = None,
        client_kwargs: Mapping[str, Any] | None = None,
        download_file_kwargs: Mapping[str, Any] | None = None,
        force: bool = False,
    ) -> Path:
        """Ensure a file is downloaded.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param s3_bucket: The S3 bucket name
        :param s3_key: The S3 key name
        :param name: Overrides the name of the file at the end of the S3 key, if given.
        :param client: A botocore client. If none given, one will be created
            automatically
        :param client_kwargs: Keyword arguments to be passed to the client on
            instantiation.
        :param download_file_kwargs: Keyword arguments to be passed to
            :func:`boto3.s3.transfer.S3Transfer.download_file`
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.

        :returns: The path of the file that has been downloaded (or already exists)
        """
        if not isinstance(s3_key, str):
            s3_key = "/".join(s3_key)  # join sequence
        if name is None:
            name = name_from_s3_key(s3_key)
        path = self.join(*subkeys, name=name, ensure_exists=True)
        download_from_s3(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            path=path,
            client=client,
            client_kwargs=client_kwargs,
            force=force,
            download_file_kwargs=download_file_kwargs,
        )
        return path

    def ensure_from_google(
        self,
        *subkeys: str,
        name: str,
        file_id: str,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
    ) -> Path:
        """Ensure a file is downloaded from Google Drive.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param name: The name of the file
        :param file_id: The file identifier of the Google file. If your share link is
            https://drive.google.com/file/d/1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z/view, then
            your file ID is ``1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z``.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download_from_google`.

        :returns: The path of the file that has been downloaded (or already exists)
        """
        path = self.join(*subkeys, name=name, ensure_exists=True)
        download_from_google(file_id, path, force=force, **(download_kwargs or {}))
        return path

    @contextmanager
    def ensure_open_sqlite(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
    ) -> Generator[sqlite3.Connection, None, None]:
        """Ensure and connect to a SQLite database.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.

        :yields: An instance of :class:`sqlite3.Connection` from :func:`sqlite3.connect`

        Example usage: >>> import pystow >>> import pandas as pd >>> url =
        "https://s3.amazonaws.com/bbop-sqlite/obi.db" >>> sql = "SELECT * FROM
        entailed_edge LIMIT 10" >>> module = pystow.module("test") >>> with
        module.ensure_open_sqlite(url=url) as conn: >>> df = pd.read_sql(sql, conn)
        """
        path = self.ensure(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        with closing(sqlite3.connect(path.as_posix())) as conn:
            yield conn

    @contextmanager
    def ensure_open_sqlite_gz(
        self,
        *subkeys: str,
        url: str,
        name: str | None = None,
        force: bool = False,
        download_kwargs: Mapping[str, Any] | None = None,
    ) -> Generator[sqlite3.Connection, None, None]:
        """Ensure and connect to a SQLite database that's gzipped.

        Unfortunately, it's a paid feature to directly read gzipped sqlite files, so
        this automatically gunzips it first.

        :param subkeys: A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url: The URL to download.
        :param name: Overrides the name of the file at the end of the URL, if given.
            Also useful for URLs that don't have proper filenames with extensions.
        :param force: Should the download be done again, even if the path already
            exists? Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to
            :func:`pystow.utils.download`.

        :yields: An instance of :class:`sqlite3.Connection` from :func:`sqlite3.connect`

        Example usage: >>> import pystow >>> import pandas as pd >>> url =
        "https://s3.amazonaws.com/bbop-sqlite/hp.db.gz" >>> module =
        pystow.module("test") >>> sql = "SELECT * FROM entailed_edge LIMIT 10" >>> with
        module.ensure_open_sqlite_gz(url=url) as conn: >>> df = pd.read_sql(sql, conn)
        """
        path = self.ensure_gunzip(
            *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
        )
        with closing(sqlite3.connect(path.as_posix())) as conn:
            yield conn


def _clean_csv_kwargs(read_csv_kwargs: None | Mapping[str, Any]) -> dict[str, Any]:
    read_csv_kwargs = {} if read_csv_kwargs is None else dict(read_csv_kwargs)
    read_csv_kwargs.setdefault("sep", "\t")
    return read_csv_kwargs
