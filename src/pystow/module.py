# -*- coding: utf-8 -*-

"""Module implementation."""

import gzip
import logging
import os
import pickle
import warnings
from pathlib import Path
from typing import Any, Mapping, Optional, TYPE_CHECKING, Union

import pandas as pd

from .utils import download, getenv_path, mkdir, name_from_url, read_rdf, read_tarfile_csv, read_zipfile_csv

if TYPE_CHECKING:
    import rdflib

logger = logging.getLogger(__name__)

PYSTOW_NAME_ENVVAR = 'PYSTOW_NAME'
PYSTOW_HOME_ENVVAR = 'PYSTOW_HOME'
PYSTOW_NAME_DEFAULT = '.data'


def get_name() -> str:
    """Get the PyStow home directory name."""
    return os.getenv(PYSTOW_NAME_ENVVAR, default=PYSTOW_NAME_DEFAULT)


def get_home(ensure_exists: bool = True) -> Path:
    """Get the PyStow home directory."""
    default = Path.home() / get_name()
    return getenv_path(PYSTOW_HOME_ENVVAR, default, ensure_exists=ensure_exists)


def get_base(key: str, ensure_exists: bool = True) -> Path:
    """Get the base directory for a module."""
    _assert_valid(key)
    envvar = f'{key.upper()}_HOME'
    default = get_home(ensure_exists=False) / key
    return getenv_path(envvar, default, ensure_exists=ensure_exists)


def _assert_valid(key: str) -> None:
    if '.' in key:
        raise ValueError


class Module:
    """The class wrapping the directory lookup implementation."""

    def __init__(self, base: Union[str, Path], ensure_exists: bool = True) -> None:
        """Initialize the module.

        :param base:
            The base directory for the module
        :param ensure_exists:
            Should the base directory be created automatically?
            Defaults to true.
        """
        self.base = Path(base)
        mkdir(self.base, ensure_exists=ensure_exists)

    @classmethod
    def from_key(cls, key: str, *subkeys: str, ensure_exists: bool = True) -> 'Module':
        """Get a module for the given directory or one of its subdirectories."""
        base = get_base(key, ensure_exists=False)
        rv = cls(base=base, ensure_exists=ensure_exists)
        if subkeys:
            rv = rv.submodule(*subkeys, ensure_exists=ensure_exists)
        return rv

    def submodule(self, *subkeys: str, ensure_exists: bool = True) -> 'Module':
        """Get a module for a subdirectory of the current module.

        :param subkeys:
            A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param ensure_exists:
            Should all directories be created automatically?
            Defaults to true.
        :return:
            A module representing the subdirectory based on the given ``subkeys``.
        """
        base = self.join(*subkeys, ensure_exists=False)
        return Module(base=base, ensure_exists=ensure_exists)

    def join(
        self,
        *subkeys: str,
        name: Optional[str] = None,
        ensure_exists: bool = True,
    ) -> Path:
        """Get a subdirectory of the current module.

        :param subkeys:
            A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param ensure_exists:
            Should all directories be created automatically?
            Defaults to true.
        :param name:
            The name of the file (optional) inside the folder
        :return:
            The path of the directory or subdirectory for the given module.
        """
        rv = self.base
        if subkeys:
            rv = rv.joinpath(*subkeys)
            mkdir(rv, ensure_exists=ensure_exists)
        if name:
            rv = rv.joinpath(name)
        return rv

    def get(self, *args, **kwargs):
        """Get a subdirectory of the current module, deprecated in favor of :meth:`join`."""
        warnings.warn('Use Module.join instead of Module.get', DeprecationWarning)
        return self.join(*args, **kwargs)

    def ensure(
        self,
        *subkeys: str,
        url: str,
        name: Optional[str] = None,
        force: bool = False,
        download_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Path:
        """Ensure a file is downloaded.

        :param subkeys:
            A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param url:
            The URL to download.
        :param name:
            Overrides the name of the file at the end of the URL, if given. Also
            useful for URLs that don't have proper filenames with extensions.
        :param force:
            Should the download be done again, even if the path already exists?
            Defaults to false.
        :param download_kwargs: Keyword arguments to pass through to :func:`pystow.utils.download`.
        :return:
            The path of the file that has been downloaded (or already exists)
        """
        if name is None:
            name = name_from_url(url)
        directory = self.join(*subkeys, name=name, ensure_exists=True)
        download(
            url=url,
            path=path,
            force=force,
            **(download_kwargs or {}),
        )
        return path

    def ensure_csv(
        self,
        *subkeys: str,
        url: str,
        name: Optional[str] = None,
        force: bool = False,
        download_kwargs: Optional[Mapping[str, Any]] = None,
        read_csv_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> pd.DataFrame:
        """Download a CSV and open as a dataframe with :mod:`pandas`."""
        path = self.ensure(*subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs)
        return pd.read_csv(path, **_clean_csv_kwargs(read_csv_kwargs))

    def ensure_excel(
        self,
        *subkeys: str,
        url: str,
        name: Optional[str] = None,
        force: bool = False,
        download_kwargs: Optional[Mapping[str, Any]] = None,
        read_excel_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> pd.DataFrame:
        """Download an excel file and open as a dataframe with :mod:`pandas`."""
        path = self.ensure(*subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs)
        return pd.read_excel(path, **(read_excel_kwargs or {}))

    def ensure_tar_df(
        self,
        *subkeys: str,
        url: str,
        inner_path: str,
        name: Optional[str] = None,
        force: bool = False,
        sep: str = '\t',
        download_kwargs: Optional[Mapping[str, Any]] = None,
        read_csv_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> pd.DataFrame:
        """Download a tar file and open an inner file as a dataframe with :mod:`pandas`."""
        path = self.ensure(*subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs)
        return read_tarfile_csv(path=path, inner_path=inner_path, sep=sep, **_clean_csv_kwargs(read_csv_kwargs))

    def ensure_zip_df(
        self,
        *subkeys: str,
        url: str,
        inner_path: str,
        name: Optional[str] = None,
        force: bool = False,
        download_kwargs: Optional[Mapping[str, Any]] = None,
        read_csv_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> pd.DataFrame:
        """Download a zip file and open an inner file as a dataframe with :mod:`pandas`."""
        path = self.ensure(*subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs)
        return read_zipfile_csv(path=path, inner_path=inner_path, **_clean_csv_kwargs(read_csv_kwargs))

    def ensure_rdf(
        self,
        *subkeys: str,
        url: str,
        name: Optional[str] = None,
        force: bool = False,
        download_kwargs: Optional[Mapping[str, Any]] = None,
        precache: bool = True,
        parse_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> 'rdflib.Graph':
        """Download a RDF file and open with :mod:`rdflib`."""
        path = self.ensure(*subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs)
        if not precache:
            return read_rdf(path=path, **(parse_kwargs or {}))

        cache_path = path.with_suffix(path.suffix + '.pickle.gz')
        if cache_path.exists() and not force:
            with gzip.open(cache_path, 'rb') as file:
                return pickle.load(file)  # type: ignore

        rv = read_rdf(path=path, **(parse_kwargs or {}))
        with gzip.open(cache_path, 'wb') as file:
            pickle.dump(rv, file, protocol=pickle.HIGHEST_PROTOCOL)  # type: ignore
        return rv


def _clean_csv_kwargs(read_csv_kwargs):
    if read_csv_kwargs is None:
        read_csv_kwargs = {}
    read_csv_kwargs.setdefault('sep', '\t')
    return read_csv_kwargs
