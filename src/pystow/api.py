# -*- coding: utf-8 -*-

"""API functions for PyStow."""

import warnings
from pathlib import Path
from typing import Any, Mapping, Optional, TYPE_CHECKING

import pandas as pd

from .module import Module

if TYPE_CHECKING:
    import rdflib

__all__ = [
    'module',
    'join',
    'get',
    'ensure',
    'ensure_csv',
    'ensure_excel',
    'ensure_tar_df',
    'ensure_zip_df',
]


def module(key: str, *subkeys: str, ensure_exists: bool = True) -> Module:
    """Return a module for the application.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys:
        A sequence of additional strings to join. If none are given,
        returns the directory for this module.
    :param ensure_exists:
        Should all directories be created automatically?
        Defaults to true.
    :return:
        The module object that manages getting and ensuring
    """
    return Module.from_key(key, *subkeys, ensure_exists=ensure_exists)


def join(key: str, *subkeys: str, name: Optional[str] = None, ensure_exists: bool = True) -> Path:
    """Return the home data directory for the given module.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys:
        A sequence of additional strings to join
    :param name:
        The name of the file (optional) inside the folder
    :param ensure_exists:
        Should all directories be created automatically?
        Defaults to true.
    :return:
        The path of the directory or subdirectory for the given module.
    """
    _module = Module.from_key(key, ensure_exists=ensure_exists)
    return _module.join(*subkeys, name=name, ensure_exists=ensure_exists)


def get(*args, **kwargs):
    """Get a subdirectory of the current module, deprecated in favor of :func:`join`."""
    warnings.warn('Use pystow.join instead of pystow.get', DeprecationWarning)
    return join(*args, **kwargs)


def ensure(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
) -> Path:
    """Ensure a file is downloaded.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
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
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure(*subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs)


def ensure_csv(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_csv_kwargs: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    """Download a CSV and open as a dataframe with :mod:`pandas`."""
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_csv(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        read_csv_kwargs=read_csv_kwargs,
    )


def ensure_excel(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_excel_kwargs: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    """Download an excel file and open as a dataframe with :mod:`pandas`."""
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
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_csv_kwargs: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    """Download a tar file and open an inner file as a dataframe with :mod:`pandas`."""
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


def ensure_zip_df(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_csv_kwargs: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    """Download a zip file and open an inner file as a dataframe with :mod:`pandas`."""
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


def ensure_rdf(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    precache: bool = True,
    parse_kwargs: Optional[Mapping[str, Any]] = None,
) -> 'rdflib.Graph':
    """Download a RDF file and open with :mod:`rdflib`."""
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
