# -*- coding: utf-8 -*-

"""API functions for PyStow."""

from pathlib import Path
from typing import Optional

import pandas as pd

from .module import Module
from .utils import read_tarfile_csv, read_zipfile_csv

__all__ = [
    'module',
    'get',
    'ensure',
    'ensure_csv',
    'ensure_excel',
    'ensure_tar_df',
    'ensure_zip_df',
]


def module(key: str, ensure_exists: bool = True) -> Module:
    """Return a module for the application.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param ensure_exists:
        Should all directories be created automatically?
        Defaults to true.
    :return:
        The module object that manages getting and ensuring
    """
    return Module.from_key(key, ensure_exists=ensure_exists)


def get(key: str, *subkeys: str, ensure_exists: bool = True) -> Path:
    """Return the home data directory for the given module.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys:
        A sequence of additional strings to join
    :param ensure_exists:
        Should all directories be created automatically?
        Defaults to true.
    :return:
        The path of the directory or subdirectory for the given module.
    """
    _module = Module.from_key(key, ensure_exists=ensure_exists)
    return _module.get(*subkeys, ensure_exists=ensure_exists)


def ensure(key: str, *subkeys: str, url: str, name: Optional[str] = None, force: bool = False) -> Path:
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
    :return:
        The path of the file that has been downloaded (or already exists)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure(*subkeys, url=url, name=name, force=force)


def ensure_csv(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    sep: str = '\t',
    **kwargs,
) -> pd.DataFrame:
    """Download a CSV and open as a dataframe with :mod:`pandas`."""
    path = ensure(key, *subkeys, url=url, name=name, force=force)
    return pd.read_csv(path, sep=sep, **kwargs)


def ensure_excel(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    **kwargs,
) -> pd.DataFrame:
    """Download an excel file and open as a dataframe with :mod:`pandas`."""
    path = ensure(key, *subkeys, url=url, name=name, force=force)
    return pd.read_excel(path, **kwargs)


def ensure_tar_df(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: Optional[str] = None,
    force: bool = False,
    sep: str = '\t',
    **kwargs,
) -> pd.DataFrame:
    """Download a tar file and open an inner file as a dataframe with :mod:`pandas`."""
    path = ensure(key, *subkeys, url=url, name=name, force=force)
    return read_tarfile_csv(path=path, inner_path=inner_path, sep=sep, **kwargs)


def ensure_zip_df(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: Optional[str] = None,
    force: bool = False,
    sep: str = '\t',
    **kwargs,
) -> pd.DataFrame:
    """Download a zip file and open an inner file as a dataframe with :mod:`pandas`."""
    path = ensure(key, *subkeys, url=url, name=name, force=force)
    return read_zipfile_csv(path=path, inner_path=inner_path, sep=sep, **kwargs)
