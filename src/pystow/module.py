# -*- coding: utf-8 -*-

"""Module implementation."""

import logging
import os
from pathlib import Path
from typing import Optional, Union

from .utils import download, getenv_path, mkdir, name_from_url

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
        base = self.get(*subkeys, ensure_exists=False)
        return Module(base=base, ensure_exists=ensure_exists)

    def get(self, *subkeys: str, ensure_exists: bool = True) -> Path:
        """Get a subdirectory of the current module.

        :param subkeys:
            A sequence of additional strings to join. If none are given,
            returns the directory for this module.
        :param ensure_exists:
            Should all directories be created automatically?
            Defaults to true.
        :return:
            The path of the directory or subdirectory for the given module.
        """
        rv = self.base
        if subkeys:
            rv = rv.joinpath(*subkeys)
        mkdir(rv, ensure_exists=ensure_exists)
        return rv

    def ensure(
        self,
        *subkeys: str,
        url: str,
        name: Optional[str] = None,
        force: bool = False,
        **kwargs,
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
        :param kwargs: Keyword arguments to pass through to :func:`better_urlretrieve`.
        :return:
            The path of the file that has been downloaded (or already exists)
        """
        if name is None:
            name = name_from_url(url)
        directory = self.get(*subkeys, ensure_exists=True)
        path = directory / name
        if not path.exists() or force:
            logger.info('downloading data from %s to %s', url, path)
            download(
                url=url,
                path=path,
                **kwargs,
            )
        return path
