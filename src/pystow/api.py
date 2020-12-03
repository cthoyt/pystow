# -*- coding: utf-8 -*-

"""API functions for PyStow."""

import os
from pathlib import Path

PYSTOW_NAME_ENVVAR = 'PYSTOW_NAME'
PYSTOW_HOME_ENVVAR = 'PYSTOW_HOME'
PYSTOW_NAME_DEFAULT = '.data'


def get_name() -> str:
    """Get the PyStow home directory name."""
    return os.getenv(PYSTOW_NAME_ENVVAR) or PYSTOW_NAME_DEFAULT


def _env_or_default(envvar: str, default: Path) -> Path:
    return Path(os.getenv(envvar) or default)


def get_home(ensure_exists: bool = True) -> Path:
    """Get the PyStow home directory."""
    default = Path.home() / get_name()
    rv = _env_or_default(PYSTOW_HOME_ENVVAR, default)
    if ensure_exists:
        rv.mkdir(exist_ok=True, parents=True)
    return rv


def _assert_valid(key: str) -> None:
    if '.' in key:
        raise ValueError


def get(key: str, *subkeys: str, ensure_exists: bool = True) -> Path:
    """Return the home data directory for the given module.

    :param key: The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys: A sequence of additional strings to join
    :param ensure_exists: Should all directories be created automatically?
        Defaults to true.
    :return: The path of the directory or subdirectory for the given module.
    """
    _assert_valid(key)
    envvar = f'{key.upper()}_HOME'
    default = get_home() / key
    rv = _env_or_default(envvar, default)
    if ensure_exists:
        rv.mkdir(exist_ok=True, parents=True)

    for subkey in subkeys:
        rv = rv / subkey
        if ensure_exists:
            rv.mkdir(exist_ok=True, parents=True)

    return rv
