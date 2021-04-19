# -*- coding: utf-8 -*-

"""Configuration handling."""

import os
from configparser import ConfigParser
from functools import lru_cache
from pathlib import Path
from typing import Optional

from .utils import getenv_path

__all__ = [
    'get_config',
    'write_config',
]

CONFIG_NAME_ENVVAR = 'PYSTOW_CONFIG_NAME'
CONFIG_HOME_ENVVAR = 'PYSTOW_CONFIG_HOME'
CONFIG_NAME_DEFAULT = '.config'


def get_name() -> str:
    """Get the config home directory name."""
    return os.getenv(CONFIG_NAME_ENVVAR, default=CONFIG_NAME_DEFAULT)


def get_home(ensure_exists: bool = True) -> Path:
    """Get the config home directory."""
    default = Path.home() / get_name()
    return getenv_path(CONFIG_HOME_ENVVAR, default, ensure_exists=ensure_exists)


@lru_cache(maxsize=1)
def _get_cfp(module: str) -> ConfigParser:
    cfp = ConfigParser()
    directory = get_home()
    filenames = [
        os.path.join(directory, 'config.cfg'),
        os.path.join(directory, 'config.ini'),
        os.path.join(directory, 'pystow.cfg'),
        os.path.join(directory, 'pystow.ini'),
        os.path.join(directory, f'{module}.cfg'),
        os.path.join(directory, f'{module}.ini'),
        os.path.join(directory, module, f'{module}.cfg'),
        os.path.join(directory, module, f'{module}.ini'),
        os.path.join(directory, module, 'conf.ini'),
        os.path.join(directory, module, 'config.ini'),
        os.path.join(directory, module, 'conf.cfg'),
        os.path.join(directory, module, 'config.cfg'),
    ]
    cfp.read(filenames)
    return cfp


def get_config(
    module: str,
    key: str, *,
    passthrough: Optional[str] = None,
    default: Optional[str] = None,
) -> Optional[str]:
    """Get a configuration value.

    :param module: Name of the module (e.g., ``pybel``) to get configuration for
    :param key: Name of the key (e.g., ``connection``)
    :param passthrough: If this is not none, will get returned
    :param default: If the environment and configuration files don't contain anything,
        this is returned.
    """
    if passthrough is not None:
        return passthrough
    rv = os.getenv(f'{module.upper()}_{key.upper()}')
    if rv is not None:
        return rv
    return _get_cfp(module).get(module, key, fallback=default)


def write_config(module: str, key: str, value: str) -> None:
    """Write a configuration value."""
    _get_cfp.cache_clear()
    cfp = ConfigParser()
    path = get_home() / f'{module}.ini'
    cfp.read(path)
    cfp.set(module, key, value)
    with path.open('w') as file:
        cfp.write(file)
