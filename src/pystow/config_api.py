# -*- coding: utf-8 -*-

"""Configuration handling."""

import os
from configparser import ConfigParser
from functools import lru_cache
from pathlib import Path
from typing import Optional, Type, TypeVar

from .utils import getenv_path

__all__ = [
    "get_config",
    "write_config",
]

X = TypeVar("X")

CONFIG_NAME_ENVVAR = "PYSTOW_CONFIG_NAME"
CONFIG_HOME_ENVVAR = "PYSTOW_CONFIG_HOME"
CONFIG_NAME_DEFAULT = ".config"


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
        os.path.join(directory, "config.cfg"),
        os.path.join(directory, "config.ini"),
        os.path.join(directory, "pystow.cfg"),
        os.path.join(directory, "pystow.ini"),
        os.path.join(directory, f"{module}.cfg"),
        os.path.join(directory, f"{module}.ini"),
        os.path.join(directory, module, f"{module}.cfg"),
        os.path.join(directory, module, f"{module}.ini"),
        os.path.join(directory, module, "conf.ini"),
        os.path.join(directory, module, "config.ini"),
        os.path.join(directory, module, "conf.cfg"),
        os.path.join(directory, module, "config.cfg"),
    ]
    cfp.read(filenames)
    return cfp


def get_config(
    module: str,
    key: str,
    *,
    passthrough: Optional[X] = None,
    default: Optional[X] = None,
    dtype: Optional[Type[X]] = None,
    raise_on_missing: bool = False,
):
    """Get a configuration value.

    :param module: Name of the module (e.g., ``pybel``) to get configuration for
    :param key: Name of the key (e.g., ``connection``)
    :param passthrough: If this is not none, will get returned
    :param default: If the environment and configuration files don't contain anything,
        this is returned.
    :param dtype: The datatype to parse out. Can either be :func:`int`, :func:`float`,
        :func:`bool`, or :func:`str`. If none, defaults to :func:`str`.
    :param raise_on_missing: If true, will raise a value error if no data is found and no default
        is given
    :returns: The config value or the default.
    :raises ValueError: If ``raise_on_missing`` conditions are met
    """
    if passthrough is not None:
        return _cast(passthrough, dtype)
    rv = os.getenv(f"{module.upper()}_{key.upper()}")
    if rv is not None:
        return _cast(rv, dtype)
    rv = _get_cfp(module).get(module, key, fallback=None)
    if rv is None:
        if default is None and raise_on_missing:
            raise ValueError(f"Could not look up {module}/{key} and no default given")
        return default
    return _cast(rv, dtype)


def _cast(rv, dtype):
    if not isinstance(rv, str):  # if it's not a string, it doesn't need munging
        return rv
    if dtype in (None, str):  # no munging necessary
        return rv
    if dtype in (int, float):
        return dtype(rv)  # type: ignore
    if dtype is bool:
        if rv.lower() in ("t", "true", "yes", "1", 1, True):
            return True
        elif rv.lower() in ("f", "false", "no", "0", 0, False):
            return False
        else:
            raise ValueError(f"value can not be coerced into bool: {rv}")
    raise TypeError(f"dtype is invalid: {dtype}")


def write_config(module: str, key: str, value: str) -> None:
    """Write a configuration value."""
    _get_cfp.cache_clear()
    cfp = ConfigParser()
    path = get_home() / f"{module}.ini"
    cfp.read(path)
    cfp.set(module, key, value)
    with path.open("w") as file:
        cfp.write(file)
