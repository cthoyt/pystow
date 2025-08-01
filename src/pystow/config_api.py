"""Configuration handling."""

from __future__ import annotations

import os
from configparser import ConfigParser
from functools import lru_cache
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, TypeVar

from .utils import getenv_path

__all__ = [
    "get_config",
    "write_config",
]

X = TypeVar("X")

CONFIG_NAME_ENVVAR = "PYSTOW_CONFIG_NAME"
CONFIG_HOME_ENVVAR = "PYSTOW_CONFIG_HOME"
CONFIG_NAME_DEFAULT = ".config"


class ConfigError(ValueError):
    """Raised when configuration can not be looked up."""

    def __init__(self, module: str, key: str):
        """Initialize the configuration error.

        :param module: Name of the module, e.g., ``bioportal``
        :param key: Name of the key inside the module, e.g., ``api_key``
        """
        self.module = module
        self.key = key

    def __str__(self) -> str:
        path = get_home().joinpath(self.module).with_suffix(".ini")
        return dedent(
            f"""\
        Could not look up {self.module}/{self.key} and no default given.

        This can be solved with one of the following:

        1. Set the {self.module.upper()}_{self.key.upper()} environment variable

           - Windows, via GUI: https://www.computerhope.com/issues/ch000549.htm
           - Windows, via CLI: https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/set_1
           - Mac OS: https://apple.stackexchange.com/questions/106778/how-do-i-set-environment-variables-on-os-x
           - Linux: https://www.freecodecamp.org/news/how-to-set-an-environment-variable-in-linux/

        2. Use the PyStow CLI from the command line to
           set the configuration like so:

           $ pystow set {self.module} {self.key} <value>

           This creates an INI file in {path}
           with the configuration in the right place.

        3. Create/edit an INI file in {path} and manually
           fill it in by 1) creating a section inside it called [{self.module}]
           and 2) setting a value for {self.key} = <value> that looks like:

           # {path}
           [{self.module}]
           {self.key} = <value>

        See https://github.com/cthoyt/pystow#%EF%B8%8F%EF%B8%8F-configuration for more information.
        """
        )


def get_name() -> str:
    """Get the config home directory name.

    :returns: The name of the pystow home directory, either loaded from the
        :data:`CONFIG_NAME_ENVVAR`` environment variable or given by the default value
        :data:`CONFIG_NAME_DEFAULT`.
    """
    return os.getenv(CONFIG_NAME_ENVVAR, default=CONFIG_NAME_DEFAULT)


def get_home(ensure_exists: bool = True) -> Path:
    """Get the config home directory.

    :param ensure_exists: If true, ensures the directory is created

    :returns: A path object representing the pystow home directory, as one of:

        1. :data:`CONFIG_HOME_ENVVAR` environment variable or
        2. The default directory constructed in the user's home directory plus what's
           returned by :func:`get_name`.
    """
    default = Path.home().joinpath(get_name()).expanduser()
    return getenv_path(CONFIG_HOME_ENVVAR, default, ensure_exists=ensure_exists)


@lru_cache(maxsize=1)
def _get_cfp(module: str) -> ConfigParser:
    cfp = ConfigParser()
    directory = get_home()

    # If a multi-part module was given like "zenodo:sandbox",
    # then only look for the first part "zenodo" as the file name
    if ":" in module:
        module = module.split(":", 1)[0]

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
    passthrough: X | None = None,
    default: X | None = None,
    dtype: type[X] | None = None,
    raise_on_missing: bool = False,
) -> Any:
    """Get a configuration value.

    :param module: Name of the module (e.g., ``pybel``) to get configuration for
    :param key: Name of the key (e.g., ``connection``)
    :param passthrough: If this is not none, will get returned
    :param default: If the environment and configuration files don't contain anything,
        this is returned.
    :param dtype: The datatype to parse out. Can either be :func:`int`, :func:`float`,
        :func:`bool`, or :func:`str`. If none, defaults to :func:`str`.
    :param raise_on_missing: If true, will raise a value error if no data is found and
        no default is given

    :returns: The config value or the default.

    :raises ConfigError: If ``raise_on_missing`` conditions are met
    """
    if passthrough is not None:
        return _cast(passthrough, dtype)
    rv = os.getenv(f"{module.upper()}_{key.upper()}")
    if rv is not None:
        return _cast(rv, dtype)
    rv = _get_cfp(module).get(module, key, fallback=None)
    if rv is None:
        if default is None and raise_on_missing:
            raise ConfigError(module=module, key=key)
        return default
    return _cast(rv, dtype)


def _cast(rv: Any, dtype: None | Callable[..., Any]) -> Any:
    if not isinstance(rv, str):  # if it's not a string, it doesn't need munging
        return rv
    if dtype in (None, str):  # no munging necessary
        return rv
    if dtype in (int, float):
        return dtype(rv)
    if dtype is bool:
        if rv.lower() in ("t", "true", "yes", "1", 1, True):
            return True
        elif rv.lower() in ("f", "false", "no", "0", 0, False):
            return False
        else:
            raise ValueError(f"value can not be coerced into bool: {rv}")
    raise TypeError(f"dtype is invalid: {dtype}")


def write_config(module: str, key: str, value: str) -> None:
    """Write a configuration value.

    :param module: The name of the app (e.g., ``indra``)
    :param key: The key of the configuration in the app
    :param value: The value of the configuration in the app
    """
    _get_cfp.cache_clear()
    cfp = ConfigParser()

    # If there's a multi-part module such as "zenodo:sandbox",
    # then write to zenodo.ini with section [zenodo:sandbox]
    fname = module.split(":", 1)[0] if ":" in module else module

    path = get_home().joinpath(fname).with_suffix(".ini")
    cfp.read(path)

    # If the file did not exist, then this section will be empty
    # and running set() would raise a configparser.NoSectionError.
    if not cfp.has_section(module):
        cfp.add_section(module)

    # Note that the section duplicates the file name
    cfp.set(section=module, option=key, value=value)

    with path.open("w") as file:
        cfp.write(file)
