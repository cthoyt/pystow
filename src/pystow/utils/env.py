"""Environment utilities."""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

from ..constants import (
    PYSTOW_HOME_ENVVAR,
    PYSTOW_NAME_DEFAULT,
    PYSTOW_NAME_ENVVAR,
    PYSTOW_USE_APPDIRS,
)

__all__ = [
    "get_base",
    "get_home",
    "get_name",
    "getenv_path",
    "mkdir",
    "mock_envvar",
    "mock_home",
    "use_appdirs",
]


def mkdir(path: Path, ensure_exists: bool = True) -> None:
    """Make a directory (or parent directory if a file is given) if flagged with ``ensure_exists``.

    :param path: The path to a directory
    :param ensure_exists: Should the directories leading to the path be created if they
        don't already exist?
    """
    if ensure_exists:
        path.mkdir(exist_ok=True, parents=True)


@contextlib.contextmanager
def mock_envvar(envvar: str, value: str) -> Iterator[None]:
    """Mock the environment variable then delete it after the test is over.

    :param envvar: The environment variable to mock
    :param value: The value to temporarily put in the environment variable during this
        mock.

    :yield: None, since this just mocks the environment variable for the time being.
    """
    original_value = os.environ.get(envvar)
    os.environ[envvar] = value
    yield
    if original_value is None:
        del os.environ[envvar]
    else:
        os.environ[envvar] = original_value


@contextlib.contextmanager
def mock_home() -> Iterator[Path]:
    """Mock the PyStow home environment variable, yields the directory name.

    :yield: The path to the temporary directory.
    """
    with tempfile.TemporaryDirectory() as directory:
        with mock_envvar(PYSTOW_HOME_ENVVAR, directory):
            yield Path(directory)


def getenv_path(envvar: str, default: Path, ensure_exists: bool = True) -> Path:
    """Get an environment variable representing a path, or use the default.

    :param envvar: The environmental variable name to check
    :param default: The default path to return if the environmental variable is not set
    :param ensure_exists: Should the directories leading to the path be created if they
        don't already exist?

    :returns: A path either specified by the environmental variable or by the default.
    """
    rv = Path(os.getenv(envvar, default=default)).expanduser()
    mkdir(rv, ensure_exists=ensure_exists)
    return rv


def use_appdirs() -> bool:
    """Check if X Desktop Group (XDG) compatibility is requested.

    :returns: If the :data:`PYSTOW_USE_APPDIRS` is set to ``true`` in the environment.
    """
    return os.getenv(PYSTOW_USE_APPDIRS) in {"true", "True"}


def get_home(ensure_exists: bool = True) -> Path:
    """Get the PyStow home directory.

    :param ensure_exists: If true, ensures the directory is created

    :returns: A path object representing the pystow home directory, as one of:

        1. :data:`PYSTOW_HOME_ENVVAR` environment variable or
        2. The user data directory defined by :mod:`appdirs` or :mod:`platformdirs` if
           the :data:`PYSTOW_USE_APPDIRS` environment variable is set to ``true`` or
        3. The default directory constructed in the user's home directory plus what's
           returned by :func:`get_name`.
    """
    if use_appdirs():
        try:
            from platformdirs import user_data_dir
        except ImportError:
            from appdirs import user_data_dir

        default = Path(user_data_dir())
    else:
        default = Path.home() / get_name()
    return getenv_path(PYSTOW_HOME_ENVVAR, default, ensure_exists=ensure_exists)


def get_base(key: str, ensure_exists: bool = True) -> Path:
    """Get the base directory for a module.

    :param key: The name of the module. No funny characters. The envvar <key>_HOME where
        key is uppercased is checked first before using the default home directory.
    :param ensure_exists: Should all directories be created automatically? Defaults to
        true.

    :returns: The path to the given

    :raises ValueError: if the key is invalid (e.g., has a dot in it)
    """
    if "." in key:
        raise ValueError(f"The module should not have a dot in it: {key}")
    envvar = f"{key.upper()}_HOME"
    if use_appdirs():
        try:
            from platformdirs import user_data_dir
        except ImportError:
            from appdirs import user_data_dir

        default = Path(user_data_dir(appname=key))
    else:
        default = get_home(ensure_exists=False) / key
    return getenv_path(envvar, default, ensure_exists=ensure_exists)


def get_name() -> str:
    """Get the PyStow home directory name.

    :returns: The name of the pystow home directory, either loaded from the
        :data:`PYSTOW_NAME_ENVVAR`` environment variable or given by the default value
        :data:`PYSTOW_NAME_DEFAULT`.
    """
    return os.getenv(PYSTOW_NAME_ENVVAR, default=PYSTOW_NAME_DEFAULT)
