# -*- coding: utf-8 -*-

"""Utilities."""

import contextlib
import os
import tarfile
import zipfile
from io import BytesIO, StringIO
from pathlib import Path, PurePosixPath
from typing import Union
from urllib.parse import urlparse
from uuid import uuid4

import pandas as pd


def name_from_url(url: str) -> str:
    """Get the filename from the end of the URL."""
    parse_result = urlparse(url)
    path = PurePosixPath(parse_result.path)
    name = path.name
    return name


def mkdir(path: Path, ensure_exists: bool = True) -> None:
    """Make a directory (or parent directory if a file is given) if flagged with ``ensure_exists``."""
    if ensure_exists:
        if path.suffix:  # if it looks like a file path
            path.parent.mkdir(exist_ok=True, parents=True)
        else:
            path.mkdir(exist_ok=True, parents=True)


@contextlib.contextmanager
def mock_envvar(k: str, v: str):
    """Mock the environment variable then delete it after the test is over."""
    os.environ[k] = v
    yield
    del os.environ[k]


def getenv_path(envvar: str, default: Path, ensure_exists: bool = True) -> Path:
    """Get an environment variable representing a path, or use the default."""
    rv = Path(os.getenv(envvar, default=default))
    mkdir(rv, ensure_exists=ensure_exists)
    return rv


def n() -> str:
    """Get a random string for testing."""
    return str(uuid4())


def get_df_io(df: pd.DataFrame, sep: str = '\t', index: bool = False, **kwargs) -> BytesIO:
    """Get the dataframe as bytes."""
    sio = StringIO()
    df.to_csv(sio, sep=sep, index=index, **kwargs)
    sio.seek(0)
    bio = BytesIO(sio.read().encode('utf-8'))
    return bio


def write_zipfile_csv(
    df: pd.DataFrame,
    path: Union[str, Path],
    inner_path: str, sep='\t',
    index: bool = False,
    **kwargs,
) -> None:
    """Write a dataframe to an inner CSV file to a zip archive."""
    bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    with zipfile.ZipFile(file=path, mode='w') as zip_file:
        with zip_file.open(inner_path, mode='w') as file:
            file.write(bytes_io.read())


def read_zipfile_csv(path: Union[str, Path], inner_path: str, sep='\t', **kwargs) -> pd.DataFrame:
    """Read an inner CSV file from a zip archive."""
    with zipfile.ZipFile(file=path) as zip_file:
        with zip_file.open(inner_path) as file:
            return pd.read_csv(file, sep=sep, **kwargs)


def write_tarfile_csv(
    df: pd.DataFrame,
    path: Union[str, Path],
    inner_path: str,
    sep='\t',
    index: bool = False,
    **kwargs,
) -> None:
    """Write a dataframe to an inner CSV file from a tar archive."""
    raise NotImplementedError
    # bytes_io = get_df_io(df, sep=sep, index=index, **kwargs)
    # with tarfile.open(path, mode='w') as tar_file:
    #    with tar_file.open(inner_path, mode='w') as file:  # type: ignore
    #        file.write(bytes_io.read())


def read_tarfile_csv(path: Union[str, Path], inner_path: str, sep='\t', **kwargs) -> pd.DataFrame:
    """Read an inner CSV file from a tar archive."""
    with tarfile.open(path) as tar_file:
        with tar_file.extractfile(inner_path) as file:  # type: ignore
            return pd.read_csv(file, sep=sep, **kwargs)
