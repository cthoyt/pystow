# -*- coding: utf-8 -*-

"""PyStow: Easily pick a place to store data for your python package."""

from .api import (  # noqa
    ensure,
    ensure_csv,
    ensure_excel,
    ensure_from_google,
    ensure_from_s3,
    ensure_json,
    ensure_open,
    ensure_open_gz,
    ensure_open_lzma,
    ensure_open_tarfile,
    ensure_open_zip,
    ensure_pickle,
    ensure_rdf,
    ensure_tar_df,
    ensure_tar_xml,
    ensure_untar,
    ensure_zip_df,
    ensure_zip_np,
    join,
    joinpath_sqlite,
    module,
    open,
    open_csv,
    open_json,
    open_pickle,
    submodule,
)
from .config_api import get_config, write_config  # noqa
from .impl import Module  # noqa
from .utils import ensure_readme  # noqa

ensure_readme()
