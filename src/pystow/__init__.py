# -*- coding: utf-8 -*-

"""PyStow: Easily pick a place to store data for your python package."""

from .api import (  # noqa
    ensure, ensure_csv, ensure_excel, ensure_from_google, ensure_from_s3, ensure_rdf, ensure_tar_df, ensure_tar_xml,
    ensure_untar, ensure_zip_df, get, join, module,
)
from .config_api import get_config, write_config  # noqa
from .module import Module, ensure_readme  # noqa

ensure_readme()
