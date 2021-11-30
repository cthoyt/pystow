# -*- coding: utf-8 -*-

"""API functions for PyStow."""

from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union

from .module import Module

__all__ = [
    "module",
    "join",
    # Downloader functions
    "ensure",
    "ensure_untar",
    # Processors
    "ensure_csv",
    "ensure_json",
    "ensure_excel",
    "ensure_tar_df",
    "ensure_tar_xml",
    "ensure_zip_df",
    "ensure_from_s3",
    "ensure_from_google",
    "ensure_rdf",
]


def module(key: str, *subkeys: str, ensure_exists: bool = True) -> Module:
    """Return a module for the application.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys:
        A sequence of additional strings to join. If none are given,
        returns the directory for this module.
    :param ensure_exists:
        Should all directories be created automatically?
        Defaults to true.
    :return:
        The module object that manages getting and ensuring
    """
    return Module.from_key(key, *subkeys, ensure_exists=ensure_exists)


def join(key: str, *subkeys: str, name: Optional[str] = None, ensure_exists: bool = True) -> Path:
    """Return the home data directory for the given module.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys:
        A sequence of additional strings to join
    :param name:
        The name of the file (optional) inside the folder
    :param ensure_exists:
        Should all directories be created automatically?
        Defaults to true.
    :return:
        The path of the directory or subdirectory for the given module.
    """
    _module = Module.from_key(key, ensure_exists=ensure_exists)
    return _module.join(*subkeys, name=name, ensure_exists=ensure_exists)


def ensure(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
) -> Path:
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
    :param download_kwargs: Keyword arguments to pass through to :func:`pystow.utils.download`.
    :return:
        The path of the file that has been downloaded (or already exists)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure(
        *subkeys, url=url, name=name, force=force, download_kwargs=download_kwargs
    )


def ensure_untar(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    directory: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    extract_kwargs: Optional[Mapping[str, Any]] = None,
) -> Path:
    """Ensure a file is downloaded and untarred.

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
    :param directory:
        Overrides the name of the directory into which the tar archive is extracted.
        If none given, will use the stem of the file name that gets downloaded.
    :param force:
        Should the download be done again, even if the path already exists?
        Defaults to false.
    :param download_kwargs: Keyword arguments to pass through to :func:`pystow.utils.download`.
    :param extract_kwargs: Keyword arguments to pass to :meth:`tarfile.TarFile.extract_all`.
    :return:
        The path of the directory where the file that has been downloaded
        gets extracted to
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_untar(
        *subkeys,
        url=url,
        name=name,
        directory=directory,
        force=force,
        download_kwargs=download_kwargs,
        extract_kwargs=extract_kwargs,
    )


def ensure_csv(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_csv_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Download a CSV and open as a dataframe with :mod:`pandas`.

    :param key: The module name
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
    :param download_kwargs: Keyword arguments to pass through to :func:`pystow.utils.download`.
    :param read_csv_kwargs: Keyword arguments to pass through to :func:`pandas.read_csv`.
    :return: A pandas DataFrame
    :rtype: pandas.DataFrame

    Example usage::

    >>> import pystow
    >>> import pandas as pd
    >>> url = 'https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt'
    >>> df: pd.DataFrame = pystow.ensure_csv('pykeen', 'datasets', 'nations', url=url)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_csv(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        read_csv_kwargs=read_csv_kwargs,
    )


def ensure_json(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    json_load_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Download JSON and open with :mod:`json`.

    :param key: The module name
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
    :param download_kwargs: Keyword arguments to pass through to :func:`pystow.utils.download`.
    :param json_load_kwargs: Keyword arguments to pass through to :func:`json.load`.
    :returns: A JSON object (list, dict, etc.)

    Example usage::

    >>> import pystow
    >>> url = 'https://maayanlab.cloud/CREEDS/download/single_gene_perturbations-v1.0.json'
    >>> perturbations = pystow.ensure_csv('bio', 'creeds', '1.0', url=url)
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_json(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        json_load_kwargs=json_load_kwargs,
    )


def ensure_excel(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_excel_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Download an excel file and open as a dataframe with :mod:`pandas`.

    :param key: The module name
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
    :param download_kwargs: Keyword arguments to pass through to :func:`pystow.utils.download`.
    :param read_excel_kwargs: Keyword arguments to pass through to :func:`pandas.read_excel`.
    :return: A pandas DataFrame
    :rtype: pandas.DataFrame
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_excel(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        read_excel_kwargs=read_excel_kwargs,
    )


def ensure_tar_df(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_csv_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Download a tar file and open an inner file as a dataframe with :mod:`pandas`."""
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_tar_df(
        *subkeys,
        url=url,
        name=name,
        force=force,
        inner_path=inner_path,
        download_kwargs=download_kwargs,
        read_csv_kwargs=read_csv_kwargs,
    )


def ensure_tar_xml(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    parse_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Download a tar file and open an inner XML file with :mod:`lxml`."""
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_tar_xml(
        *subkeys,
        url=url,
        name=name,
        force=force,
        inner_path=inner_path,
        download_kwargs=download_kwargs,
        parse_kwargs=parse_kwargs,
    )


def ensure_zip_df(
    key: str,
    *subkeys: str,
    url: str,
    inner_path: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    read_csv_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Download a zip file and open an inner file as a dataframe with :mod:`pandas`."""
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_zip_df(
        *subkeys,
        url=url,
        name=name,
        force=force,
        inner_path=inner_path,
        download_kwargs=download_kwargs,
        read_csv_kwargs=read_csv_kwargs,
    )


def ensure_rdf(
    key: str,
    *subkeys: str,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    download_kwargs: Optional[Mapping[str, Any]] = None,
    precache: bool = True,
    parse_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Download a RDF file and open with :mod:`rdflib`.

    :param key: The module name
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
    :param download_kwargs: Keyword arguments to pass through to :func:`pystow.utils.download`.
    :param precache: Should the parsed :class:`rdflib.Graph` be stored as a pickle for fast loading?
    :param parse_kwargs:
        Keyword arguments to pass through to :func:`pystow.utils.read_rdf` and transitively to
        :func:`rdflib.Graph.parse`.
    :return: An RDF graph
    :rtype: rdflib.Graph

    Example usage::

    >>> import pystow
    >>> import rdflib
    >>> url = 'https://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz'
    >>> rdf_graph: rdflib.Graph = pystow.ensure_rdf('rhea', url=url)

    If :mod:`rdflib` fails to guess the format, you can explicitly specify it using the `parse_kwargs` argument:

    >>> import pystow
    >>> import rdflib
    >>> url = "http://oaei.webdatacommons.org/tdrs/testdata/persistent/knowledgegraph" \
    ... "/v3/suite/memoryalpha-stexpanded/component/reference.xml"
    >>> rdf_graph: rdflib.Graph = pystow.ensure_rdf("memoryalpha-stexpanded", url=url, parse_kwargs={"format": "xml"})
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_rdf(
        *subkeys,
        url=url,
        name=name,
        force=force,
        download_kwargs=download_kwargs,
        precache=precache,
        parse_kwargs=parse_kwargs,
    )


def ensure_from_s3(
    key: str,
    *subkeys: str,
    s3_bucket: str,
    s3_key: Union[str, Sequence[str]],
    name: Optional[str] = None,
    force: bool = False,
) -> Path:
    """Ensure a file is downloaded.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys:
        A sequence of additional strings to join. If none are given,
        returns the directory for this module.
    :param s3_bucket:
        The S3 bucket name
    :param s3_key:
        The S3 key name
    :param name:
        Overrides the name of the file at the end of the S3 key, if given.
    :param force:
        Should the download be done again, even if the path already exists?
        Defaults to false.
    :return:
        The path of the file that has been downloaded (or already exists)

    Example downloading ProtMapper 0.0.21:

    >>> version = '0.0.21'
    >>> ensure_from_s3('test', version, s3_bucket='bigmech', s3_key=f'protmapper/{version}/refseq_uniprot.csv')
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_from_s3(
        *subkeys, s3_bucket=s3_bucket, s3_key=s3_key, name=name, force=force
    )


def ensure_from_google(
    key: str,
    *subkeys: str,
    name: str,
    file_id: str,
    force: bool = False,
) -> Path:
    """Ensure a file is downloaded from google drive.

    :param key:
        The name of the module. No funny characters. The envvar
        <key>_HOME where key is uppercased is checked first before using
        the default home directory.
    :param subkeys:
        A sequence of additional strings to join. If none are given,
        returns the directory for this module.
    :param name:
        The name of the file
    :param file_id:
        The file identifier of the google file. If your share link is
        https://drive.google.com/file/d/1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z/view, then your file id is
        ``1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z``.
    :param force:
        Should the download be done again, even if the path already exists?
        Defaults to false.
    :return:
        The path of the file that has been downloaded (or already exists)

    Example downloading the WK3l-15k dataset as motivated by
    https://github.com/pykeen/pykeen/pull/403:

    >>> ensure_from_google('test', name='wk3l15k.zip', file_id='1AsPPU4ka1Rc9u-XYMGWtvV65hF3egi0z')
    """
    _module = Module.from_key(key, ensure_exists=True)
    return _module.ensure_from_google(*subkeys, name=name, file_id=file_id, force=force)
