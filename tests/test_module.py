# -*- coding: utf-8 -*-

"""Tests for PyStow."""

import contextlib
import itertools as itt
import lzma
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Mapping, Union
from unittest import mock

import pandas as pd

import pystow
from pystow import join
from pystow.constants import PYSTOW_HOME_ENVVAR, PYSTOW_NAME_ENVVAR
from pystow.module import Module, get_home, get_name
from pystow.utils import mock_envvar, n, write_tarfile_csv, write_zipfile_csv

HERE = Path(__file__).parent.resolve()
RESOURCES = HERE.joinpath("resources")

TSV_URL = n()
JSON_URL = n()
MOCK_FILES: Mapping[str, Path] = {
    TSV_URL: RESOURCES / "test_1.tsv",
    JSON_URL: RESOURCES / "test_1.json",
}

TEST_TSV_ROWS = [
    ("h1", "h2", "h3"),
    ("v1_1", "v1_2", "v1_3"),
    ("v2_1", "v2_2", "v2_3"),
]
TEST_DF = pd.DataFrame(TEST_TSV_ROWS)


class TestMocks(unittest.TestCase):
    """Tests for :mod:`pystow` mocks and context managers."""

    def test_mock_home(self):
        """Test that home can be properly mocked."""
        name = n()

        with tempfile.TemporaryDirectory() as d:
            expected_path = Path(d) / name
            self.assertFalse(expected_path.exists())

            with mock_envvar(PYSTOW_HOME_ENVVAR, expected_path.as_posix()):
                self.assertFalse(expected_path.exists())
                self.assertEqual(expected_path, get_home(ensure_exists=False))
                self.assertFalse(expected_path.exists())

    def test_mock_name(self):
        """Test that the name can be properly mocked."""
        name = n()

        expected_path = Path.home() / name
        self.assertFalse(expected_path.exists())

        with mock_envvar(PYSTOW_NAME_ENVVAR, name):
            self.assertEqual(name, get_name())

            self.assertFalse(expected_path.exists())
            self.assertEqual(expected_path, get_home(ensure_exists=False))
            self.assertFalse(expected_path.exists())


class TestGet(unittest.TestCase):
    """Tests for :mod:`pystow`."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        """Tear down the test case."""
        self.directory.cleanup()

    @contextlib.contextmanager
    def mock_directory(self):
        """Use this test case's temporary directory as a mock environment variable."""
        with mock_envvar(PYSTOW_HOME_ENVVAR, self.directory.name):
            yield

    @staticmethod
    def mock_download():
        """Mock connection to the internet using local resource files."""

        def _mock_get_data(url: str, path: Union[str, Path], **_kwargs) -> Path:
            return shutil.copy(MOCK_FILES[url], path)

        return mock.patch("pystow.utils.download", side_effect=_mock_get_data)

    @staticmethod
    def mock_download_once(local_path):
        """Mock connection to the internet using local resource files."""

        def _mock_get_data(path: Union[str, Path], **_kwargs) -> Path:
            return shutil.copy(local_path, path)

        return mock.patch("pystow.utils.download", side_effect=_mock_get_data)

    def join(self, *parts) -> Path:
        """Help join the parts to this test case's temporary directory."""
        return Path(os.path.join(self.directory.name, *parts))

    def test_mock(self):
        """Test that mocking the directory works properly for this test case."""
        with self.mock_directory():
            self.assertEqual(os.getenv(PYSTOW_HOME_ENVVAR), self.directory.name)

    def test_get(self):
        """Test the :func:`get` function."""
        parts_examples = [
            [n()],
            [n(), n()],
            [n(), n(), n()],
        ]
        with self.mock_directory():
            for parts in parts_examples:
                with self.subTest(parts=parts):
                    self.assertEqual(self.join(*parts), join(*parts))

    def test_ensure(self):
        """Test ensuring a CSV file."""
        with self.mock_directory(), self.mock_download():
            with self.subTest(type="tsv"):
                df = pystow.ensure_csv("test", url=TSV_URL)
                self.assertEqual(3, len(df.columns))

            with self.subTest(type="json"):
                j = pystow.ensure_json("test", url=JSON_URL)
                self.assertIn("key", j)
                self.assertEqual("value", j["key"])

    def test_ensure_open_lzma(self):
        """Test opening lzma-encoded files."""
        with tempfile.TemporaryDirectory() as directory, self.mock_directory():
            path = Path(directory) / n()
            with self.mock_download_once(path):
                with lzma.open(path, "wt") as file:
                    for row in TEST_TSV_ROWS:
                        print(*row, sep="\t", file=file)  # noqa:T001
                with pystow.module("test").ensure_open_lzma(url=n()) as file:
                    df = pd.read_csv(file, sep="\t")
                    self.assertEqual(3, len(df.columns))

    def test_ensure_open_zip(self):
        """Test opening tar-encoded files."""
        with tempfile.TemporaryDirectory() as directory, self.mock_directory():
            path = Path(directory) / n()
            inner_path = n()
            with self.mock_download_once(path):
                write_zipfile_csv(TEST_DF, path, inner_path)
                with pystow.module("test").ensure_open_zip(url=n(), inner_path=inner_path) as file:
                    df = pd.read_csv(file, sep="\t")
                    self.assertEqual(3, len(df.columns))

    def test_ensure_open_tarfile(self):
        """Test opening tarfile-encoded files."""
        with tempfile.TemporaryDirectory() as directory, self.mock_directory():
            path = Path(directory) / n()
            inner_path = n()
            with self.mock_download_once(path):
                write_tarfile_csv(TEST_DF, path, inner_path)
                with pystow.module("test").ensure_open_tarfile(
                    url=n(), inner_path=inner_path
                ) as file:
                    df = pd.read_csv(file, sep="\t")
                    self.assertEqual(3, len(df.columns))

    def test_ensure_module(self):
        """Test that the ``ensure_exist`` argument in :meth:`Module.from_key` works properly."""
        parts_examples = [
            [n()],
            [n(), n()],
            [n(), n(), n()],
        ]
        ensure_examples = [False, True]

        for ensure_exists, parts in itt.product(ensure_examples, parts_examples):
            with self.subTest(ensure_exists=ensure_exists, parts=parts), self.mock_directory():
                expected_directory = self.join(*parts)

                module = Module.from_key(*parts, ensure_exists=ensure_exists)

                self.assertEqual(expected_directory, module.base)
                self.assertIs(
                    expected_directory.exists(),
                    ensure_exists,
                    msg=f'{expected_directory} should{"" if ensure_exists else " not"} exist.',
                )
