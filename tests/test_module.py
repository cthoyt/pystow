# -*- coding: utf-8 -*-

"""Tests for PyStow."""

import contextlib
import itertools as itt
import os
import tempfile
import unittest
from pathlib import Path

from pystow import ensure_csv, join
from pystow.module import Module, PYSTOW_HOME_ENVVAR, PYSTOW_NAME_ENVVAR, get_home, get_name
from pystow.utils import mock_envvar, n


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
        with mock_envvar(PYSTOW_HOME_ENVVAR, self.directory.name) as rv:
            yield rv

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
        test_url = 'https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt'
        with self.mock_directory():
            df = ensure_csv('test', url=test_url)
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
