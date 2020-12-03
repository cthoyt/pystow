# -*- coding: utf-8 -*-

"""Tests for PYSTOW."""

import contextlib
import os
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from pystow.api import (
    PYSTOW_HOME_ENVVAR, PYSTOW_NAME_ENVVAR, get_home, get_name,
)


@contextlib.contextmanager
def mock_envvar(k: str, v: str):
    """Mock the environment variable then delete it after the test is over."""
    os.environ[k] = v
    yield
    del os.environ[k]


class TestPYSTOW(unittest.TestCase):
    """Tests for :mod:`PYSTOW`."""

    def test_mock_envvar(self):
        """Test that environment variables can be mocked properly."""
        name = 'abcabcabcabc'
        value = 'adlgkdgkjsg'
        self.assertNotIn(name, os.environ)
        with mock_envvar(name, value):
            self.assertIn(name, os.environ)
            self.assertEqual(value, os.getenv(name))
        self.assertNotIn(name, os.environ)

    def test_mock_home(self):
        """Test that home can be properly mocked."""
        name = str(uuid4())

        with tempfile.TemporaryDirectory() as d:
            expected_path_str = os.path.join(d, name)
            expected_path = Path(d) / name
            self.assertFalse(expected_path.exists())

            with mock_envvar(PYSTOW_HOME_ENVVAR, expected_path_str):
                self.assertFalse(expected_path.exists())
                self.assertEqual(expected_path, get_home(ensure_exists=False))
                self.assertFalse(expected_path.exists())

    def test_mock_name(self):
        """Test that the name can be properly mocked."""
        name = str(uuid4())

        expected_path = Path.home() / name
        self.assertFalse(expected_path.exists())

        with mock_envvar(PYSTOW_NAME_ENVVAR, name):
            self.assertEqual(name, get_name())

            self.assertFalse(expected_path.exists())
            self.assertEqual(expected_path, get_home(ensure_exists=False))
            self.assertFalse(expected_path.exists())
