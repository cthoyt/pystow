# -*- coding: utf-8 -*-

"""Tests for caching."""

import os
import tempfile
import unittest
from pathlib import Path

from pystow.cache import CachedPickle

EXPECTED = 5
EXPECTED_2 = 6


class TestCache(unittest.TestCase):
    """Tests for caches."""

    def setUp(self) -> None:
        """Set up the test case with a temporary directory."""
        self.tmpdir = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmpdir.name)

    def tearDown(self) -> None:
        """Tear down the test case's temporary directory."""
        self.tmpdir.cleanup()

    def test_cache_exception(self):
        """Test that exceptions aren't swallowed."""
        path = self.directory.joinpath("test.pkl")

        self.assertFalse(path.is_file())

        @CachedPickle(path=path)
        def _f1():
            raise NotImplementedError

        self.assertFalse(path.is_file(), msg="function has not been called")

        with self.assertRaises(NotImplementedError):
            _f1()

        self.assertFalse(
            path.is_file(),
            msg="file should not have been created if an exception was thrown by the function",
        )

    def test_cache_pickle(self):
        """Test caching a pickle."""
        path = self.directory.joinpath("test.pkl")
        self.assertFalse(
            path.is_file(),
            msg="the file should not exist at the beginning of the test",
        )

        raise_flag = True

        @CachedPickle(path=path)
        def _f1():
            if raise_flag:
                raise ValueError
            return EXPECTED

        self.assertFalse(path.is_file(), msg="the file should not exist until function is called")

        with self.assertRaises(ValueError):
            _f1()
        self.assertFalse(
            path.is_file(),
            msg="the function should throw an exception because of the flag, and no file should be created",
        )

        raise_flag = False
        actual = _f1()
        self.assertEqual(EXPECTED, actual)
        self.assertTrue(path.is_file(), msg="a file should have been created")

        raise_flag = True
        actual_2 = _f1()  # if raises, the caching mechanism didn't work
        self.assertEqual(EXPECTED, actual_2)
        self.assertTrue(path.is_file())

        os.unlink(path)
        self.assertFalse(path.is_file())
        with self.assertRaises(ValueError):
            _f1()

        @CachedPickle(path=path, force=True)
        def _f2():
            return EXPECTED_2

        self.assertEqual(EXPECTED_2, _f2())  # overwrites the file
        self.assertEqual(EXPECTED_2, _f1())
