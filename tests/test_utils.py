# -*- coding: utf-8 -*-

"""Tests for utilities."""

import hashlib
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from pystow.utils import (
    HexDigestError,
    download,
    getenv_path,
    mkdir,
    mock_envvar,
    n,
    name_from_url,
    read_tarfile_csv,
    read_zipfile_csv,
    write_tarfile_csv,
    write_zipfile_csv,
)

HERE = Path(__file__).resolve().parent
TEST_TXT = HERE.joinpath("resources", "test.txt")


class TestUtils(unittest.TestCase):
    """Test utility functions."""

    def test_name_from_url(self):
        """Test :func:`name_from_url`."""
        data = [
            ("test.tsv", "https://example.com/test.tsv"),
            ("test.tsv", "https://example.com/deeper/test.tsv"),
            ("test.tsv.gz", "https://example.com/deeper/test.tsv.gz"),
        ]
        for name, url in data:
            with self.subTest(name=name, url=url):
                self.assertEqual(name, name_from_url(url))

    def test_mkdir(self):
        """Test for ensuring a directory."""
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            subdirectory = directory / "sd1"
            self.assertFalse(subdirectory.exists())

            mkdir(subdirectory, ensure_exists=False)
            self.assertFalse(subdirectory.exists())

            mkdir(subdirectory, ensure_exists=True)
            self.assertTrue(subdirectory.exists())

    def test_mock_envvar(self):
        """Test that environment variables can be mocked properly."""
        name, value = n(), n()

        self.assertNotIn(name, os.environ)
        with mock_envvar(name, value):
            self.assertIn(name, os.environ)
            self.assertEqual(value, os.getenv(name))
        self.assertNotIn(name, os.environ)

    def test_getenv_path(self):
        """Test that :func:`getenv_path` works properly."""
        envvar = n()

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            value = directory / n()
            default = directory / n()

            self.assertEqual(default, getenv_path(envvar, default))
            with mock_envvar(envvar, value.as_posix()):
                self.assertEqual(value, getenv_path(envvar, default))
            # Check that it goes back
            self.assertEqual(default, getenv_path(envvar, default))

    def test_compressed_io(self):
        """Test that the read/write to compressed folder functions work."""
        rows = [[1, 2], [3, 4], [5, 6]]
        columns = ["A", "B"]
        df = pd.DataFrame(rows, columns=columns)
        inner_path = "okay.tsv"

        data = [
            ("test.zip", write_zipfile_csv, read_zipfile_csv),
            ("test.tar.gz", write_tarfile_csv, read_tarfile_csv),
        ]
        for name, writer, reader in data:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                path = directory / name
                self.assertFalse(path.exists())
                writer(df, path=path, inner_path=inner_path)
                self.assertTrue(path.exists())
                new_df = reader(path=path, inner_path=inner_path)
                self.assertEqual(list(df.columns), list(new_df.columns))
                self.assertEqual(df.values.tolist(), new_df.values.tolist())


class TestHashing(unittest.TestCase):
    """Tests for hexdigest checking."""

    def setUp(self) -> None:
        """Set up a test."""
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name).joinpath("test.tsv")

        md5 = hashlib.md5()  # noqa:S303
        with TEST_TXT.open("rb") as file:
            md5.update(file.read())
        self.expected_md5 = md5.hexdigest()
        self.mismatching_md5_hexdigest = "yolo"
        self.assertNotEqual(self.mismatching_md5_hexdigest, self.expected_md5)

    def tearDown(self) -> None:
        """Tear down a test."""
        self.directory.cleanup()

    def test_hash_success(self):
        """Test checking actually works."""
        self.assertFalse(self.path.exists())
        download(
            url=TEST_TXT.as_uri(),
            path=self.path,
            hexdigests={
                "md5": self.expected_md5,
            },
        )

    def test_hash_error(self):
        """Test hash error on download."""
        self.assertFalse(self.path.exists())
        with self.assertRaises(HexDigestError):
            download(
                url=TEST_TXT.as_uri(),
                path=self.path,
                hexdigests={
                    "md5": self.mismatching_md5_hexdigest,
                },
            )

    def test_override_hash_error(self):
        """Test hash error on download."""
        self.path.write_text("test file content")

        self.assertTrue(self.path.exists())
        with self.assertRaises(HexDigestError):
            download(
                url=TEST_TXT.as_uri(),
                path=self.path,
                hexdigests={
                    "md5": self.expected_md5,
                },
                force=False,
            )

    def test_force(self):
        """Test overwriting wrong file."""
        # now if force=True it should not bother with the hash check
        self.path.write_text("test file content")

        self.assertTrue(self.path.exists())
        download(
            url=TEST_TXT.as_uri(),
            path=self.path,
            hexdigests={
                "md5": self.expected_md5,
            },
            force=True,
        )
