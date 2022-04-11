# -*- coding: utf-8 -*-

"""Tests for utilities."""

import hashlib
import os
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from requests_file import FileAdapter

from pystow.utils import (
    HexDigestError,
    download,
    get_hexdigests_remote,
    getenv_path,
    mkdir,
    mock_envvar,
    n,
    name_from_url,
    read_tarfile_csv,
    read_zip_np,
    read_zipfile_csv,
    write_tarfile_csv,
    write_zipfile_csv,
    write_zipfile_np,
)

HERE = Path(__file__).resolve().parent
TEST_TXT = HERE.joinpath("resources", "test.txt")
TEST_TXT_MD5 = HERE.joinpath("resources", "test.txt.md5")
TEST_TXT_VERBOSE_MD5 = HERE.joinpath("resources", "test_verbose.txt.md5")
TEST_TXT_WRONG_MD5 = HERE.joinpath("resources", "test_wrong.txt.md5")

skip_on_windows = unittest.skipIf(
    os.name == "nt",
    reason="Funny stuff happens in requests with a file adapter on windows that adds line breaks",
)


class _Session(requests.sessions.Session):
    """A mock session."""

    def __init__(self):
        """Instantiate the patched session with an additional file adapter."""
        super().__init__()
        self.mount("file://", FileAdapter())


requests.sessions.Session = _Session


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

    @skip_on_windows
    def test_file_values(self):
        """Test encodings."""
        for url, value in [
            (TEST_TXT, "this is a test file\n"),
            (TEST_TXT_MD5, "4221d002ceb5d3c9e9137e495ceaa647"),
            (TEST_TXT_VERBOSE_MD5, "MD5(text.txt)=4221d002ceb5d3c9e9137e495ceaa647"),
            (TEST_TXT_WRONG_MD5, "yolo"),
        ]:
            with self.subTest(name=url.name):
                self.assertEqual(value, requests.get(url.as_uri()).text)

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

    def test_numpy_io(self):
        """Test IO with numpy."""
        arr = np.array([[0, 1], [2, 3]])
        inner_path = "okay.npz"
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            path = directory / "test.zip"
            write_zipfile_np(arr, inner_path=inner_path, path=path)
            reloaded_arr = read_zip_np(path=path, inner_path=inner_path)
            self.assertTrue(np.array_equal(arr, reloaded_arr))


class TestHashing(unittest.TestCase):
    """Tests for hexdigest checking."""

    def setUp(self) -> None:
        """Set up a test."""
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name).joinpath("test.tsv")

        md5 = hashlib.md5()  # noqa:S303,S324
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

    @skip_on_windows
    def test_hash_remote_success(self):
        """Test checking actually works."""
        self.assertFalse(self.path.exists())
        download(
            url=TEST_TXT.as_uri(),
            path=self.path,
            hexdigests_remote={
                "md5": TEST_TXT_MD5.as_uri(),
            },
            hexdigests_strict=True,
        )
        self.assertTrue(self.path.exists())

    @skip_on_windows
    def test_hash_remote_verbose_success(self):
        """Test checking actually works."""
        self.assertFalse(self.path.exists())
        download(
            url=TEST_TXT.as_uri(),
            path=self.path,
            hexdigests_remote={
                "md5": TEST_TXT_VERBOSE_MD5.as_uri(),
            },
            hexdigests_strict=False,
        )
        self.assertTrue(self.path.exists())

    def test_hash_remote_verbose_failure(self):
        """Test checking actually works."""
        self.assertFalse(self.path.exists())
        with self.assertRaises(HexDigestError):
            download(
                url=TEST_TXT.as_uri(),
                path=self.path,
                hexdigests_remote={
                    "md5": TEST_TXT_VERBOSE_MD5.as_uri(),
                },
                hexdigests_strict=True,
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

    def test_hash_remote_error(self):
        """Test hash error on download."""
        self.assertFalse(self.path.exists())
        with self.assertRaises(HexDigestError):
            download(
                url=TEST_TXT.as_uri(),
                path=self.path,
                hexdigests_remote={
                    "md5": TEST_TXT_WRONG_MD5.as_uri(),
                },
                hexdigests_strict=True,
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

    def test_override_hash_remote_error(self):
        """Test hash error on download."""
        self.path.write_text("test file content")

        self.assertTrue(self.path.exists())
        with self.assertRaises(HexDigestError):
            download(
                url=TEST_TXT.as_uri(),
                path=self.path,
                hexdigests_remote={
                    "md5": TEST_TXT_MD5.as_uri(),
                },
                hexdigests_strict=True,
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

    @skip_on_windows
    def test_remote_force(self):
        """Test overwriting wrong file."""
        # now if force=True it should not bother with the hash check
        self.path.write_text("test file content")

        self.assertTrue(self.path.exists())
        download(
            url=TEST_TXT.as_uri(),
            path=self.path,
            hexdigests_remote={
                "md5": TEST_TXT_MD5.as_uri(),
            },
            hexdigests_strict=True,
            force=True,
        )

    def test_hexdigest_urls(self):
        """Test getting hex digests from URLs."""
        for url, strict in [
            (TEST_TXT_MD5, True),
            (TEST_TXT_MD5, False),
            (TEST_TXT_VERBOSE_MD5, False),
        ]:
            hexdigests = get_hexdigests_remote(
                {"md5": url.as_uri()},
                hexdigests_strict=strict,
            )
            self.assertEqual(
                "4221d002ceb5d3c9e9137e495ceaa647",
                hexdigests["md5"],
            )

        hexdigests = get_hexdigests_remote(
            {"md5": TEST_TXT_VERBOSE_MD5.as_uri()}, hexdigests_strict=True
        )
        self.assertNotEqual(
            "4221d002ceb5d3c9e9137e495ceaa647",
            hexdigests["md5"],
        )

        # Live test case
        # hexdigests = get_hexdigests_remote(
        #     {"md5": "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed22n0001.xml.gz.md5"},
        #     hexdigests_strict=False,
        # )
        # self.assertEqual(
        #     {
        #         "md5": "0f08d8f3947dde1f3bced5e1f450c0da",
        #     },
        #     hexdigests,
        # )
