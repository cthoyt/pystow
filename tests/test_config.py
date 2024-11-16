"""Test configuration loading."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pystow
from pystow.config_api import CONFIG_HOME_ENVVAR, _get_cfp
from pystow.utils import mock_envvar


class TestConfig(unittest.TestCase):
    """Test configuration."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the class for testing."""
        cls.test_section = "test"
        cls.test_option = "option"
        cls.test_value = "value"
        cls.cfp = _get_cfp(cls.test_section)
        cls.cfp.add_section(cls.test_section)
        cls.cfp.set(
            section=cls.test_section,
            option=cls.test_option,
            value=cls.test_value,
        )

    def test_env_cast(self):
        """Test casting works properly when getting from the environment."""
        with mock_envvar("TEST_VAR", "1234"):
            self.assertEqual("1234", pystow.get_config("test", "var"))
            self.assertEqual("1234", pystow.get_config("test", "var", dtype=str))
            self.assertEqual(1234, pystow.get_config("test", "var", dtype=int))
            with self.assertRaises(ValueError):
                pystow.get_config("test", "var", dtype=bool)
            with self.assertRaises(TypeError):
                pystow.get_config("test", "var", dtype=object)

    def test_get_config(self):
        """Test lookup not existing."""
        self.assertIsNone(pystow.get_config(self.test_section, "key"))
        self.assertEqual("1234", pystow.get_config(self.test_section, "key", default="1234"))

        value = "not_value"
        self.assertEqual(
            value, pystow.get_config(self.test_section, self.test_option, passthrough=value)
        )

        self.assertEqual(1, pystow.get_config(self.test_section, self.test_option, passthrough=1))
        self.assertEqual(
            1, pystow.get_config(self.test_section, self.test_option, passthrough="1", dtype=int)
        )

        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="1", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="yes", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="Yes", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="YES", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="True", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="TRUE", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="T", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough="t", dtype=bool),
        )
        self.assertEqual(
            True,
            pystow.get_config(self.test_section, self.test_option, passthrough=True, dtype=bool),
        )
        self.assertEqual(
            True, pystow.get_config(self.test_section, self.test_option, passthrough=1, dtype=bool)
        )

    def test_subsection(self):
        """Test subsections."""
        with tempfile.TemporaryDirectory() as directory, mock_envvar(CONFIG_HOME_ENVVAR, directory):
            directory = Path(directory)
            path = directory.joinpath("test.ini")
            self.assertFalse(path.is_file(), msg="file should not already exist")

            self.assertIsNone(pystow.get_config("test:subtest", "key"))
            self.assertFalse(path.is_file(), msg="getting config should not create a file")

            pystow.write_config("test:subtest", "key", "value")
            self.assertTrue(path.is_file(), msg=f"{list(directory.iterdir())}")

            self.assertEqual("value", pystow.get_config("test:subtest", "key"))
