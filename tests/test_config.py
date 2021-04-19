# -*- coding: utf-8 -*-

"""Test configuration loading."""

import unittest

import pystow
from pystow.config_api import _get_cfp


class TestConfig(unittest.TestCase):
    """Test configuration."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the class for testing."""
        cls.test_section = 'test'
        cls.test_option = 'option'
        cls.test_value = 'value'
        cls.cfp = _get_cfp(cls.test_section)
        cls.cfp.add_section(cls.test_section)
        cls.cfp.set(
            section=cls.test_section,
            option=cls.test_option,
            value=cls.test_value,
        )

    def test_get_config(self):
        """Test lookup not existing."""
        self.assertIsNone(pystow.get_config(self.test_section, 'key'))
        self.assertEqual('1234', pystow.get_config(self.test_section, 'key', default='1234'))

        value = 'not_value'
        self.assertEqual(value, pystow.get_config(self.test_section, self.test_option, passthrough=value))

        self.assertEqual(1, pystow.get_config(self.test_section, self.test_option, passthrough=1))
        self.assertEqual(1, pystow.get_config(self.test_section, self.test_option, passthrough='1', dtype=int))

        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='1', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='yes', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='Yes', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='YES', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='True', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='TRUE', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='T', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough='t', dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough=True, dtype=bool))
        self.assertEqual(True, pystow.get_config(self.test_section, self.test_option, passthrough=1, dtype=bool))
