# -*- coding: utf-8 -*-

"""Test for API completeness."""

import inspect
import unittest

import pystow
from pystow import Module

SKIP = {"__init__"}


class TestExposed(unittest.TestCase):
    """Test API exposure."""

    def test_exposed(self):
        """Test that all module-level functions also have a counterpart in the top-level API."""
        for name, func in Module.__dict__.items():
            if not inspect.isfunction(func) or name in SKIP:
                continue
            with self.subTest(name=name):
                self.assertIn(
                    name,
                    pystow.api.__all__,
                    msg=f"Module.{name} should be included in from `pystow.api.__all__`.",
                )
                self.assertTrue(
                    hasattr(pystow.api, name),
                    msg=f"`Module.{name} should be exposed as a top-level function in `pystow.api`.",
                )
                self.assertTrue(
                    hasattr(pystow, name),
                    msg=f"`pystow.api.{name}` should be imported in `pystow.__init__`.",
                )
