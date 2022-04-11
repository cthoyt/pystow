# -*- coding: utf-8 -*-

"""Test for API completeness."""

import inspect
import unittest

import pandas as pd

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

    def test_io(self):
        """Test IO functions."""
        for ext, dump, load in [
            ("json", pystow.dump_json, pystow.load_json),
            ("pkl", pystow.dump_pickle, pystow.load_pickle),
        ]:
            with self.subTest(ext=ext):
                path = pystow.join("test", name=f"test.{ext}")
                path.unlink(missing_ok=True)
                self.assertFalse(path.is_file())

                obj = ["a", "b", "c"]
                dump("test", name=f"test.{ext}", obj=obj)
                self.assertTrue(path.is_file())

                self.assertEqual(obj, load("test", name=f"test.{ext}"))

    def test_pd_io(self):
        """Test pandas IO."""
        columns = list("abc")
        data = [(1, 2, 3), (4, 5, 6)]
        df = pd.DataFrame(data, columns=columns)
        path = pystow.join("test", name="test.tsv")
        path.unlink(missing_ok=True)
        self.assertFalse(path.is_file())

        pystow.dump_df("test", name="test.tsv", df=df)
        self.assertTrue(path.is_file())

        self.assertEqual(
            df.values.tolist(), pystow.load_df("test", name="test.tsv").values.tolist()
        )
