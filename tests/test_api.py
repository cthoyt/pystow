# -*- coding: utf-8 -*-

"""Test for API completeness."""

import inspect
import unittest

import pandas as pd
import rdflib

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
            name = f"test.{ext}"
            with self.subTest(ext=ext):
                path = pystow.join("test", name=name)
                if path.is_file():
                    path.unlink()
                self.assertFalse(path.is_file())

                obj = ["a", "b", "c"]
                dump("test", name=name, obj=obj)
                self.assertTrue(path.is_file())

                self.assertEqual(obj, load("test", name=name))

    def test_pd_io(self):
        """Test pandas IO."""
        columns = list("abc")
        data = [(1, 2, 3), (4, 5, 6)]
        df = pd.DataFrame(data, columns=columns)
        name = "test.tsv"
        path = pystow.join("test", name=name)
        if path.is_file():
            path.unlink()
        self.assertFalse(path.is_file())

        pystow.dump_df("test", name=name, df=df)
        self.assertTrue(path.is_file())

        self.assertEqual(df.values.tolist(), pystow.load_df("test", name=name).values.tolist())

    def test_rdf_io(self):
        """Test RDFlib IO."""
        graph = rdflib.Graph()
        graph.add(
            (
                rdflib.URIRef("http://example.com/subject"),
                rdflib.URIRef("http://example.com/predicate"),
                rdflib.URIRef("http://example.com/object"),
            )
        )
        self.assertEqual(1, len(graph))
        name = "test.ttl"
        path = pystow.join("test", name=name)
        if path.is_file():
            path.unlink()
        self.assertFalse(path.is_file())

        pystow.dump_rdf("test", name=name, obj=graph)
        self.assertTrue(path.is_file())

        graph_reloaded = pystow.load_rdf("test", name=name)
        self.assertIsInstance(graph_reloaded, rdflib.Graph)
        self.assertEqual(
            {tuple(t) for t in graph},
            {tuple(t) for t in graph_reloaded},
        )
