# -*- coding: utf-8 -*-

"""Test for API completeness."""

import inspect
import unittest

import pandas as pd
import rdflib
from lxml import etree

import pystow
from pystow import Module

SKIP = {"__init__"}


def _df_equal(a: pd.DataFrame, b: pd.DataFrame, msg=None) -> bool:
    return a.values.tolist() == b.values.tolist()


def _rdf_equal(a: rdflib.Graph, b: rdflib.Graph, msg=None) -> bool:
    return {tuple(t) for t in a} == {tuple(t) for t in b}


def _etree_equal(a: etree.ElementTree, b: etree.ElementTree, msg=None) -> bool:
    return etree.tostring(a) == etree.tostring(b)


class TestExposed(unittest.TestCase):
    """Test API exposure."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.addTypeEqualityFunc(pd.DataFrame, _df_equal)
        self.addTypeEqualityFunc(rdflib.Graph, _rdf_equal)
        self.addTypeEqualityFunc(type(etree.ElementTree()), _etree_equal)

    def assert_io(self, obj, ext: str, dump, load):
        """Test an object can be  dumped and loaded.

        :param obj: The object to dump
        :param ext: The extension to use
        :param dump: The dump function
        :param load: The load function
        """
        name = f"test.{ext}"
        path = pystow.join("test", name=name)
        if path.is_file():
            path.unlink()
        self.assertFalse(path.is_file())

        dump("test", name=name, obj=obj)
        self.assertTrue(path.is_file())
        self.assertEqual(obj, load("test", name=name))

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
        obj = ["a", "b", "c"]
        for ext, dump, load in [
            ("json", pystow.dump_json, pystow.load_json),
            ("pkl", pystow.dump_pickle, pystow.load_pickle),
        ]:
            with self.subTest(ext=ext):
                self.assert_io(obj, ext=ext, dump=dump, load=load)

    def test_pd_io(self):
        """Test pandas IO."""
        columns = list("abc")
        data = [(1, 2, 3), (4, 5, 6)]
        df = pd.DataFrame(data, columns=columns)
        self.assert_io(df, ext="tsv", load=pystow.load_df, dump=pystow.dump_df)

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
        self.assert_io(graph, ext="ttl", dump=pystow.dump_rdf, load=pystow.load_rdf)

    def test_xml_io(self):
        """Test XML I/O."""
        root = etree.Element("root")
        root.set("interesting", "somewhat")
        etree.SubElement(root, "test")
        my_tree = etree.ElementTree(root)
        self.assert_io(my_tree, ext="xml", dump=pystow.dump_xml, load=pystow.load_xml)
