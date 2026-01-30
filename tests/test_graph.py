"""Test graph."""

import tempfile
import unittest

from pystow.graph import build_digraph_cache


class TestGraph(unittest.TestCase):
    """Test graph."""

    def test_graph(self) -> None:
        """Test building a graph."""
        edges = [
            ("a", "b"),
            ("a", "c"),
            ("a", "d"),
            ("b", "d"),
            ("d", "e"),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValueError):
                build_digraph_cache(edges, tmpdir)

            graph = build_digraph_cache(lambda: edges, tmpdir, sort_nodes=True, progress=False)

            self.assertEqual({"b", "c", "d"}, set(graph.out_edges("a")))
            self.assertEqual({"d"}, set(graph.out_edges("b")))
            self.assertEqual(set(), set(graph.out_edges("c")))
            self.assertEqual({"e"}, set(graph.out_edges("d")))
            self.assertEqual(set(), set(graph.out_edges("e")))

            self.assertEqual(set(), set(graph.in_edges("a")))
            self.assertEqual({"a"}, set(graph.in_edges("b")))
            self.assertEqual({"a"}, set(graph.in_edges("c")))
            self.assertEqual({"a", "b"}, set(graph.in_edges("d")))
            self.assertEqual({"d"}, set(graph.in_edges("e")))
