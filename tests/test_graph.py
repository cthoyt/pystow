"""Test graph."""

import tempfile
import unittest

from pystow.graph import GraphCachePaths, build_graph_cache


class TestGraph(unittest.TestCase):
    """Test graph."""

    def test_cache(self) -> None:
        """Test error on missing directory."""
        with self.assertRaises(NotADirectoryError):
            GraphCachePaths.from_directory("blahblahblah")

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
                build_graph_cache(edges, tmpdir)  # type:ignore[arg-type]

            paths = GraphCachePaths.from_directory(tmpdir)
            self.assertFalse(paths.exists())

            graph = build_graph_cache(lambda: edges, paths, sort_nodes=True, progress=False)

            self.assertTrue(paths.exists())

            self.assertEqual([], graph.out_edges("ZZZ"))
            self.assertEqual([], graph.in_edges("ZZZ"))
            with self.assertRaises(KeyError):
                graph.out_edges("ZZZ", raise_on_missing=True)
            with self.assertRaises(KeyError):
                graph.in_edges("ZZZ", raise_on_missing=True)

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
