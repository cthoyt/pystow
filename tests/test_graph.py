"""Test graph."""

import tempfile
import unittest
from pathlib import Path

from pystow.graph import MemoryGraph, construct


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
            directory = Path(tmpdir)

            construct(lambda: edges, directory, sort_nodes=True, progress=False)
            graph = MemoryGraph(directory)

            self.assertEqual({"a": 0, "b": 1, "c": 2, "d": 3, "e": 4}, graph.node_to_id)
            self.assertEqual({"b", "c", "d"}, set(graph.get_out_edges("a")))
            self.assertEqual({"d"}, set(graph.get_out_edges("b")))
            self.assertEqual(set(), set(graph.get_out_edges("c")))
            self.assertEqual({"e"}, set(graph.get_out_edges("d")))
            self.assertEqual(set(), set(graph.get_out_edges("e")))

            self.assertEqual(set(), set(graph.get_in_edges("a")))
            self.assertEqual({"a"}, set(graph.get_in_edges("b")))
            self.assertEqual({"a"}, set(graph.get_in_edges("c")))
            self.assertEqual({"a", "b"}, set(graph.get_in_edges("d")))
            self.assertEqual({"d"}, set(graph.get_in_edges("e")))
