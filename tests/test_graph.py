import unittest
from pathlib import Path

from pystow.graph import construct
import tempfile

class TestGraph(unittest.TestCase):

    def test_graph(self) -> None:
        edges = [
            ("a", "b"),
            ("a", "c"),
            ("a", "d"),
            ("b", "d"),
            ("d", "e"),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)

            graph = construct(edges, directory)
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
