"""Implementation of an efficient cached graph data structure."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from tqdm import tqdm
from typing_extensions import Self

from .utils import safe_open

__all__ = [
    "GraphCache",
    "GraphCachePaths",
    "build_graph_cache",
]


@dataclass
class GraphCachePaths:
    """An object with the paths for cached graph data."""

    nodes: Path
    forward_indices_pointer: Path
    forward_indices: Path
    reverse_indices_pointer: Path
    reverse_indices: Path

    def exists(self) -> bool:
        """Check that the cache exists."""
        return all(
            path.is_file()
            for path in (
                self.nodes,
                self.forward_indices_pointer,
                self.reverse_indices_pointer,
                self.forward_indices,
                self.reverse_indices,
            )
        )

    @classmethod
    def from_directory(cls, directory: str | Path) -> Self:
        """Instantiate the object from the given directory."""
        directory = Path(directory).expanduser().resolve()
        if not directory.is_dir():
            raise NotADirectoryError
        return cls(
            nodes=directory / "nodes.txt.gz",
            forward_indices_pointer=directory / "fwd_indptr.bin",
            forward_indices=directory / "fwd_indices.bin",
            reverse_indices_pointer=directory / "rev_indptr.bin",
            reverse_indices=directory / "rev_indices.bin",
        )


class SingleGraphCache:
    """A tool for a one-directional graph."""

    def __init__(
        self,
        index_pointer_path: Path,
        index_path: Path,
        node_to_id: dict[str, int],
        id_to_node: dict[int, str],
    ) -> None:
        """Construct a memory graph."""
        self.node_to_id = node_to_id
        self.id_to_node = id_to_node
        self.indices_pointers = np.memmap(index_pointer_path, dtype=np.int64, mode="r")
        self.indices = np.memmap(index_path, dtype=np.int32, mode="r")

    def edges(self, node: str) -> list[str]:
        """Get edges for the node."""
        return [self.id_to_node[neighbor] for neighbor in self._get_edges(self.node_to_id[node])]

    def _get_edges(self, node: int) -> Collection[int]:
        return self.indices[self.indices_pointers[node] : self.indices_pointers[node + 1]]


class GraphCache:
    """A tool for looking up in and out edges quickly."""

    def __init__(self, paths: GraphCachePaths) -> None:
        """Construct a memory graph."""
        with safe_open(paths.nodes) as file:
            node_to_id = {node.strip(): i for i, node in enumerate(file)}
        id_to_node = {v: k for k, v in node_to_id.items()}
        self.forward = SingleGraphCache(
            paths.forward_indices_pointer, paths.forward_indices, node_to_id, id_to_node
        )
        self.reverse = SingleGraphCache(
            paths.reverse_indices_pointer, paths.reverse_indices, node_to_id, id_to_node
        )

    @classmethod
    def from_directory(cls, directory: str | Path) -> Self:
        """Construct a memory graph from a directory."""
        return cls(GraphCachePaths.from_directory(directory))

    def in_edges(self, node: str) -> list[str]:
        """Get in-edges for the node."""
        return self.reverse.edges(node)

    def out_edges(self, node: str) -> list[str]:
        """Get out-edges for the node."""
        return self.forward.edges(node)


def build_graph_cache(
    edges: Callable[[], Iterable[tuple[str, str]]],
    directory: str | Path | GraphCachePaths,
    *,
    sort_nodes: bool = False,
    progress: bool = True,
    estimated_edges: int | None = None,
) -> GraphCache:
    """Cache the directed graph to the disk using a CSR data structure.

    :param edges: A function, that when called, produces an iterable of edges (pairs of
        strings). This function accepts an iterable because it needs to make three
        passes over the edges, and to support large edge lists, this might take an
        iterator that reads a file.
    :param directory: The directory to use for caching, or a pre-instantiated cache
        directory object
    :param sort_nodes: Whether to sort the nodes first. This is not really needed in
        case of testing.
    :param progress: Whether to show a progress bar on the three passes of
    :param estimated_edges: On the first pass of the edge iterator, it's unknown how
        many edges there are. If you know, or have a good estimate, use this parameter
        to give more information to the progress bar.

    :returns: A graph cache object, which can access the written binaries quickly
    """
    if not callable(edges):
        raise ValueError(
            "`edges` argument must be callable. This is because construction "
            "takes three passes, so it's better that a function that can iterate "
            "is given, to avoid needing to load into memory. If you already have "
            "your graph in memory, pass edges=lambda: edges`"
        )

    if isinstance(directory, GraphCachePaths):
        paths = directory
    else:
        paths = GraphCachePaths.from_directory(directory)

    nodes: set[str] = set()
    n_edges = 0
    for edge in tqdm(
        edges(),
        unit="edge",
        unit_scale=True,
        desc="indexing nodes",
        disable=not progress,
        total=estimated_edges,
    ):
        nodes.update(edge)
        n_edges += 1

    n = len(nodes)

    node_to_id = {}
    with safe_open(paths.nodes, operation="write") as file:
        for i, node in enumerate(sorted(nodes) if sort_nodes else nodes):
            node_to_id[node] = i
            print(node, file=file)

    forward_indices_pointer = np.zeros(n + 1, dtype=np.int64)
    reverse_indices_pointer = np.zeros(n + 1, dtype=np.int64)

    for u_str, v_str in tqdm(
        edges(),
        total=n_edges,
        unit="edge",
        unit_scale=True,
        desc="constructing pointers",
        disable=not progress,
    ):
        forward_indices_pointer[node_to_id[u_str] + 1] += 1
        reverse_indices_pointer[node_to_id[v_str] + 1] += 1

    np.cumsum(forward_indices_pointer, out=forward_indices_pointer)
    np.cumsum(reverse_indices_pointer, out=reverse_indices_pointer)

    forward_indices = np.memmap(
        paths.forward_indices, dtype=np.int32, mode="w+", shape=(forward_indices_pointer[-1],)
    )
    reverse_indices = np.memmap(
        paths.reverse_indices, dtype=np.int32, mode="w+", shape=(reverse_indices_pointer[-1],)
    )

    forward_cursor = forward_indices_pointer.copy()
    reverse_cursor = reverse_indices_pointer.copy()

    for u_str, v_str in tqdm(
        edges(),
        total=n_edges,
        unit="edge",
        unit_scale=True,
        desc="filling edges",
        disable=not progress,
    ):
        u = node_to_id[u_str]
        v = node_to_id[v_str]

        forward_indices[forward_cursor[u]] = v
        forward_cursor[u] += 1

        reverse_indices[reverse_cursor[v]] = u
        reverse_cursor[v] += 1

    forward_indices_pointer.tofile(paths.forward_indices_pointer)
    reverse_indices_pointer.tofile(paths.reverse_indices_pointer)
    reverse_indices.flush()
    forward_indices.flush()

    return GraphCache(paths)
