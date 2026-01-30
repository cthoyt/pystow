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
    "MemoryGraph",
    "Paths",
    "construct",
]


@dataclass
class Paths:
    """An object with the paths for cached graph data."""

    nodes: Path
    forward_index_pointer: Path
    forward_index: Path
    reverse_index_pointer: Path
    reverse_index: Path

    @classmethod
    def from_directory(cls, directory: str | Path) -> Self:
        """Instantiate the object from the given directory."""
        directory = Path(directory).expanduser().resolve()
        if not directory.is_dir():
            raise NotADirectoryError
        return cls(
            nodes=directory / "nodes.txt.gz",
            forward_index_pointer=directory / "fwd_indptr.bin",
            forward_index=directory / "fwd_indices.bin",
            reverse_index_pointer=directory / "rev_indptr.bin",
            reverse_index=directory / "rev_indices.bin",
        )


class MemoryGraphUndirected:
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
        self.index_pointers = np.memmap(index_pointer_path, dtype=np.int64, mode="r")
        self.index = np.memmap(index_path, dtype=np.int32, mode="r")

    def get_edges(self, u: str) -> list[str]:
        """Get edges for the node."""
        return [self.id_to_node[neighbor] for neighbor in self._get_edges(self.node_to_id[u])]

    def _get_edges(self, u: int) -> Collection[int]:
        return self.index[self.index_pointers[u] : self.index_pointers[u + 1]]


class MemoryGraph:
    """A tool for looking up in and out edges quickly."""

    def __init__(self, paths: Paths) -> None:
        """Construct a memory graph."""
        with safe_open(paths.nodes) as file:
            node_to_id = {node.strip(): i for i, node in enumerate(file)}
        id_to_node = {v: k for k, v in node_to_id.items()}
        self.forward = MemoryGraphUndirected(
            paths.forward_index_pointer, paths.forward_index, node_to_id, id_to_node
        )
        self.reverse = MemoryGraphUndirected(
            paths.reverse_index_pointer, paths.reverse_index, node_to_id, id_to_node
        )

    @classmethod
    def from_directory(cls, directory: str | Path) -> Self:
        """Construct a memory graph from a directory."""
        return cls(Paths.from_directory(directory))

    def get_in_edges(self, node: str) -> list[str]:
        """Get in-edges for the node."""
        return self.reverse.get_edges(node)

    def get_out_edges(self, node: str) -> list[str]:
        """Get out-edges for the node."""
        return self.forward.get_edges(node)


def construct(
    edges: Callable[[], Iterable[tuple[str, str]]],
    directory: Path,
    *,
    sort_nodes: bool = False,
    progress: bool = True,
    estimated_edges: int | None = None
) -> None:
    """Construct a memory graph."""
    paths = Paths.from_directory(directory)
    nodes: set[str] = set()
    n_edges = 0
    for edge in tqdm(
        edges(), unit="edge", unit_scale=True, desc="indexing nodes", disable=not progress, total=estimated_edges
    ):
        nodes.update(edge)
        n_edges += 1

    n = len(nodes)

    node_to_id = {}
    with safe_open(paths.nodes, operation="write") as file:
        for i, node in enumerate(sorted(nodes) if sort_nodes else nodes):
            node_to_id[node] = i
            print(node, file=file)

    fwd_indptr = np.zeros(n + 1, dtype=np.int64)
    rev_indptr = np.zeros(n + 1, dtype=np.int64)

    for u, v in tqdm(
        edges(),
        total=n_edges,
        unit="edge",
        unit_scale=True,
        desc="constructing pointers",
        disable=not progress,
    ):
        fwd_indptr[node_to_id[u] + 1] += 1
        rev_indptr[node_to_id[v] + 1] += 1

    np.cumsum(fwd_indptr, out=fwd_indptr)
    np.cumsum(rev_indptr, out=rev_indptr)

    fwd_indices = np.memmap(paths.forward_index, dtype=np.int32, mode="w+", shape=(fwd_indptr[-1],))

    rev_indices = np.memmap(paths.reverse_index, dtype=np.int32, mode="w+", shape=(rev_indptr[-1],))

    fwd_cursor = fwd_indptr.copy()
    rev_cursor = rev_indptr.copy()

    for u, v in tqdm(
        edges(),
        total=n_edges,
        unit="edge",
        unit_scale=True,
        desc="filling edges",
        disable=not progress,
    ):
        fwd_indices[fwd_cursor[node_to_id[u]]] = node_to_id[v]
        fwd_cursor[node_to_id[u]] += 1

        rev_indices[rev_cursor[node_to_id[v]]] = node_to_id[u]
        rev_cursor[node_to_id[v]] += 1

    fwd_indptr.tofile(paths.forward_index_pointer)
    rev_indptr.tofile(paths.reverse_index_pointer)
    fwd_indices.flush()
    rev_indices.flush()
