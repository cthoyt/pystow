"""Implementation of an efficient cached graph data structure."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from tqdm import tqdm
from typing_extensions import Self

from pystow.utils import safe_open


@dataclass
class Paths:
    """An object with the paths for cached graph data."""

    nodes_path: Path
    forward_index_pointer: Path
    forward_indices: Path
    reverse_index_pointer: Path
    reverse_indicies: Path

    @classmethod
    def from_directory(cls, directory: str | Path) -> Self:
        """Instantiate the object from the given directory."""
        directory = Path(directory).expanduser().resolve()
        if not directory.is_dir():
            raise NotADirectoryError
        return cls(
            nodes_path=directory / "nodes.txt.gz",
            forward_index_pointer=directory / "fwd_indptr.bin",
            forward_indices=directory / "fwd_indices.bin",
            reverse_index_pointer=directory / "rev_indptr.bin",
            reverse_indicies=directory / "rev_indices.bin",
        )


class MemoryGraph:
    """A tool for looking up in and out edges quickly."""

    def __init__(self, directory: Path) -> None:
        """Construct a memory graph."""
        paths = Paths.from_directory(directory)
        with safe_open(paths.nodes_path) as file:
            self.node_to_id = {node.strip(): i for i, node in enumerate(file)}
        self.id_to_node = {v: k for k, v in self.node_to_id.items()}
        self.fwd_indptr = np.memmap(paths.forward_index_pointer, dtype=np.int64, mode="r")
        self.fwd_indices = np.memmap(paths.forward_indices, dtype=np.int32, mode="r")
        self.rev_indptr = np.memmap(paths.reverse_index_pointer, dtype=np.int64, mode="r")
        self.rev_indices = np.memmap(paths.reverse_indicies, dtype=np.int32, mode="r")

    def get_in_edges(self, u: str) -> list[str]:
        """Get in-edges for the node."""
        return [self.id_to_node[node] for node in self._get_in_edges(self.node_to_id[u])]

    def _get_in_edges(self, u: int) -> Collection[int]:
        return self.rev_indices[self.rev_indptr[u] : self.rev_indptr[u + 1]]

    def get_out_edges(self, u: str) -> list[str]:
        """Get out-edges for the node."""
        return [self.id_to_node[node] for node in self._get_out_edges(self.node_to_id[u])]

    def _get_out_edges(self, u: int) -> Collection[int]:
        return self.fwd_indices[self.fwd_indptr[u] : self.fwd_indptr[u + 1]]


def construct(
    edges: Callable[[], Iterable[tuple[str, str]]],
    directory: Path,
    *,
    sort_nodes: bool = False,
    progress: bool = True,
) -> None:
    """Construct a memory graph."""
    paths = Paths.from_directory(directory)
    nodes: set[str] = set()
    n_edges = 0
    for edge in tqdm(
        edges(), unit="edge", unit_scale=True, desc="indexing nodes", disable=not progress
    ):
        nodes.update(edge)
        n_edges += 1

    n = len(nodes)

    node_to_id = {}
    with safe_open(paths.nodes_path, operation="write") as file:
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

    fwd_indices = np.memmap(
        paths.forward_indices, dtype=np.int32, mode="w+", shape=(fwd_indptr[-1],)
    )

    rev_indices = np.memmap(
        paths.reverse_indicies, dtype=np.int32, mode="w+", shape=(rev_indptr[-1],)
    )

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
