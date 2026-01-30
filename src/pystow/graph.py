from pathlib import Path

import numpy as np
from typing import TypeVar, Generic, Collection, Self

X = TypeVar("X")


class MemoryPaths:
    def __init__(self, directory: Path) -> None:
        if not directory.is_dir():
            raise NotADirectoryError
        self.directory = directory
        self.fwd_indptr_path = directory / "fwd_indptr.bin"
        self.fwd_indices_path = directory / "fwd_indices.bin"
        self.rev_indptr_path = directory / "rev_indptr.bin"
        self.rev_indices_path = directory / "rev_indices.bin"


class MemoryGraph(Generic[X]):
    """A tool for looking up in and out edges quickly."""

    def __init__(self, directory: Path, node_to_id: dict[X, int]) -> None:
        memory_paths = MemoryPaths(directory)
        self.node_to_id = node_to_id
        self.id_to_node = {v: k for k, v in node_to_id.items()}
        self.fwd_indptr = np.memmap(memory_paths.fwd_indptr_path, dtype=np.int64, mode="r")
        self.fwd_indices = np.memmap(memory_paths.fwd_indices_path, dtype=np.int32, mode="r")
        self.rev_indptr = np.memmap(memory_paths.rev_indptr_path, dtype=np.int64, mode="r")
        self.rev_indices = np.memmap(memory_paths.rev_indices_path, dtype=np.int32, mode="r")

    def get_in_edges(self, u: X) -> Collection[X]:
        return [
            self.id_to_node[node]
            for node in self._get_in_edges(self.node_to_id[u])
        ]

    def _get_in_edges(self, u: int) -> Collection[int]:
        return self.rev_indices[self.rev_indptr[u]:self.rev_indptr[u + 1]]

    def get_out_edges(self, u: X) -> Collection[X]:
        return [
            self.id_to_node[node]
            for node in self._get_out_edges(self.node_to_id[u])
        ]

    def _get_out_edges(self, u: int) -> Collection[int]:
        return self.fwd_indices[self.fwd_indptr[u]:self.fwd_indptr[u + 1]]


def construct(edges: Collection[tuple[X, X]], directory: Path) -> MemoryGraph:
    memory_paths = MemoryPaths(directory)
    nodes: set[X] = {
        node
        for edge in edges
        for node in edge
    }
    n = len(nodes)
    node_to_id = {
        node: i
        for i, node in enumerate(nodes)
    }

    fwd_indptr = np.zeros(n + 1, dtype=np.int64)
    rev_indptr = np.zeros(n + 1, dtype=np.int64)

    for u, v in edges:
        fwd_indptr[node_to_id[u] + 1] += 1
        rev_indptr[node_to_id[v] + 1] += 1

    np.cumsum(fwd_indptr, out=fwd_indptr)
    np.cumsum(rev_indptr, out=rev_indptr)

    fwd_indices = np.memmap(
        memory_paths.fwd_indices_path, dtype=np.int32, mode="w+",
        shape=(fwd_indptr[-1],)
    )

    rev_indices = np.memmap(
        memory_paths.rev_indices_path, dtype=np.int32, mode="w+",
        shape=(rev_indptr[-1],)
    )

    fwd_cursor = fwd_indptr.copy()
    rev_cursor = rev_indptr.copy()

    for u, v in edges:
        fwd_indices[fwd_cursor[node_to_id[u]]] = node_to_id[v]
        fwd_cursor[node_to_id[u]] += 1

        rev_indices[rev_cursor[node_to_id[v]]] = node_to_id[u]
        rev_cursor[node_to_id[v]] += 1

    fwd_indptr.tofile(memory_paths.fwd_indptr_path)
    rev_indptr.tofile(memory_paths.rev_indptr_path)
    fwd_indices.flush()
    rev_indices.flush()

    return MemoryGraph(directory, node_to_id)
