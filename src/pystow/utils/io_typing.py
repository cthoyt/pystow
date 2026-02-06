"""Typing for I/O."""

import typing
from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal, TypeAlias

if TYPE_CHECKING:
    import _csv

__all__ = [
    "MODE_MAP",
    "OPERATION_VALUES",
    "REPRESENTATION_VALUES",
    "REVERSE_MODE_MAP",
    "_MODE_TO_SIMPLE",
    "Operation",
    "Reader",
    "Representation",
    "Writer",
    "get_mode_pair",
]

Reader: TypeAlias = "_csv._reader"
Writer: TypeAlias = "_csv._writer"

#: A human-readable flag for how to open a file.
Operation: TypeAlias = Literal["read", "write"]
OPERATION_VALUES: set[str] = set(typing.get_args(Operation))

#: A human-readable flag for how to open a file.
Representation: TypeAlias = Literal["text", "binary"]
REPRESENTATION_VALUES: set[str] = set(typing.get_args(Representation))

#: Characters for "unqualified" modes, which might be interpreted
#: differently by different functions
UnqualifiedMode: TypeAlias = Literal["r", "w"]

#: Characters for "qualified" modes, which are absolute (as opposed to
#: :data:`UnqualifiedMode`, which is context-dependent)
QualifiedMode: TypeAlias = Literal["rt", "wt", "rb", "wb"]

ModePair: TypeAlias = tuple[Operation, Representation]

_MODE_TO_SIMPLE: Mapping[Operation, UnqualifiedMode] = {
    "read": "r",
    "write": "w",
}

#: A mapping between operation/representation pairs and qualified modes
MODE_MAP: dict[ModePair, QualifiedMode] = {
    ("read", "text"): "rt",
    ("read", "binary"): "rb",
    ("write", "text"): "wt",
    ("write", "binary"): "wb",
}

#: A mapping between qualified modes and operation/representation pairs
REVERSE_MODE_MAP: dict[QualifiedMode, ModePair] = {
    "rt": ("read", "text"),
    "rb": ("read", "binary"),
    "wt": ("write", "text"),
    "wb": ("write", "binary"),
}

UNQUALIFIED_TEXT_MAP: dict[UnqualifiedMode, ModePair] = {
    "r": ("read", "text"),
    "w": ("write", "text"),
}
UNQUALIFIED_BINARY_MAP: dict[UnqualifiedMode, ModePair] = {
    "r": ("read", "binary"),
    "w": ("write", "binary"),
}


def get_mode_pair(
    mode: UnqualifiedMode | QualifiedMode, interpretation: Representation
) -> ModePair:
    """Get the mode pair."""
    match mode:
        case "rt" | "wt" | "rb" | "wb":
            return REVERSE_MODE_MAP[mode]
        case "r" | "w" if interpretation == "text":
            return UNQUALIFIED_TEXT_MAP[mode]
        case "r" | "w" if interpretation == "binary":
            return UNQUALIFIED_BINARY_MAP[mode]
        case _:
            raise ValueError(f"invalid mode: {mode}")
