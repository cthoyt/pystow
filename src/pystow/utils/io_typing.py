"""Typing for I/O."""

from __future__ import annotations

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
    "InvalidOperationError",
    "InvalidRepresentationError",
    "Operation",
    "Reader",
    "Representation",
    "Writer",
    "ensure_sensible_default_encoding",
    "ensure_sensible_newline",
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


class InvalidRepresentationError(ValueError):
    """Raised when passing an invalid representation."""

    def __init__(self, representation: str) -> None:
        """Instantiate the exception."""
        self.representation = representation

    def __str__(self) -> str:
        """Create a string for the exception."""
        return (
            f"Invalid representation: {self.representation}. "
            f"Should be one of {REPRESENTATION_VALUES}."
        )


class InvalidOperationError(ValueError):
    """Raised when passing an invalid operation."""

    def __init__(self, operation: str) -> None:
        """Instantiate the exception."""
        self.operation = operation

    def __str__(self) -> str:
        """Create a string for the exception."""
        return f"Invalid operation: {self.operation}. Should be one of {OPERATION_VALUES}."


def ensure_sensible_default_encoding(
    encoding: str | None, *, representation: Representation
) -> str | None:
    """Get a sensible default encoding."""
    # this function exists because windows doesn't use UTF-8 as a default
    # encoding for some reason, and that's bonk. So we intercept the encoding
    # and set it explicitly to UTF-8
    if representation == "binary":
        if encoding is not None:
            raise ValueError
        else:
            return None
    elif representation == "text":
        if encoding is not None:
            return encoding
        return "utf-8"
    else:
        raise InvalidRepresentationError(representation)


def ensure_sensible_newline(newline: str | None, *, representation: Representation) -> str | None:
    """Get a sensible default newline."""
    # this function exists to override the default way newlines are
    # automatically interpreted by python on Windows to always use
    # \n instead of \r\n
    if representation == "binary":
        if newline is not None:
            raise ValueError
        return None
    elif representation == "text":
        if newline is not None:
            return newline
        return "\n"
    else:
        raise InvalidRepresentationError(representation)
