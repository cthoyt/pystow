"""Utilities for working with Pydantic."""

from __future__ import annotations

import typing
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from .safe_open import safe_open

if TYPE_CHECKING:
    import pydantic

__all__ = [
    "iter_pydantic_jsonl",
    "read_pydantic_jsonl",
    "write_pydantic_jsonl",
]

BaseModelVar = typing.TypeVar("BaseModelVar", bound="pydantic.BaseModel")


def iter_pydantic_jsonl(
    file: str | Path | TextIO, model_cls: type[BaseModelVar]
) -> Iterable[BaseModelVar]:
    """Read models to a file as JSONL."""
    with safe_open(file, operation="read", representation="text") as file:
        for line in file:
            yield model_cls.model_validate_json(line)


def read_pydantic_jsonl(
    file: str | Path | TextIO, model_cls: type[BaseModelVar]
) -> list[BaseModelVar]:
    """Read models to a file as JSONL."""
    return list(iter_pydantic_jsonl(file, model_cls))


def write_pydantic_jsonl(
    models: Iterable[pydantic.BaseModel], file: str | Path | TextIO, **kwargs: Any
) -> None:
    """Write models to a file as JSONL."""
    kwargs.setdefault("exclude_none", True)
    kwargs.setdefault("exclude_unset", True)
    kwargs.setdefault("exclude_defaults", True)
    with safe_open(file, operation="write", representation="text") as file:
        for model in models:
            file.write(model.model_dump_json(**kwargs) + "\n")
