"""Utilities for working with Pydantic."""

from __future__ import annotations

import logging
import typing
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TextIO, TypeAlias

from tqdm import tqdm

from .safe_open import safe_open

if TYPE_CHECKING:
    import pydantic

__all__ = [
    "ModelValidateFailureAction",
    "iter_pydantic_jsonl",
    "read_pydantic_jsonl",
    "stream_write_pydantic_jsonl",
    "write_pydantic_jsonl",
]

logger = logging.getLogger(__name__)
BaseModelVar = typing.TypeVar("BaseModelVar", bound="pydantic.BaseModel")

#: The action to take on model validation failure
ModelValidateFailureAction: TypeAlias = Literal["raise", "skip"]


def iter_pydantic_jsonl(
    file: str | Path | TextIO,
    model_cls: type[BaseModelVar],
    *,
    progress: bool = False,
    failure_action: ModelValidateFailureAction = "skip",
    encoding: str | None = None,
    newline: str | None = None,
) -> Iterable[BaseModelVar]:
    """Read models to a file as JSONL."""
    with safe_open(
        file, operation="read", representation="text", encoding=encoding, newline=newline
    ) as file:
        for i, line in enumerate(
            tqdm(
                file,
                desc="Reading mappings",
                leave=False,
                unit="mapping",
                unit_scale=True,
                disable=not progress,
            )
        ):
            try:
                yv = model_cls.model_validate_json(line.strip())
            except pydantic.ValidationError:
                if failure_action == "raise":
                    raise
                else:
                    logger.debug("[line:%d] failed to parse JSON", i)
                    continue
            else:
                yield yv


def read_pydantic_jsonl(
    file: str | Path | TextIO, model_cls: type[BaseModelVar], **kwargs: Any
) -> list[BaseModelVar]:
    """Read models to a file as JSONL."""
    return list(iter_pydantic_jsonl(file, model_cls, **kwargs))


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


def stream_write_pydantic_jsonl(
    models: Iterable[BaseModelVar], file: str | Path | TextIO, **kwargs: Any
) -> Generator[BaseModelVar, None, None]:
    """Write models to a file as JSONL and yield them."""
    kwargs.setdefault("exclude_none", True)
    kwargs.setdefault("exclude_unset", True)
    kwargs.setdefault("exclude_defaults", True)
    with safe_open(file, operation="write", representation="text") as file:
        for model in models:
            file.write(model.model_dump_json(**kwargs) + "\n")
            yield model
