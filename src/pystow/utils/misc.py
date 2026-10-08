"""Tools for tabulation."""

from collections import Counter
from collections.abc import Sequence
from typing import Any

__all__ = [
    "tabulate_counter",
]


def tabulate_counter(counter: Counter[Any], *, n: int | None = None, **kwargs: Any) -> str:
    """Tabulate a counter."""
    from tabulate import tabulate

    key = next(iter(counter))
    if isinstance(key, Sequence) and not isinstance(key, str):
        return tabulate([(*key, count) for key, count in counter.most_common(n=n)], **kwargs)
    else:
        return tabulate(counter.most_common(n=n), **kwargs)
