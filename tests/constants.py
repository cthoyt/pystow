"""Test constants."""

from __future__ import annotations

import os
import unittest
from pathlib import Path

__all__ = [
    "HERE",
    "RESOURCES",
    "skip_on_windows",
]

skip_on_windows = unittest.skipIf(
    os.name == "nt",
    reason="Funny stuff happens in requests with a file adapter on windows that adds line breaks",
)

HERE = Path(__file__).resolve().parent
RESOURCES = HERE.joinpath("resources")
