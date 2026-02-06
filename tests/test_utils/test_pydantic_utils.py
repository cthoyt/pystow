"""Tests for Pydantic utilities."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import pydantic

from pystow.utils import read_pydantic_jsonl, write_pydantic_jsonl, stream_write_pydantic_jsonl


@unittest.skipUnless(importlib.util.find_spec("pydantic"), "pydantic not installed")
class TestPydanticUtils(unittest.TestCase):
    """Tests for Pydantic utilities."""

    def test_pydantic_io(self) -> None:
        """Test writing Pydantic."""
        from pydantic import BaseModel

        class Model(BaseModel):
            """A test model."""

            name: str

        models = [Model(name=f"test {i}") for i in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).joinpath("data.jsonl")
            write_pydantic_jsonl(models, path)
            self.assertEqual(models, read_pydantic_jsonl(path, Model))

    def test_streaming_writer(self) -> None:
        """Test writing Pydantic."""
        from pydantic import BaseModel

        class Model(BaseModel):
            """A test model."""

            name: str

        models = [Model(name=f"test {i}") for i in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).joinpath("data.jsonl")
            new_models = list(stream_write_pydantic_jsonl(models, path))
            self.assertEqual(models, new_models)
            self.assertEqual(models, read_pydantic_jsonl(path, Model))

    def test_error_action(self) -> None:
        """Test error actions."""
        from pydantic import BaseModel

        class Model(BaseModel):
            """A test model."""

            name: str

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).joinpath("data.jsonl")
            with path.open("w") as file:
                print(json.dumps({"name": "1"}), file=file)
                print(json.dumps({"name": "2"}), file=file)
                print(json.dumps({"nope": "3"}), file=file)

            self.assertEqual(
                [Model(name="1"), Model(name="2")],
                list(read_pydantic_jsonl(path, Model, failure_action="skip")),
            )

            with self.assertRaises(pydantic.ValidationError):
                (list(read_pydantic_jsonl(path, Model, failure_action="raise")),)
