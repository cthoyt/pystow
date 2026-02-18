"""Tests for Pydantic utilities."""

import datetime
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pystow.utils import (
    read_pydantic_jsonl,
    read_pydantic_tsv,
    stream_write_pydantic_jsonl,
    write_pydantic_jsonl,
)


@unittest.skipUnless(importlib.util.find_spec("pydantic"), "pydantic not installed")
class TestPydanticUtils(unittest.TestCase):
    """Tests for Pydantic utilities."""

    def test_pydantic_jsonl(self) -> None:
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

    def test_streaming_jsonl_writer(self) -> None:
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

    def test_read_jsonl_error_action(self) -> None:
        """Test error actions."""
        import pydantic
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

    def test_read_tsv(self) -> None:
        """Test reading pydantic models from a TSV file."""
        from pydantic import BaseModel

        class Model(BaseModel):
            """A test model."""

            name: str
            date: datetime.date

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).joinpath("data.tsv")
            with path.open("w") as file:
                print("name", "date", sep="\t", file=file)
                print("1", "2025-01-01", sep="\t", file=file)
                print("2", "2025-01-02", sep="\t", file=file)
                print("3", "2025-01-03", sep="\t", file=file)

            self.assertEqual(
                [
                    Model(name="1", date="2025-01-01"),
                    Model(name="2", date="2025-01-02"),
                    Model(name="3", date="2025-01-03"),
                ],
                list(read_pydantic_tsv(path, Model)),
            )

    def test_read_tsv_with_processing(self) -> None:
        """Test reading pydantic models from a TSV file with custom row-based processing."""
        from pydantic import BaseModel

        class Model(BaseModel):
            """A test model."""

            name: str
            date: datetime.date

        def _process(record: dict[str, Any]) -> dict[str, Any]:
            date_value = record.pop("date")
            if len(record) == 4:
                record["date"] = f"{date_value}-01-01"
            else:
                record["date"] = date_value
            return record

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).joinpath("data.tsv")
            with path.open("w") as file:
                print("name", "date", sep="\t", file=file)
                print("1", "2025", sep="\t", file=file)
                print("2", "2025-01-02", sep="\t", file=file)
                print("3", "2025-01-03", sep="\t", file=file)

            self.assertEqual(
                [
                    Model(name="1", date="2025-01-01"),
                    Model(name="2", date="2025-01-02"),
                    Model(name="3", date="2025-01-03"),
                ],
                list(read_pydantic_tsv(path, Model, process=_process)),
            )
