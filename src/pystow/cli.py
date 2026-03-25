"""Command line interface for PyStow."""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

import click


@click.group()
def main() -> None:
    """Run the PyStow CLI."""


@main.command()
@click.argument("keys", nargs=-1)
@click.option("--name")
def join(keys: Sequence[str], name: str | None) -> None:
    """List a directory."""
    from . import api

    click.echo(api.join(*keys, name=name))


@main.command()
@click.argument("keys", nargs=-1)
def ls(keys: Sequence[str]) -> None:
    """List a directory."""
    from . import api

    directory = api.join(*keys)
    _ls(directory)


@main.command()
@click.argument("keys", nargs=-1)
@click.option("--url", required=True)
@click.option("--name")
@click.option("--force", is_flag=True)
def ensure(keys: Sequence[str], url: str, name: str | None, force: bool) -> None:
    """Ensure a file is downloaded."""
    from . import api

    path = api.ensure(*keys, url=url, name=name, force=force)
    _ls(path.parent)


def _ls(directory: Path) -> None:
    command = f"ls -al {directory}"
    click.secho(f"[pystow] {command}", fg="cyan", bold=True)
    os.system(command)  # noqa:S605


@main.command(name="set")
@click.argument("module")
@click.argument("key")
@click.argument("value")
def set_config(module: str, key: str, value: str) -> None:
    """Set a configuration value."""
    from .config_api import write_config

    write_config(module, key, value)


if __name__ == "__main__":
    main()
