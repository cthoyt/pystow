# -*- coding: utf-8 -*-

"""Command line interface for PyStow."""

import os
from typing import Optional, Sequence

import click

from . import api


@click.group()
def main():
    """Run the PyStow CLI."""


def _ls(directory):
    command = f'ls -al {directory}'
    click.secho(f'[pystow] {command}', fg='cyan', bold=True)
    os.system(command)  # noqa:S605


@main.command()
@click.argument('keys', nargs=-1)
def join(keys: Sequence[str]):
    """List a directory."""
    directory = api.join(*keys)
    _ls(directory)


@main.command()
@click.argument('keys', nargs=-1)
@click.option('--url', required=True)
@click.option('--name')
@click.option('--force', is_flag=True)
def ensure(keys: Sequence[str], url: str, name: Optional[str], force: bool):
    """Ensure a file is downloaded."""
    path = api.ensure(*keys, url=url, name=name, force=force)
    _ls(path.parent)


if __name__ == '__main__':
    main()
