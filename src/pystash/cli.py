# -*- coding: utf-8 -*-

"""Command line interface for PyStash."""

import os

import click

from .api import get


@click.command()
@click.argument('keys', nargs=-1)
def main(keys: str):
    """List a PyStash directory."""
    directory = get(*keys)
    click.secho(f'[pystash] {directory}', fg='cyan', bold=True)
    os.system(f'ls -al {directory}')  # noqa:S605


if __name__ == '__main__':
    main()
