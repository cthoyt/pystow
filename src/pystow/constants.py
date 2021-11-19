# -*- coding: utf-8 -*-

"""PyStow constants."""

from textwrap import dedent

__all__ = [
    "PYSTOW_NAME_ENVVAR",
    "PYSTOW_HOME_ENVVAR",
    "PYSTOW_USE_APPDIRS",
    "PYSTOW_NAME_DEFAULT",
    "README_TEXT",
]

PYSTOW_NAME_ENVVAR = "PYSTOW_NAME"
PYSTOW_HOME_ENVVAR = "PYSTOW_HOME"
PYSTOW_USE_APPDIRS = "PYSTOW_USE_APPDIRS"
PYSTOW_NAME_DEFAULT = ".data"
README_TEXT = dedent(
    """\
# PyStow Data Directory

This directory is used by [`pystow`](https://github.com/cthoyt/pystow) as a
reproducible location to store and access data.

### ⚙️️ Configuration

By default, data is stored in the `$HOME/.data` directory. By default, the `<app>` app will create the
`$HOME/.data/<app>` folder.

If you want to use an alternate folder name to `.data` inside the home directory, you can set the `PYSTOW_NAME`
environment variable. For example, if you set `PYSTOW_NAME=mydata`, then the following code for the `pykeen` app will
create the `$HOME/mydata/pykeen/` directory:

```python
import os
import pystow

# Only for demonstration purposes. You should set environment
# variables either with your .bashrc or in the command line REPL.
os.environ['PYSTOW_NAME'] = 'mydata'

# Get a directory (as a pathlib.Path) for ~/mydata/pykeen
pykeen_directory = pystow.join('pykeen')
```

If you want to specify a completely custom directory that isn't relative to your home directory, you can set
the `PYSTOW_HOME` environment variable. For example, if you set `PYSTOW_HOME=/usr/local/`, then the following code for
the `pykeen` app will create the `/usr/local/pykeen/` directory:

```python
import os
import pystow

# Only for demonstration purposes. You should set environment
# variables either with your .bashrc or in the command line REPL.
os.environ['PYSTOW_HOME'] = '/usr/local/'

# Get a directory (as a pathlib.Path) for /usr/local/pykeen
pykeen_directory = pystow.join('pykeen')
```

Note: if you set `PYSTOW_HOME`, then `PYSTOW_NAME` is disregarded.
"""
)
