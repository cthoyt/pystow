<h1 align="center">
  PyStow
</h1>

<p align="center">
  <a href="https://github.com/cthoyt/pystow/actions">
    <img src="https://github.com/cthoyt/pystow/workflows/Tests/badge.svg" alt="Build status" height="20" />
  </a>

  <a href="https://pypi.org/project/pystow">
    <img alt="PyPI - Python Version" src="https://img.shields.io/pypi/pyversions/pystow">
  </a>

  <a href='https://opensource.org/licenses/MIT'>
    <img src='https://img.shields.io/badge/License-MIT-blue.svg' alt='License'/>
  </a>

  <a href='https://pystow.readthedocs.io/en/latest/?badge=latest'>
    <img src='https://readthedocs.org/projects/pystow/badge/?version=latest' alt='Documentation Status' />
  </a>

  <a href="https://zenodo.org/badge/latestdoi/318194121">
    <img src="https://zenodo.org/badge/318194121.svg" alt="DOI">
  </a>

  <a href="https://github.com/psf/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Code style: black">
  </a>
</p>

👜 Easily pick a place to store data for your python code.

## 🚀 Getting Started

Get a directory for your application.

```python
import pystow

# Get a directory (as a pathlib.Path) for ~/.data/pykeen
pykeen_directory = pystow.join('pykeen')

# Get a subdirectory (as a pathlib.Path) for ~/.data/pykeen/experiments
pykeen_experiments_directory = pystow.join('pykeen', 'experiments')

# You can go as deep as you want
pykeen_deep_directory = pystow.join('pykeen', 'experiments', 'a', 'b', 'c')
```

If you reuse the same directory structure a lot, you can save them in a module:

```python
import pystow

pykeen_module = pystow.module("pykeen")

# Access the module's directory with .base
assert pystow.join("pykeen") == pystow.module("pykeen").base

# Get a subdirectory (as a pathlib.Path) for ~/.data/pykeen/experiments
pykeen_experiments_directory = pykeen_module.join('experiments')

# You can go as deep as you want past the original "pykeen" module
pykeen_deep_directory = pykeen_module.join('experiments', 'a', 'b', 'c')
```

Get a file path for your application by adding the `name` keyword argument. This is made explicit so PyStow knows which
parent directories to automatically create. This works with `pystow` or any module you create with `pystow.module`.

```python
import pystow

# Get a directory (as a pathlib.Path) for ~/.data/indra/database.tsv
indra_database_path = pystow.join('indra', 'database', name='database.tsv')
```

Ensure a file from the internet is available in your application's directory:

```python
import pystow

url = 'https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt'
path = pystow.ensure('pykeen', 'datasets', 'nations', url=url)
```

Ensure a tabular data file from the internet and load it for usage (requires `pip install pandas`):

```python
import pystow
import pandas as pd

url = 'https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt'
df: pd.DataFrame = pystow.ensure_csv('pykeen', 'datasets', 'nations', url=url)
```

Ensure a RDF file from the internet and load it for usage (requires `pip install rdflib`)

```python
import pystow
import rdflib

url = 'https://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz'
rdf_graph: rdflib.Graph = pystow.ensure_rdf('rhea', url=url)
```

Also see `pystow.ensure_excel()`, `pystow.ensure_rdf()`, `pystow.ensure_zip_df()`, and `pystow.ensure_tar_df()`.

## ⚙️️ Configuration

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

### X Desktop Group (XDG) Compatibility

While PyStow's main goal is to make application data less opaque and less
hidden, some users might want to use the
[XDG specifications](http://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html)
for storing their app data.

If you set `PYSTOW_USE_APPDIRS` to `true` or `True`, then the
[`appdirs`](https://pypi.org/project/appdirs/) package will be used to choose
the base directory based on the `user data dir` option. This can still be
overridden by `PYSTOW_HOME`.

## 🚀 Installation

The most recent release can be installed from
[PyPI](https://pypi.org/project/pystow/) with:

```bash
$ pip install pystow
```

The most recent code and data can be installed directly from GitHub with:

```bash
$ pip install git+https://github.com/cthoyt/pystow.git
```

To install in development mode, use the following:

```bash
$ git clone git+https://github.com/cthoyt/pystow.git
$ cd pystow
$ pip install -e .
```

## ⚖️ License

The code in this package is licensed under the MIT License.
