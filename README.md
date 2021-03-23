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
</p>

👜 Easily pick a place to store data for your python package.

## 💪 Usage

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

### ⚠️ Configuration

Data gets stored in `~/.data` by default. If you want to change the name of the directory, set the environment
variable `PYSTOW_NAME`. If you want to change the default parent directory to be other than the home directory,
set `PYSTOW_HOME`

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
