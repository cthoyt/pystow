<h1 align="center">
  PyStow
</h1>

<p align="center">
    <a href="https://github.com/cthoyt/pystow/actions/workflows/tests.yml">
        <img alt="Tests" src="https://github.com/cthoyt/pystow/actions/workflows/tests.yml/badge.svg" /></a>
    <a href="https://pypi.org/project/pystow">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/pystow" /></a>
    <a href="https://pypi.org/project/pystow">
        <img alt="PyPI - Python Version" src="https://img.shields.io/pypi/pyversions/pystow" /></a>
    <a href="https://github.com/cthoyt/pystow/blob/main/LICENSE">
        <img alt="PyPI - License" src="https://img.shields.io/pypi/l/pystow" /></a>
    <a href='https://pystow.readthedocs.io/en/latest/?badge=latest'>
        <img src='https://readthedocs.org/projects/pystow/badge/?version=latest' alt='Documentation Status' /></a>
    <a href="https://codecov.io/gh/cthoyt/pystow/branch/main">
        <img src="https://codecov.io/gh/cthoyt/pystow/branch/main/graph/badge.svg" alt="Codecov status" /></a>  
    <a href="https://github.com/cthoyt/cookiecutter-python-package">
        <img alt="Cookiecutter template from @cthoyt" src="https://img.shields.io/badge/Cookiecutter-snekpack-blue" /></a>
    <a href="https://github.com/astral-sh/ruff">
        <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="Ruff" style="max-width:100%;"></a>
    <a href="https://github.com/cthoyt/pystow/blob/main/.github/CODE_OF_CONDUCT.md">
        <img src="https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg" alt="Contributor Covenant"/></a>
    <a href="https://zenodo.org/badge/latestdoi/318194121">
        <img src="https://zenodo.org/badge/318194121.svg" alt="DOI"></a>
</p>

👜 Easily pick a place to store data for your Python code

## 💪 Getting Started

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

Get a file path for your application by adding the `name` keyword argument. This
is made explicit so PyStow knows which parent directories to automatically
create. This works with `pystow` or any module you create with `pystow.module`.

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

Ensure a tabular data file from the internet and load it for usage (requires
`pip install pandas`):

```python
import pystow
import pandas as pd

url = 'https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt'
df: pd.DataFrame = pystow.ensure_csv('pykeen', 'datasets', 'nations', url=url)
```

Ensure a comma-separated tabular data file from the internet and load it for
usage (requires `pip install pandas`):

```python
import pystow
import pandas as pd

url = 'https://raw.githubusercontent.com/cthoyt/pystow/main/tests/resources/test_1.csv'
df: pd.DataFrame = pystow.ensure_csv('pykeen', 'datasets', 'nations', url=url, read_csv_kwargs=dict(sep=","))
```

Ensure a RDF file from the internet and load it for usage (requires
`pip install rdflib`)

```python
import pystow
import rdflib

url = 'https://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz'
rdf_graph: rdflib.Graph = pystow.ensure_rdf('rhea', url=url)
```

Also see `pystow.ensure_excel()`, `pystow.ensure_rdf()`,
`pystow.ensure_zip_df()`, and `pystow.ensure_tar_df()`.

If your data comes with a lot of different files in an archive, you can ensure
the archive is downloaded and get specific files from it:

```python
import numpy as np
import pystow

url = "https://cloud.enterprise.informatik.uni-leipzig.de/index.php/s/LHPbMCre7SLqajB/download/MultiKE_D_Y_15K_V1.zip"
# the path inside the archive to the file you want
inner_path = "MultiKE/D_Y_15K_V1/721_5fold/1/20210219183115/ent_embeds.npy"
with pystow.ensure_open_zip("kiez", url=url, inner_path=inner_path) as file:
    emb = np.load(file)
```

Also see `pystow.module.ensure_open_lzma()`,
`pystow.module.ensure_open_tarfile()` and `pystow.module.ensure_open_gz()`.

## ⚙️️ Configuration

By default, data is stored in the `$HOME/.data` directory. By default, the
`<app>` app will create the `$HOME/.data/<app>` folder.

If you want to use an alternate folder name to `.data` inside the home
directory, you can set the `PYSTOW_NAME` environment variable. For example, if
you set `PYSTOW_NAME=mydata`, then the following code for the `pykeen` app will
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

If you want to specify a completely custom directory that isn't relative to your
home directory, you can set the `PYSTOW_HOME` environment variable. For example,
if you set `PYSTOW_HOME=/usr/local/`, then the following code for the `pykeen`
app will create the `/usr/local/pykeen/` directory:

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

If you set the environment variable `PYSTOW_USE_APPDIRS` to `true` or `True`,
then the [`appdirs`](https://pypi.org/project/appdirs/) package will be used to
choose the base directory based on the `user data dir` option. This can still be
overridden by `PYSTOW_HOME`.

## 🚀 Installation

The most recent release can be installed from
[PyPI](https://pypi.org/project/pystow/) with uv:

```console
$ uv pip install pystow
```

or with pip:

```console
$ python3 -m pip install pystow
```

The most recent code and data can be installed directly from GitHub with uv:

```console
$ uv pip install git+https://github.com/cthoyt/pystow.git
```

or with pip:

```console
$  python3 -m pip install git+https://github.com/cthoyt/pystow.git
```

## 👐 Contributing

Contributions, whether filing an issue, making a pull request, or forking, are
appreciated. See
[CONTRIBUTING.md](https://github.com/cthoyt/pystow/blob/master/.github/CONTRIBUTING.md)
for more information on getting involved.

## 👋 Attribution

### ⚖️ License

The code in this package is licensed under the MIT License.

### 🍪 Cookiecutter

This package was created with
[@audreyfeldroy](https://github.com/audreyfeldroy)'s
[cookiecutter](https://github.com/cookiecutter/cookiecutter) package using
[@cthoyt](https://github.com/cthoyt)'s
[cookiecutter-snekpack](https://github.com/cthoyt/cookiecutter-snekpack)
template.

## 🛠️ For Developers

<details>
  <summary>See developer instructions</summary>

The final section of the README is for if you want to get involved by making a
code contribution.

### Development Installation

To install in development mode, use the following:

```console
$ git clone git+https://github.com/cthoyt/pystow.git
$ cd pystow
$ uv pip install -e .
```

Alternatively, install using pip:

```console
$ python3 -m pip install -e .
```

### Updating Package Boilerplate

This project uses `cruft` to keep boilerplate (i.e., configuration, contribution
guidelines, documentation configuration) up-to-date with the upstream
cookiecutter package. Install cruft with either `uv tool install cruft` or
`python3 -m pip install cruft` then run:

```console
$ cruft update
```

More info on Cruft's update command is available
[here](https://github.com/cruft/cruft?tab=readme-ov-file#updating-a-project).

### 🥼 Testing

After cloning the repository and installing `tox` with
`uv tool install tox --with tox-uv` or `python3 -m pip install tox tox-uv`, the
unit tests in the `tests/` folder can be run reproducibly with:

```console
$ tox -e py
```

Additionally, these tests are automatically re-run with each commit in a
[GitHub Action](https://github.com/cthoyt/pystow/actions?query=workflow%3ATests).

### 📖 Building the Documentation

The documentation can be built locally using the following:

```console
$ git clone git+https://github.com/cthoyt/pystow.git
$ cd pystow
$ tox -e docs
$ open docs/build/html/index.html
```

The documentation automatically installs the package as well as the `docs` extra
specified in the [`pyproject.toml`](pyproject.toml). `sphinx` plugins like
`texext` can be added there. Additionally, they need to be added to the
`extensions` list in [`docs/source/conf.py`](docs/source/conf.py).

The documentation can be deployed to [ReadTheDocs](https://readthedocs.io) using
[this guide](https://docs.readthedocs.io/en/stable/intro/import-guide.html). The
[`.readthedocs.yml`](.readthedocs.yml) YAML file contains all the configuration
you'll need. You can also set up continuous integration on GitHub to check not
only that Sphinx can build the documentation in an isolated environment (i.e.,
with `tox -e docs-test`) but also that
[ReadTheDocs can build it too](https://docs.readthedocs.io/en/stable/pull-requests.html).

#### Configuring ReadTheDocs

1. Log in to ReadTheDocs with your GitHub account to install the integration at
   https://readthedocs.org/accounts/login/?next=/dashboard/
2. Import your project by navigating to https://readthedocs.org/dashboard/import
   then clicking the plus icon next to your repository
3. You can rename the repository on the next screen using a more stylized name
   (i.e., with spaces and capital letters)
4. Click next, and you're good to go!

### 📦 Making a Release

#### Configuring Zenodo

[Zenodo](https://zenodo.org) is a long-term archival system that assigns a DOI
to each release of your package.

1. Log in to Zenodo via GitHub with this link:
   https://zenodo.org/oauth/login/github/?next=%2F. This brings you to a page
   that lists all of your organizations and asks you to approve installing the
   Zenodo app on GitHub. Click "grant" next to any organizations you want to
   enable the integration for, then click the big green "approve" button. This
   step only needs to be done once.
2. Navigate to https://zenodo.org/account/settings/github/, which lists all of
   your GitHub repositories (both in your username and any organizations you
   enabled). Click the on/off toggle for any relevant repositories. When you
   make a new repository, you'll have to come back to this

After these steps, you're ready to go! After you make "release" on GitHub (steps
for this are below), you can navigate to
https://zenodo.org/account/settings/github/repository/cthoyt/pystow to see the
DOI for the release and link to the Zenodo record for it.

#### Registering with the Python Package Index (PyPI)

You only have to do the following steps once.

1. Register for an account on the
   [Python Package Index (PyPI)](https://pypi.org/account/register)
2. Navigate to https://pypi.org/manage/account and make sure you have verified
   your email address. A verification email might not have been sent by default,
   so you might have to click the "options" dropdown next to your address to get
   to the "re-send verification email" button
3. 2-Factor authentication is required for PyPI since the end of 2023 (see this
   [blog post from PyPI](https://blog.pypi.org/posts/2023-05-25-securing-pypi-with-2fa/)).
   This means you have to first issue account recovery codes, then set up
   2-factor authentication
4. Issue an API token from https://pypi.org/manage/account/token

#### Configuring your machine's connection to PyPI

You have to do the following steps once per machine.

```console
$ uv tool install keyring
$ keyring set https://upload.pypi.org/legacy/ __token__
$ keyring set https://test.pypi.org/legacy/ __token__
```

Note that this deprecates previous workflows using `.pypirc`.

#### Uploading to PyPI

After installing the package in development mode and installing `tox` with
`uv tool install tox --with tox-uv` or `python3 -m pip install tox tox-uv`, run
the following from the console:

```console
$ tox -e finish
```

This script does the following:

1. Uses [bump-my-version](https://github.com/callowayproject/bump-my-version) to
   switch the version number in the `pyproject.toml`, `CITATION.cff`,
   `src/pystow/version.py`, and [`docs/source/conf.py`](docs/source/conf.py) to
   not have the `-dev` suffix
2. Packages the code in both a tar archive and a wheel using
   [`uv build`](https://docs.astral.sh/uv/guides/publish/#building-your-package)
3. Uploads to PyPI using
   [`uv publish`](https://docs.astral.sh/uv/guides/publish/#publishing-your-package).
4. Push to GitHub. You'll need to make a release going with the commit where the
   version was bumped.
5. Bump the version to the next patch. If you made big changes and want to bump
   the version by minor, you can use `tox -e bumpversion -- minor` after.

#### Releasing on GitHub

1. Navigate to https://github.com/cthoyt/pystow/releases/new to draft a new
   release
2. Click the "Choose a Tag" dropdown and select the tag corresponding to the
   release you just made
3. Click the "Generate Release Notes" button to get a quick outline of recent
   changes. Modify the title and description as you see fit
4. Click the big green "Publish Release" button

This will trigger Zenodo to assign a DOI to your release as well.

</details>
