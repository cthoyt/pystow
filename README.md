<h1 align="center">
  PyStow
</h1>

<p align="center">
  <a href='https://opensource.org/licenses/MIT'>
    <img src='https://img.shields.io/badge/License-MIT-blue.svg' alt='License'/>
  </a>

  <a href="https://zenodo.org/badge/latestdoi/318194121">
    <img src="https://zenodo.org/badge/318194121.svg" alt="DOI">
  </a>
</p>

👜 Easily pick a place to store data for your python package.

## 🚀 Installation

`pip install pystow`

## 💪 Usage

Get a directory for your application.

```python
import pystow

# Get a directory (as a pathlib.Path) for ~/.data/pykeen
pykeen_directory = pystow.get('pykeen')

# Get a subdirectory (as a pathlib.Path) for ~/.data/pykeen/experiments
pykeen_experiments_directory = pystow.get('pykeen', 'experiments')

# You can go as deep as you want
pykeen_deep_directory = pystow.get('pykeen', 'experiments', 'a', 'b', 'c')
```

Ensure a file from the internet is available in your application's directory:

```python
import pystow

url = 'https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt'
path = pystow.ensure('pykeen', 'datasets', 'nations', url=url)
```

Ensure a file from the internet and load it for usage:

```python
import pystow

url = 'https://raw.githubusercontent.com/pykeen/pykeen/master/src/pykeen/datasets/nations/test.txt'
df = pystow.ensure_csv('pykeen', 'datasets', 'nations', url=url)


```

Also see `pystow.ensure_excel()`, `pystow.ensure_zip_df()`, and `pystow.ensure_tar_df()` .

## ⚠️ Configuration

Data gets stored in `~/.data` by default. If you want to change the name of the directory, set the environment
variable `PYSTOW_NAME`. If you want to change the default parent directory to be other than the home directory,
set `PYSTOW_HOME`
