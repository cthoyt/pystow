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

👜 Easily pick a place to store data to go with your python package.

🚀 Install with: `pip install pystow`

💪 Example usage:

```python
import pystow.api
import pystow

# Get a directory (as a pathlib.Path) for ~/.data/pykeen
pykeen_directory = pystow.api.get('pykeen')

# Get a subdirectory (as a pathlib.Path) for ~/.data/pykeen/experiments
pykeen_experiments_directory = pystow.api.get('pykeen', 'experiments')

# You can go as deep as you want
pykeen_experiments_directory = pystow.api.get('pykeen', 'experiments', 'a', 'b', 'c')
```

⚠️ Data gets stored in `~/.data` by default. If you want to change the name of the directory, set the environment
variable `PYSTOW_NAME`. If you want to change the default parent directory to be other than the home directory,
set `PYSTOW_HOME`
