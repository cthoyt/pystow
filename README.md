# PyStash

Easily pick a place to store data to go with your python package.

Install with: `pip install pystash`

Example usage:

```python
import pystash

# Get a directory (as a pathlib.Path)
pykeen_directory = pystash.get('pykeen')

# Get a subdirectory (as a pathlib.Path).
# You can specify as deep as you want.
pykeen_experiments_directory = pystash.get('pykeen', 'experiments')
```
