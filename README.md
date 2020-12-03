# PyStash

Easy configuration of storing stuff to go with your python package

Install with: `pip install pystash`

Example usage:

```python
import pystash

# Get a directory (as a pathlib.Path)
pykeen_directory = pystash.get_directory('pykeen')

# Get a subdirectory (as a pathlib.Path)
pykeen_experiments_directory = pystash.get_directory('pykeen', 'experiments')
```
