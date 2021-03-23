PyStow |release| Documentation
==============================
If you've ever written the following few lines of code, :mod:`pystow` is for you:

.. code-block:: python

    import os
    home = os.path.expanduser('~')
    project_name = 'adeft'
    envvar_name = f'{project_name.upper()}_HOME'
    if envvar_name in os.environ:
        ADEFT_HOME = os.environ[envvar_name]
    else:
        ADEFT_HOME = os.path.join(home, f'.{project_name}')
    os.makedirs(ADEFT_HOME, exist_ok=True)

Many projects (let's use `Adeft <https://github.com/indralab/adeft>`_ as an example) create a folder in the home
directory as a dot-file such as ``$HOME/.adeft``. I found that I had so many of these that I stared grouping
them inside a ``$HOME/.data`` folder. It's also the case that every time you create one of these folders,
you need to ensure its existence.

:mod:`pystow` takes care of these things. You can replace the previous code with:

.. code-block:: python

    import pystow
    ADEFT_HOME = pystow.join('adeft')

First, it takes the name of the module, uppercases it, and postpends ``_HOME`` on to it (e.g., ``ADEFT_HOME``)
and looks in the environment. If this variable is available, it uses that as the directory. It ensures it
exists, then returns a :class:`pathlib.Path` pointing to it.

If ``ADEFT_HOME`` (or more generally, ``<MODULENAME>_HOME`` is not available in the environment, it picks the
path as ``$HOME/.data/<module name>``. Normally, ``$HOME`` is specified in your OS. However, if you want to
pick another location to stick the data, you can override using ``$HOME`` by setting ``$PYSTOW_HOME`` in
the environment.

If you want to go more directories deep inside the adeft default directory, you can just keep using more
positional arguments (the same semantics as :func:`os.path.join`). These directories automatically
get created as well.

.. code-block:: python

    >>> import pystow
    >>> from pathlib import Path
    >>> # already set somewhere
    >>> __version__ = ...
    >>> ADEFT_VERSION_HOME: Path = pystow.join('adeft', __version__)

.. toctree::
   :maxdepth: 2
   :caption: Getting Started
   :name: start

   installation
   usage
   utils
   cli

Indices and Tables
------------------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
