Installation
============

The most recent release can be installed from `PyPI <https://pypi.org/project/pystow>`_
with uv:

.. code-block:: console

    $ uv pip install pystow

or with pip:

.. code-block:: console

    $ python3 -m pip install pystow

Installing from git
-------------------

The most recent code and data can be installed directly from GitHub with uv:

.. code-block:: console

    $ uv pip install git+https://github.com/cthoyt/pystow.git

or with pip:

.. code-block:: console

    $ python3 -m pip install git+https://github.com/cthoyt/pystow.git

Installing for development
--------------------------

To install in development mode with uv:

.. code-block:: console

    $ git clone git+https://github.com/cthoyt/pystow.git
    $ cd pystow
    $ uv pip install -e .

or with pip:

.. code-block:: console

    $ python3 -m pip install -e .

Configuration
=============

By default, data is stored in the ``$HOME/.data`` directory. By default, the ``<app>``
app will create the ``$HOME/.data/<app>`` folder.

If you want to use an alternate folder name to ``.data`` inside the home directory, you
can set the ``PYSTOW_NAME`` environment variable. For example, if you set
``PYSTOW_NAME=mydata``, then the following code for the ``pykeen`` app will create the
``$HOME/mydata/pykeen/`` directory:

.. code-block:: python

    import os
    import pystow

    # Only for demonstration purposes. You should set environment
    # variables either with your .bashrc or in the command line REPL.
    os.environ["PYSTOW_NAME"] = "mydata"

    # Get a directory (as a pathlib.Path) for ~/mydata/pykeen
    pykeen_directory = pystow.join("pykeen")

If you want to specify a completely custom directory that isn't relative to your home
directory, you can set the ``PYSTOW_HOME`` environment variable. For example, if you set
``PYSTOW_HOME=/usr/local/``, then the following code for the ``pykeen`` app will create
the ``/usr/local/pykeen/`` directory:

.. code-block:: python

    import os
    import pystow

    # Only for demonstration purposes. You should set environment
    # variables either with your .bashrc or in the command line REPL.
    os.environ["PYSTOW_HOME"] = "/usr/local/"

    # Get a directory (as a pathlib.Path) for /usr/local/pykeen
    pykeen_directory = pystow.join("pykeen")

.. warning::

    If you set ``PYSTOW_HOME``, then ``PYSTOW_NAME`` is disregarded.

X Desktop Group (XDG) Compatibility
-----------------------------------

While PyStow's main goal is to make application data less opaque and less hidden, some
users might want to use the `XDG specifications
<http://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html>`_ for storing
their app data.

If you set the environment variable ``PYSTOW_USE_APPDIRS`` to ``true`` or ``True``, then
the `appdirs <https://pypi.org/project/appdirs>`_ or `platformdirs
<https://pypi.org/project/platformdirs>`_ package will be used to choose the base
directory based on the ``user data dir`` option.

.. warning::

    If you use this setting, make sure you first do ``pip install platformdirs``

.. note::

    This can still be overridden by ``PYSTOW_HOME``.
