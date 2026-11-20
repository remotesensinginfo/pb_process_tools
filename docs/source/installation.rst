Installation
============

Requirements
------------

* Python 3.9 or later (CI tested on Python 3.10, 3.11, 3.12, 3.13, and 3.14)
* PostgreSQL database server (for production job queues)
* Core Python packages: ``sqlalchemy>=2.0``, ``tqdm``, ``jinja2``
* PostgreSQL driver: ``psycopg2-binary`` (or ``psycopg2``)

pip (from source)
-----------------

``pb_process_tools`` is packaged with ``pyproject.toml``. From the repository root directory:

.. code-block:: bash

    pip install .

To include the PostgreSQL driver or development/test tools:

.. code-block:: bash

    pip install ".[postgres]"
    pip install ".[dev]"

Conda
-----

You can also install dependencies via conda-forge::

    conda install -c conda-forge sqlalchemy tqdm jinja2 psycopg2

and then install ``pb_process_tools`` with ``pip install . --no-deps``.

Database Configuration
----------------------

v4.0.0 uses PostgreSQL exclusively for production job queues (SQLite queue support was removed).
Provide a SQLAlchemy PostgreSQL connection URL either:

1. In a text file passed via ``db_conn_file`` / ``--dbfile``::

    postgresql+psycopg2://username:password@hostname:5432/database_name

2. Or via the ``PBPT_DB_CONN_STR`` environment variable::

    export PBPT_DB_CONN_STR="postgresql+psycopg2://username:password@hostname:5432/database_name"

Singularity / Apptainer
-----------------------

When executing workflows inside a container on an HPC cluster, pass ``--singularity`` (and optionally ``--sing_paths``) to ``pbpt_gen_template.py`` to automatically wrap worker invocations and load the ``singularity`` module in generated Slurm batch scripts::

    singularity build au-eoed.sif docker://petebunting/au-eoed