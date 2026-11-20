PB Process Tools (pbprocesstools)
=================================

PB Process Tools is a set of utilities for batch processing large datasets on workstations (via GNU Parallel) or High Performance Computing (HPC) clusters (via Slurm, including Slurm job arrays), backed by a PostgreSQL job queue.

Each workflow specifies its own database table (and optional PostgreSQL schema), allowing multiple independent tasks to run concurrently from the same PostgreSQL database.

Source code is available on GitHub: https://github.com/remotesensinginfo/pb_process_tools


Information
-----------

.. toctree::
   :maxdepth: 2

   getstarted
   installation
   commandlinetools
   examples
   about


Library Documentation
---------------------

.. toctree::
   :maxdepth: 2

   pbpt_q_process
   pbpt_db_clean
   pbpt_utils


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
