Command Line Tools
==================

``pb_process_tools`` provides two standalone CLI utilities (``pbpt_gen_template.py`` and ``pbpt_clean_db.py``) plus standardised CLI interfaces on generated ``GenCmds`` and ``ProcessCmd`` scripts.

pbpt_gen_template.py
--------------------

Scaffolds a complete workflow directory containing:

* ``<prefix>_gen_cmds.py`` and ``<prefix>_perform_analysis.py`` (formatted with ``black`` if installed)
* Helper shell wrappers with argument forwarding (``"$@"``): ``run_gen_cmds.sh``, ``run_status.sh``, ``run_errors.sh``, ``run_report.sh``, ``run_reset_errs.sh``, and ``run_checks.sh``.

Options::

    pbpt_gen_template.py -o OUTPUT -t TABLE [options]

* ``-o``, ``--output``: Output directory where files will be created (required).
* ``-t``, ``--table``: PostgreSQL table name for this task's job queue (required; use a distinct name per concurrent task).
* ``--schema``: Optional PostgreSQL schema name for the queue table.
* ``-d``, ``--dbfile``: Path to a text file containing the PostgreSQL connection string (optional if ``PBPT_DB_CONN_STR`` is set).
* ``-p``, ``--prefix``: Optional filename prefix for generated Python, shell, and DB info files.
* ``--slurm``: Configure ``gen_cmds.py`` to generate Slurm sbatch scripts instead of GNU Parallel shell scripts.
* ``--partition``: Slurm partition name when using ``--slurm`` (default: ``compute``).
* ``--array``: Generate a single Slurm job array script (``#SBATCH --array=0-(N-1)[%max_concurrent]``).
* ``--max_concurrent``: Optional maximum concurrent array tasks (used with ``--array``).
* ``--singularity``: Container ``.sif`` image path to wrap worker commands with ``singularity exec``.
* ``--sing_paths``: Optional bind mount paths passed to Singularity (``--bind <path>``).
* ``--overwrite``: Overwrite existing template files if present.


pbpt_clean_db.py (or pbpt_clean_db)
------------------------------------

Inspects a PostgreSQL database for ``pb_process_tools`` job queue tables and safely removes leftover tables without touching unrelated application tables. Active tables (with pending or running jobs) are skipped unless ``--force`` is provided.

Options::

    pbpt_clean_db.py [-d DBFILE | --db-conn-str URL] [options]

Connection & Scope:

* ``-d``, ``--dbfile``: File containing the PostgreSQL connection string (falls back to ``PBPT_DB_CONN_STR``).
* ``--db-conn-str``: Direct SQLAlchemy PostgreSQL connection string.
* ``--schema``: Specific PostgreSQL schema to inspect (default: default/``public`` schema).
* ``--all-schemas``: Scan all non-system PostgreSQL schemas.

Listing & Output:

* ``-l``, ``--list``: List discovered queue tables, job counts, state, last activity, and size (default action).
* ``--json``: Print output in JSON format.

Cleanup & Filtering:

* ``--drop``: Drop matching ``pb_process_tools`` queue tables.
* ``-i``, ``--interactive``: Interactively prompt ``[y/N/q]`` for each matching table.
* ``-t``, ``--tables``: Specific table name(s) or ``schema.table`` entries to target.
* ``--pattern``: Glob pattern to match table names (e.g., ``landsat_*``).
* ``--completed``: Select queue tables where all jobs completed with zero errors.
* ``--empty``: Select empty queue tables (0 jobs).
* ``--finished``: Select idle/finished queue tables with no pending or running jobs.
* ``--older-than DAYS``: Restrict selection to tables whose latest activity is at least ``DAYS`` old.
* ``--all``: Select all discovered queue tables.
* ``--force``: Allow dropping ``ACTIVE`` tables that still have pending or running jobs.
* ``--drop-empty-schemas``: Drop custom schemas left empty after removing tables (never drops ``public`` or system schemas).
* ``--vacuum``: Run PostgreSQL ``VACUUM ANALYZE`` after cleanup (or standalone).
* ``--dry-run``: Preview matching tables without modifying the database.
* ``-y``, ``--yes``: Confirm non-interactive ``--drop`` without prompting.


GenCmds Script CLI Options
--------------------------

Any script calling ``PBPTGenQProcessToolCmds.parse_cmds()`` supports:

* ``--gen``: Run ``run_gen_commands()`` (recreates table unless ``--append`` is set).
* ``--append``: Insert only new jobs without dropping the existing table.
* ``--no-dedup``: Skip duplicate detection when using ``--append``.
* ``--dry-run``: Preview ``--gen`` / ``--append`` counts and script paths without modifying DB or files.
* ``--status`` / ``--watch [--interval SECS]``: Print one-off or live-polling queue counts, completion %, throughput, and ETA.
* ``--check [-w WORKERS] [--chkoutfiles]``: Verify outputs for unchecked jobs, optionally across ``WORKERS`` threads.
* ``--errors [--limit N]``: Print errored and incomplete jobs directly to the terminal.
* ``--inspect PID``: Inspect parameters, timestamps, and error tracebacks for a single ``PID``.
* ``--report [-o FILE]``: Output a full JSON status report to console or ``FILE``.
* ``--reset (--all | --error | --pids ...)``: Reset DB job state without deleting output files.
* ``--rmouts (--all | --error | --pids ...)``: Delete output files via ``remove_outputs()`` and reset DB job state.
* ``--drop [-y]``: Drop this task's queue table from PostgreSQL.
* ``--table NAME`` / ``--schema NAME``: Override target queue table/schema from the CLI.


Worker Script CLI Options
-------------------------

Any worker script calling ``PBPTQProcessTool.std_run()`` supports:

* ``--dbinfo FILE``: Path to the JSON DB connection metadata file (``db_conn_str``, ``db_table_name``, optional ``db_schema``).
* ``-j JOBID``: Run or inspect a single job PID in debug mode (does not update DB status).
* ``-p``: With ``-j``, print the job's parameter dictionary.
* ``-r``: With ``-j``, execute ``remove_outputs()`` for that single job.
