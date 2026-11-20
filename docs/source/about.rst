About
=====

These tools are a collection of utilities developed to support large-scale batch data processing across workstations and HPC clusters. In v4.0.0, the queue backend has been modernised around SQLAlchemy 2.0 and PostgreSQL, providing:

* Atomic worker job claiming using PostgreSQL ``FOR UPDATE SKIP LOCKED``.
* Per-task queue table (and optional schema) isolation so multiple independent workflows can share a single PostgreSQL database concurrently.
* Incremental job appending with deduplication, live ``--status`` / ``--watch`` monitoring, parallel output checks (``--check -w``), error inspection, Slurm job arrays, and a dedicated database cleanup CLI (``pbpt_clean_db.py``).

If you have comments, bug reports, or suggestions, please open an issue on GitHub:
https://github.com/remotesensinginfo/pb_process_tools/issues.
