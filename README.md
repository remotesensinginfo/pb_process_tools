# PB Process Tools

![example workflow](https://github.com/remotesensinginfo/pb_process_tools/actions/workflows/main.yml/badge.svg)

Tools for batch processing workflows as a set of jobs backed by a PostgreSQL job queue. Workflows can execute either on an HPC cluster using Slurm (including job arrays) or on a local workstation using GNU Parallel.

## Installation

Install `pb_process_tools` (v4.0.0) from the source directory (where `pyproject.toml` is located):

```bash
pip install .
```

Include PostgreSQL driver support (`psycopg2-binary`) or development/test dependencies:

```bash
pip install ".[postgres]"
pip install ".[dev]"
```

## Quick Start

1. Provide your PostgreSQL SQLAlchemy connection string (for example in a file `db_conn.txt` containing `postgresql+psycopg2://user:password@host:5432/dbname`, or via the `PBPT_DB_CONN_STR` environment variable).
2. Generate a template workflow with a unique `--table` name for the task:

```bash
pbpt_gen_template.py --output /path/to/out/dir --dbfile db_conn.txt --table my_task_jobs
```

3. Implement `ProcessCmd.do_processing()` in `perform_analysis.py` and job discovery in `GenCmds.gen_command_info()` in `gen_cmds.py`.
4. Populate the queue, run, monitor, verify outputs, and clean up:

```bash
./run_gen_cmds.sh                # Populate queue & generate execution scripts
./run_exe_analysis.sh            # Run jobs (GNU Parallel or sbatch)
./run_status.sh --watch          # Monitor live progress, throughput, and ETA
./run_checks.sh -w 8             # Parallel output verification
./run_errors.sh                  # Inspect errored/incomplete jobs
pbpt_clean_db.py -d db_conn.txt --drop --completed -y   # Tidy leftover queue tables
```
