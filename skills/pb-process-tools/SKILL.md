---
name: pb-process-tools
description: Generate, scaffold, debug, and run batch data processing workflows using `pb_process_tools` (`pbprocesstools` v4.x) backed by a PostgreSQL job queue, GNU Parallel, or Slurm (including job arrays and Singularity). Use whenever writing `PBPTQProcessTool` worker scripts, `PBPTGenQProcessToolCmds` command/queue generators, `pbpt_gen_template.py` scaffolds, `pbpt_clean_db.py` cleanup commands, or pairing `pb_process_tools` with `rsgispy` for geospatial batch processing.
---

# `pb_process_tools` Code Generation Guide (v4.0.2)

`pb_process_tools` (`pbprocesstools`) orchestrates large batch processing pipelines using a **PostgreSQL job queue** backed by SQLAlchemy 2.0 (`FOR UPDATE SKIP LOCKED`) and either **GNU Parallel** (local workstations) or **Slurm** (HPC clusters, optionally with job arrays and Singularity/Apptainer).

> **Important v4.x rules:**
> - **PostgreSQL only**: SQLite production queues (`sqlite_db_file`) were removed in v4.0.0. Passing `sqlite_db_file` raises `ValueError`. Configure PostgreSQL via `db_conn_file`, `db_conn_str`, or `PBPT_DB_CONN_STR`.
> - **Per-task table isolation**: Every `PBPTGenQProcessToolCmds` workflow **must** specify a non-empty `db_table_name` (and optionally `db_schema`) so concurrent tasks can share the same PostgreSQL database safely.
> - **Use `rsgispy`, never `rsgislib`**: Dataset validation (`gdal_image`, `gdal_vector`, `hdf5`), string/file utilities (`PBPTUtils`, `PBPTProcessToolsBase`), and geospatial processing examples all use `rsgispy`.
> - **`lock_file_path` is deprecated/ignored**: PostgreSQL row-level locking (`FOR UPDATE SKIP LOCKED`) replaces filesystem lock files for queues.

---

## Installation & Environment Setup

Requires Python >= 3.9 (`sqlalchemy>=2.0`, `tqdm`, `jinja2`, `rsgispy`) plus a PostgreSQL driver (`psycopg2-binary` or `psycopg2`):

```bash
pip install ".[postgres]"          # From pb_process_tools source root
# Or with conda-forge:
conda install -c conda-forge sqlalchemy tqdm jinja2 psycopg2 rsgispy
pip install . --no-deps
```

Provide your PostgreSQL SQLAlchemy connection URL (`postgresql+psycopg2://user:password@host:5432/dbname`) in **one** of three ways (resolution order):
1. `db_conn_str="..."` passed directly in Python (or `--db-conn-str` in `pbpt_clean_db.py`)
2. `db_conn_file="db_conn.txt"` (or `--dbfile` / `-d`) pointing to a single-line text file
3. `PBPT_DB_CONN_STR` environment variable

Optional logging env vars:
- `PBPT_LOG_LVL`: `INFO` (default), `DEBUG`, `WARNING`, `ERROR`, `CRITICAL`
- `PBPT_LOG_CFG`: Path to a JSON `dictConfig` file (see `share/loggingconfig.json`)

---

## Architecture Overview

A standard workflow consists of two Python scripts (scaffolded via `pbpt_gen_template.py` or written directly):

1. **Worker script** (`perform_analysis.py`) subclassing [`PBPTQProcessTool`](references/api-reference.md#pbptqprocesstool-worker-base-class):
   - Reads DB connection info from `--dbinfo <file.json>` (written with `0o600` permissions by the generator).
   - Claims pending rows (`Started=False, Completed=False, Error=False`) atomically via `FOR UPDATE SKIP LOCKED` in `std_run()`.
   - Validates `required_fields()`, runs `do_processing()`, marks `Completed=True` (and touches `self.params["out_cmp_file"]` if that key is present), or records exception tracebacks into `ErrorInfo`.
   - Implements `outputs_present()` and `remove_outputs()` for `--check` and `--rmouts` / `-j <PID> -r`.
2. **Generator & queue controller** (`gen_cmds.py`) subclassing [`PBPTGenQProcessToolCmds`](references/api-reference.md#pbptgenqprocesstoolcmds-queue--script-generator-base-class):
   - Discovers inputs in `gen_command_info()` and appends JSON-serializable parameter dicts to `self.params`.
   - Calls `self.pop_params_db()` (drops & recreates table by default, or creates-if-missing + SHA-256 deduplicates under `--append`) and writes execution scripts via `self.create_shell_exe(...)` or `self.create_slurm_sub_sh(...)`.
   - Exposes full queue lifecycle commands via `parse_cmds()`.

---

## Quick Scaffolding with `pbpt_gen_template.py`

Prefer scaffolding new projects with `pbpt_gen_template.py` when starting from scratch:

```bash
# Local GNU Parallel workflow
pbpt_gen_template.py \
    --output ./my_workflow \
    --dbfile db_conn.txt \
    --table my_task_jobs \
    --prefix task

# Slurm job array workflow with Singularity
pbpt_gen_template.py \
    --output ./my_workflow \
    --dbfile db_conn.txt \
    --table my_task_jobs \
    --schema eo_workflows \
    --prefix task \
    --slurm --partition compute --array --max_concurrent 20 \
    --singularity /path/to/container.sif \
    --sing_paths /data:/data /scratch:/scratch
```

- `--dbfile` is optional if `PBPT_DB_CONN_STR` is set in the environment.
- Generates `<prefix>_perform_analysis.py`, `<prefix>_gen_cmds.py`, and executable wrappers (`run_gen_cmds.sh`, `run_status.sh`, `run_errors.sh`, `run_report.sh`, `run_reset_errs.sh`, `run_checks.sh`) that forward `"$@"` flags. Pass `--overwrite` to regenerate existing files.

---

## Code Generation Patterns

### 1. Worker Script Pattern (`PBPTQProcessTool`)

Always implement all four abstract methods (`do_processing`, `required_fields`, `outputs_present`, `remove_outputs`) and end with `if __name__ == "__main__": ProcessCmd().std_run()`.

```python
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTQProcessTool

logger = logging.getLogger(__name__)


class ProcessCmd(PBPTQProcessTool):
    def __init__(self):
        super().__init__(cmd_name="perform_analysis.py", descript="Process dataset items.")

    def do_processing(self, **kwargs):
        in_file = self.params["in_file"]
        out_file = self.params["out_file"]
        os.makedirs(os.path.dirname(os.path.abspath(out_file)), exist_ok=True)
        logger.info("Processing %s -> %s", in_file, out_file)
        # Implement per-job processing using self.params[...] and optional **kwargs

    def required_fields(self, **kwargs):
        return ["in_file", "out_file"]

    def outputs_present(self, **kwargs):
        # Simple strings: "gdal_image", "gdal_vector", "hdf5", "filesize", "file"
        # Or dict spec: {"type": "gdal_image", "n_bands": 1, "chk_proj": True, "epsg_code": 32630}
        return self.check_files({self.params["out_file"]: "filesize"})

    def remove_outputs(self, **kwargs):
        out_file = self.params["out_file"]
        if os.path.exists(out_file):
            os.remove(out_file)


if __name__ == "__main__":
    ProcessCmd().std_run()
```

**Worker coding rules:**
- Keep `self.params` values JSON-serializable (`str`, `int`, `float`, `bool`, `list`, `dict`).
- Use `use_pbar=False` when calling `rsgispy` functions inside workers so parallel Slurm/GNU Parallel logs stay clean.
- Create parent output directories inside `do_processing()` (`os.makedirs(..., exist_ok=True)`) as well as in `gen_command_info()` for cluster portability.
- If `self.params` contains `"out_cmp_file"`, `completed_processing()` touches that sentinel file automatically upon success—useful when outputs are directories or variable filenames; remove it in `remove_outputs()` too.
- `std_run(**kwargs)` forwards extra keyword arguments to `check_required_fields`, `do_processing`, `completed_processing`, and debug `remove_outputs`.
- Return `(bool_all_ok, dict_filepath_to_error)` from `outputs_present()`—`self.check_files(...)` returns this exact tuple shape.
- In `remove_outputs()`, remove sidecar files (`.aux.xml`, `.ovr`, shapefile `.dbf`/`.shx`/`.prj`) if applicable.

### 2. Generator Script Pattern (`PBPTGenQProcessToolCmds`)

Implement `gen_command_info(self, **kwargs)` and `run_gen_commands(self)` and call `parse_cmds()` under `__main__`:

```python
import glob
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

logger = logging.getLogger(__name__)


class GenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        out_dir = os.path.abspath(kwargs["out_dir"])
        os.makedirs(out_dir, exist_ok=True)

        for in_file in sorted(glob.glob(kwargs["in_glob"])):
            basename = self.get_file_basename(in_file)
            out_file = os.path.join(out_dir, f"{basename}_out.tif")
            if not os.path.exists(out_file):
                self.params.append(
                    {
                        "in_file": os.path.abspath(in_file),
                        "out_file": out_file,
                    }
                )

    def run_gen_commands(self):
        self.gen_command_info(
            in_glob="/data/inputs/*.tif",
            out_dir="/data/outputs",
        )
        # Supports --append (incremental + SHA-256 deduplication) and --dry-run
        self.pop_params_db()

        # Choose EITHER create_shell_exe (local) OR create_slurm_sub_sh (HPC):
        self.create_shell_exe(
            run_script="run_exe_analysis.sh",
            cmds_sh_file="pbpt_cmds_lst.sh",
            n_cores=8,
            db_info_file="pbpt_lcl_db_info.json",
        )


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    py_script = os.path.join(script_dir, "perform_analysis.py")

    gen = GenCmds(
        cmd=f"python {py_script}",
        db_conn_file="db_conn.txt",  # Or omit to use PBPT_DB_CONN_STR env var
        process_tools_mod="perform_analysis",
        process_tools_cls="ProcessCmd",
        db_table_name="my_unique_task_table",
        db_schema=None,  # Optional PostgreSQL schema (auto-created), e.g. "eo_workflows"
    )
    gen.parse_cmds()
```

**Generator & execution rules:**
- **`process_tools_mod` / `process_tools_cls` / `process_tools_path`**: Dynamically imported by `--check` and `--rmouts` to instantiate your worker and invoke `outputs_present()` / `remove_outputs()`. Keep the worker module free of top-level execution side effects (`if __name__ == "__main__":`). Set `process_tools_path` if the worker lives outside `sys.path`.
- **Basename helper**: `self.get_file_basename(path)` strips 1 extension (`rm_n_exts=1`); pass `rm_n_exts=0` to strip compound extensions (`.tar.gz`), `n_comps=N` to keep the first `N` underscore tokens, and `check_valid=True` to sanitize spaces/punctuation.
- **Paths & Singularity**: Quote `py_script` or container paths with `shlex.quote` if they may contain spaces (note `db_info_file` and `cmds_sh_file` are quoted internally). For Singularity/Apptainer, set:
  `cmd=f"singularity exec --bind /data:/data /path/to/img.sif python {shlex.quote(py_script)}"` and `module_load="module load parallel singularity"`.
- **GNU Parallel vs. Slurm**:
  - Use `create_shell_exe(...)` for a single workstation (`parallel -j n_cores`).
  - Use `create_slurm_sub_sh(...)` for HPC clusters:
    - Prefer `use_array=True` with `max_concurrent=<N>` (`#SBATCH --array=0-(n_jobs-1)%max_concurrent`).
    - Provide either `mem_per_core_mb=...` (`--mem-per-cpu`) or `mem_total_mb=...` (`--mem`).
    - `n_xtr_cmds` adds extra worker lines to `jobcmds.sh` beyond `n_cores_per_job` so GNU Parallel keeps slots busy if a worker finishes early, though normally `n_xtr_cmds=0` is fine because each worker loops until the PostgreSQL queue is empty.
    - Optional Slurm flags: `account_name`, `partition`, `qos`, `constraint`, `gres`, `extra_sbatch_args`, `module_load`.

---

## Standard Operational Lifecycle

1. **Preview without side effects**: `python gen_cmds.py --gen --dry-run`
2. **Create queue & scripts**: `python gen_cmds.py --gen`
3. **Execute workers**: `./run_exe_analysis.sh` (runs GNU Parallel or submits `sbatch`)
4. **Monitor live progress**: `python gen_cmds.py --watch --interval 10` (or `--status`)
5. **Append new inputs later**: `python gen_cmds.py --gen --append` (SHA-256 deduplicates `JobParams` automatically; pass `--no-dedup` to insert all)
6. **Parallel output verification**: `python gen_cmds.py --check -w 8` (add `--chkoutfiles` to save timestamped `.txt`/`.json` error reports)
7. **Inspect failures & debug one PID**:
   - `python gen_cmds.py --errors --limit 10`
   - `python gen_cmds.py --inspect 42`
   - `python perform_analysis.py --dbinfo pbpt_lcl_db_info.json -j 42 -p` (print params)
   - `python perform_analysis.py --dbinfo pbpt_lcl_db_info.json -j 42` (run single job without mutating DB)
8. **Retry failed jobs**:
   - `python gen_cmds.py --reset --error` (DB state reset only; works with `--all`, `--error`, or `--pids 42 43`)
   - `python gen_cmds.py --rmouts --error` (delete partial outputs + reset DB state)
9. **Override table/schema from CLI**: `python gen_cmds.py --status --table other_tbl --schema eo_workflows`
10. **Clean up queue tables**:
    - Single table: `python gen_cmds.py --drop -y`
    - Database-wide sweep: `pbpt_clean_db.py -d db_conn.txt --all-schemas --drop --completed --drop-empty-schemas --vacuum -y`

---

## Programmatic Queue Management & Cleanup

All lifecycle operations can also be driven directly from Python (without `parse_cmds()`):

```python
from pbprocesstools.pbpt_db_clean import PBPTDatabaseCleaner

# Inspect & clean completed tables programmatically
cleaner = PBPTDatabaseCleaner(db_conn_file="db_conn.txt")
tables = cleaner.list_queue_tables(all_schemas=True, pattern="landsat_*")
candidates = cleaner.select_tables_for_cleanup(all_schemas=True, completed=True, older_than_days=7)
summary = cleaner.drop_tables(candidates, dry_run=False, drop_empty_schemas=True, vacuum=True)
```

On a `GenCmds` instance, you can call `get_jobs_status()`, `inspect_job(pid)`, `get_job_errors(limit=20)`, `check_job_outputs(n_workers=8)`, `reset_jobs(error_jobs=True)`, `create_jobs_report("report.json")`, and `drop_params_db()` directly in orchestrator scripts or tests.

---

## Common Pitfalls & Troubleshooting

- **`SQLite support has been removed in v4.0.0`**: Never pass `sqlite_db_file=...` or include `"sqlite_db_file"` in `--dbinfo` JSON.
- **`--drop` fails in non-interactive shells/CI**: Pass `-y` / `--yes` (`python gen_cmds.py --drop -y` or `pbpt_clean_db.py --drop ... -y`).
- **Active tables skipped by `pbpt_clean_db.py`**: Tables with `pending > 0` or `running > 0` have state `ACTIVE` and require `--force` to drop.
- **Jobs stuck `Started=True, Completed=False, Error=False` after node preemption**: `get_job_errors()`, `--reset --error`, and `--rmouts --error` treat started-incomplete rows as errors so they can be inspected and retried cleanly.
- **Output check failures overwrite DB status**: `--check` marks missing/corrupt outputs `Completed=False, Error=True`, while preserving any existing `ErrorInfo` on jobs that never completed.

---

## Installing This Skill into Gemini

### Gemini Online (`gemini.google.com`)

1. Package or locate the `skills/pb-process-tools/` folder with `SKILL.md` at the root (if creating a `.zip`, exclude hidden files like `.DS_Store`: `zip -r pb-process-tools-skill.zip . -x "*.DS_Store"`).
2. Open [`gemini.google.com`](https://gemini.google.com), go to **Customize** (under **More** if hidden) → **Skills** → **Upload**.
3. Upload the `skills/pb-process-tools/` folder, the `.zip` archive, or `SKILL.md`, then click **Create**.
4. Invoke in chat with `/pb-process-tools` or `@pb-process-tools`.

### Gemini CLI & Local Workspaces

```bash
# Install directly from GitHub via Gemini CLI
gemini skills install https://github.com/remotesensinginfo/pb_process_tools.git --path skills/pb-process-tools

# Or symlink into user-global Gemini skills directory
mkdir -p ~/.gemini/skills
ln -s /path/to/pb_process_tools/skills/pb-process-tools ~/.gemini/skills/pb-process-tools

# Or copy into a project-local skills directory
mkdir -p /path/to/your_project/.gemini/skills
cp -r /path/to/pb_process_tools/skills/pb-process-tools /path/to/your_project/.gemini/skills/
```

---

## Reference & Examples

- Full class, method, helper, and CLI reference: [references/api-reference.md](references/api-reference.md)
- End-to-end Landsat NDVI (`rsgispy`), GNU Parallel + `out_cmp_file`, and concurrent multi-task queue examples: [examples/landsat_ndvi_workflow.md](examples/landsat_ndvi_workflow.md)
