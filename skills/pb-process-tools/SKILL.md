---
name: pb-process-tools
description: Generate, scaffold, debug, and run batch data processing workflows using `pb_process_tools` (`pbprocesstools` v4.x) backed by a PostgreSQL job queue, GNU Parallel, or Slurm (including job arrays and Singularity). Use whenever writing `PBPTQProcessTool` worker scripts, `PBPTGenQProcessToolCmds` command/queue generators, `pbpt_gen_template.py` scaffolds, `pbpt_clean_db.py` cleanup commands, or pairing `pb_process_tools` with `rsgispy` for geospatial batch processing.
---

# `pb_process_tools` Code Generation Guide (v4.x)

`pb_process_tools` (`pbprocesstools`) orchestrates large batch processing pipelines using a **PostgreSQL job queue** backed by SQLAlchemy 2.0 (`FOR UPDATE SKIP LOCKED`) and either **GNU Parallel** (local workstations) or **Slurm** (HPC clusters, optionally with job arrays and Singularity/Apptainer).

> **Important v4.x rules:**
> - **PostgreSQL only**: SQLite production queues (`sqlite_db_file`) were removed in v4.0.0. Always configure PostgreSQL via `db_conn_file`, `db_conn_str`, or `PBPT_DB_CONN_STR`.
> - **Per-task table isolation**: Every `PBPTGenQProcessToolCmds` workflow **must** specify a unique `db_table_name` (and optionally `db_schema`) so concurrent tasks can share the same PostgreSQL database safely.
> - **Use `rsgispy`, never `rsgislib`**: File/dataset validation (`gdal_image`, `gdal_vector`, `hdf5`), string/file utilities, and geospatial processing examples use `rsgispy`.
> - **`lock_file_path` is deprecated/ignored**: PostgreSQL row-level locking replaces filesystem lock files for queues.

---

## Architecture Overview

A standard workflow consists of two Python scripts (typically scaffolded via `pbpt_gen_template.py` or written directly):

1. **Worker script** (`perform_analysis.py`) subclassing [`PBPTQProcessTool`](references/api-reference.md#pbptqprocesstool):
   - Claims pending jobs atomically (`Started=False, Completed=False, Error=False`) via `std_run()`.
   - Validates `required_fields()`, runs `do_processing()`, marks completion or records exception tracebacks into `ErrorInfo`, and verifies/deletes outputs via `outputs_present()` / `remove_outputs()`.
2. **Generator & queue controller** (`gen_cmds.py`) subclassing [`PBPTGenQProcessToolCmds`](references/api-reference.md#pbptgenqprocesstoolcmds):
   - Discovers inputs in `gen_command_info()` and appends JSON-serializable parameter dicts to `self.params`.
   - Calls `self.pop_params_db()` (recreates table by default, or appends/deduplicates under `--append`) and writes execution scripts via `self.create_shell_exe(...)` or `self.create_slurm_sub_sh(...)`.
   - Exposes queue lifecycle commands via `parse_cmds()` (`--gen`, `--append`, `--dry-run`, `--status`, `--watch`, `--check`, `--errors`, `--inspect`, `--reset`, `--rmouts`, `--report`, `--drop`).

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
- Generates `<prefix>_perform_analysis.py`, `<prefix>_gen_cmds.py`, and executable wrappers (`run_gen_cmds.sh`, `run_status.sh`, `run_errors.sh`, `run_report.sh`, `run_reset_errs.sh`, `run_checks.sh`) that forward `"$@"` flags.

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
        # Implement per-job processing using self.params[...]

    def required_fields(self, **kwargs):
        return ["in_file", "out_file"]

    def outputs_present(self, **kwargs):
        # Options: "gdal_image", "gdal_vector", "hdf5", "filesize", "file",
        # or dict spec e.g. {"type": "gdal_image", "n_bands": 1, "chk_proj": True, "epsg_code": 32630}
        return self.check_files({self.params["out_file"]: "filesize"})

    def remove_outputs(self, **kwargs):
        out_file = self.params["out_file"]
        if os.path.exists(out_file):
            os.remove(out_file)


if __name__ == "__main__":
    ProcessCmd().std_run()
```

**Worker coding rules:**
- Keep `self.params` keys simple JSON-serializable types (`str`, `int`, `float`, `bool`, `list`, `dict`).
- Use `use_pbar=False` when calling `rsgispy` functions inside workers so parallel Slurm/GNU Parallel logs remain clean.
- Create parent output directories inside `do_processing()` (with `os.makedirs(..., exist_ok=True)`) as well as in `gen_command_info()` so jobs remain portable across nodes.
- Return `(bool_all_ok, dict_filepath_to_error)` from `outputs_present()`—`self.check_files(...)` returns this exact tuple shape.
- When removing raster/vector outputs in `remove_outputs()`, remove sidecar files (e.g. `.aux.xml`, `.ovr`, shapefile sidecars) if applicable.

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
        db_schema=None,  # Optional PostgreSQL schema, e.g. "eo_workflows"
    )
    gen.parse_cmds()
```

**Important details for `GenCmds`:**
- `process_tools_mod` + `process_tools_cls` (and optional `process_tools_path` if the worker module lives in another directory) are dynamically imported by `--check` and `--rmouts` to instantiate the worker class and invoke `outputs_present()` / `remove_outputs()`. Ensure the worker module is importable without side effects (keep execution inside `if __name__ == "__main__":`).
- `self.get_file_basename(path)` strips 1 extension by default (`rm_n_exts=1`); pass `rm_n_exts=0` to strip compound extensions like `.tar.gz`, and `check_valid=True` to sanitize spaces/punctuation.
- Use `shlex.quote` if `py_script` or container paths may contain spaces—note `pb_process_tools` already quotes `db_info_file` internally in generated shell/sbatch scripts.
- For **Slurm**, replace `create_shell_exe(...)` with `create_slurm_sub_sh(...)`:
  - Prefer `use_array=True` with `max_concurrent=<N>` for cleaner Slurm queue management (`#SBATCH --array=0-(n_jobs-1)%max_concurrent`).
  - Pass either `mem_per_core_mb=...` (`--mem-per-cpu`) or `mem_total_mb=...` (`--mem`), and optional `account_name`, `partition`, `qos`, `constraint`, `gres`, `extra_sbatch_args`, `module_load`.

---

## Standard Operational Lifecycle

Always remind users (or emit wrapper commands) for the complete operational lifecycle:

1. **Preview without side effects**: `python gen_cmds.py --gen --dry-run`
2. **Create queue & scripts**: `python gen_cmds.py --gen`
3. **Execute workers**: `./run_exe_analysis.sh` (runs GNU Parallel or submits `sbatch`)
4. **Monitor live progress**: `python gen_cmds.py --watch --interval 10` (or `--status`)
5. **Append new inputs later**: `python gen_cmds.py --gen --append` (deduplicates existing `JobParams` automatically; pass `--no-dedup` to force insert)
6. **Parallel output verification**: `python gen_cmds.py --check -w 8`
7. **Inspect failures & debug one PID**:
   - `python gen_cmds.py --errors --limit 10`
   - `python gen_cmds.py --inspect 42`
   - `python perform_analysis.py --dbinfo pbpt_lcl_db_info.json -j 42 -p` (print params)
   - `python perform_analysis.py --dbinfo pbpt_lcl_db_info.json -j 42` (run single job without mutating queue DB)
8. **Retry failed jobs**:
   - `python gen_cmds.py --reset --error` (DB state reset only)
   - `python gen_cmds.py --rmouts --error` (delete partial outputs + reset DB state)
9. **Clean up queue tables**:
   - Single table: `python gen_cmds.py --drop -y`
   - Sweep database: `pbpt_clean_db.py -d db_conn.txt --all-schemas --drop --completed --drop-empty-schemas --vacuum -y`

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
- End-to-end Landsat NDVI (`rsgispy`) & concurrent multi-task queue examples: [examples/landsat_ndvi_workflow.md](examples/landsat_ndvi_workflow.md)
