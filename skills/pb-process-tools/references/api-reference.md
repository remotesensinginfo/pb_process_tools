# `pb_process_tools` API & CLI Reference (v4.0.2)

## Package Metadata & Logging (`pbprocesstools`)

Version exports in `pbprocesstools`:
- `__version__` / `PB_PROCESS_TOOLS_VERSION` (`"4.0.2"`)
- `PB_PROCESS_TOOLS_VERSION_MAJOR`, `PB_PROCESS_TOOLS_VERSION_MINOR`, `PB_PROCESS_TOOLS_VERSION_PATCH`
- `PB_PROCESS_TOOLS_VERSION_INFO` / `PB_PROCESS_TOOLS_VERSION_OBJ`

Environment & logging configuration:
- `PBPT_DB_CONN_STR`: Fallback SQLAlchemy PostgreSQL connection URL when `db_conn_file` / `db_conn_str` is omitted.
- `PBPT_LOG_LVL`: `INFO` (default), `DEBUG`, `WARNING`, `ERROR`, `CRITICAL`.
- `PBPT_LOG_CFG`: Path to a JSON `logging.config.dictConfig` file (a template is shipped in `share/loggingconfig.json`).
- `configure_logging(level=None)`: Configures root logging (`%(asctime)s - %(name)s - %(levelname)s - %(message)s`) if no root handlers exist; called automatically by `PBPTQProcessTool.std_run()`, `PBPTGenQProcessToolCmds.parse_cmds()`, and `pbpt_db_clean.main()`.

PostgreSQL connection URL format:

```text
postgresql+psycopg2://username:password@hostname:5432/database_name
```

Connection resolution order in `PBPTGenQProcessToolCmds` and `PBPTDatabaseCleaner`:
1. Direct `db_conn_str` argument (stripped of whitespace)
2. `db_conn_file` / `--dbfile` text file (read via `PBPTUtils.readTextFileNoNewLines`, stripped)
3. `PBPT_DB_CONN_STR` environment variable

---

## PostgreSQL Queue Table Schema (`get_job_model`)

`get_job_model(table_name: str, schema: Optional[str] = None)` maps a SQLAlchemy 2.0 ORM model with:
- `PID`: `INTEGER` primary key (autoincrement)
- `JobParams`: `JSONB` (PostgreSQL) / `JSON` (generic)
- `Start`: `DATETIME` (nullable)
- `End`: `DATETIME` (nullable)
- `Started`: `BOOLEAN` (`NOT NULL`, default `False`)
- `Completed`: `BOOLEAN` (`NOT NULL`, default `False`)
- `Checked`: `BOOLEAN` (`NOT NULL`, default `False`)
- `Error`: `BOOLEAN` (`NOT NULL`, default `False`)
- `ErrorInfo`: `JSONB` (PostgreSQL) / `JSON` (generic)

Partial PostgreSQL indexes created automatically:
- `ix_<schema_><table>_pending`: `PID` where `Completed = False AND Started = False AND Error = False`
- `ix_<schema_><table>_unchecked`: `PID` where `Checked = False`
- `ix_<schema_><table>_incomplete`: `PID` where `Started = True AND Completed = False`

---

## `pbprocesstools.pbpt_q_process`

### `PBPTQProcessTool` (Worker Base Class)

Inherits from `_DBSessionMixin` and `PBPTProcessToolsBase`.

```python
PBPTQProcessTool(
    queue_db_info=None,   # Dict: {"db_conn_str": ..., "db_table_name": ..., optional "db_schema": ...}
    cmd_name=None,        # Script name for argparse prog
    descript=None,        # CLI description
    params=None,          # Optional single-job params dict
    uid_len=6,
    db_table_name=None,
    db_schema=None,
)
```

Abstract methods to implement:
- `do_processing(self, **kwargs)` — Execute per-job work using `self.params`. Raising any exception marks the row `Error=True, Completed=False, End=None` and stores `{"error": str(err), "traceback": ...}` in `ErrorInfo`.
- `required_fields(self, **kwargs) -> list[str]` — Return required keys in `self.params`. Checked before `do_processing()` (and before debug `-r` `remove_outputs()`).
- `outputs_present(self, **kwargs) -> tuple[bool, dict]` — Return `(all_present, {filepath: error_msg})`. Typically calls `self.check_files(...)`.
- `remove_outputs(self, **kwargs)` — Delete output files generated for `self.params`.

Execution, state, and helper methods:
- `std_run(self, max_db_retries=5, **kwargs)` — Calls `configure_logging()` and `parse_cmds(**kwargs)` (exits `1` on failure). Extra `**kwargs` are forwarded to `check_required_fields`, `do_processing`, `completed_processing`, and debug `remove_outputs`. If `-j/--job` is passed, runs debug mode without updating DB state; otherwise loops claiming pending rows via `FOR UPDATE SKIP LOCKED` with exponential backoff retries (`min(2**attempt, 30)`s) on transient DB errors.
- `parse_cmds(self, argv=None, **kwargs) -> bool` — Parses `-d/--dbinfo` (required), `-j/--job`, `-p/--params`, `-r/--rmouts` and calls `check_db_info()`.
- `set_params(self, params)` — Set `self.params` programmatically.
- `set_queue_db_info(self, queue_db_info)` — Update `self.queue_db_info` and dispose cached engine.
- `check_db_info() -> bool` — Validates `db_conn_str` (falling back to `PBPT_DB_CONN_STR`), rejects `sqlite_db_file`, resolves `db_table_name`/`db_schema`, and executes `SELECT 1`.
- `check_required_fields(self, **kwargs) -> bool` — Raises `ValueError` if `self.params is None` or `KeyError` if required keys are missing.
- `completed_processing(self, **kwargs)` — Marks `Completed=True, Error=False, End=now()` for `self.job_pid`. If `"out_cmp_file"` is present in `self.params`, automatically touches that file path (`Path(...).touch()`).
- `record_process_error(self, err_info, **kwargs)` — Marks `Completed=False, Error=True, End=None, ErrorInfo=_sanitize_json(err_info)`.
- `dispose_engine()` — Disposes SQLAlchemy engine and clears pooled connections.

---

### `PBPTGenQProcessToolCmds` (Queue & Script Generator Base Class)

Inherits from `_DBSessionMixin` and `PBPTProcessToolsBase`.

```python
PBPTGenQProcessToolCmds(
    cmd,                      # Worker command prefix, e.g. "python /abs/path/perform_analysis.py"
    sqlite_db_file=None,      # Unsupported in v4.x (raises ValueError if not None)
    db_conn_file=None,        # File with SQLAlchemy PostgreSQL URL
    lock_file_path=None,      # Deprecated and ignored
    uid_len=6,
    process_tools_path=None,  # Directory containing process_tools_mod if not on sys.path
    process_tools_mod=None,   # Module name containing process_tools_cls
    process_tools_cls=None,   # Class name subclassing PBPTQProcessTool
    db_conn_str=None,         # Direct PostgreSQL URL
    db_table_name=None,       # REQUIRED non-empty queue table name
    db_schema=None,           # Optional PostgreSQL schema name (auto-created if missing)
)
```

Abstract methods to implement:
- `gen_command_info(self, **kwargs)` — Discover inputs and append JSON-serializable parameter dicts to `self.params`.
- `run_gen_commands(self)` — Entry point invoked by `--gen` / `--append`; typically calls `gen_command_info()`, `pop_params_db()`, and either `create_shell_exe()` or `create_slurm_sub_sh()`.

Queue population, verification, monitoring, and reset methods:
- `pop_params_db(self, batch_size=5000, append=None, deduplicate=None) -> int` — If `append` is False (default unless `--append` CLI flag was passed), drops and recreates the queue table (creating `db_schema` if needed) and bulk-inserts `self.params` in chunks of `batch_size`. If `append` is True, delegates to `append_params_db()`. Honors `self.dry_run`.
- `append_params_db(self, batch_size=5000, deduplicate=True) -> int` — Ensures schema/table exists (`checkfirst=True`) and inserts jobs from `self.params`. When `deduplicate=True`, computes canonical JSON SHA-256 digests (`sort_keys=True, separators=(",", ":")`) of existing `JobParams` and skips duplicates (both against the DB and within `self.params`). Honors `self.dry_run`.
- `check_job_outputs(self, out_err_pid_file=None, out_err_info_file=None, process_tools_path=None, process_tools_mod=None, process_tools_cls=None, batch_size=500, n_workers=1, **kwargs)` — Queries rows with `Checked=False`. Incomplete/never-started rows are recorded as errors (preserving existing `ErrorInfo` or setting `"Started but did not complete."` / `"Never Started."`). Completed rows run `worker.outputs_present(**kwargs)` across `n_workers` threads (with per-thread worker instances), updating `Checked=True` or `Completed=False, Error=True, ErrorInfo=<errs_dict>` in batches of `batch_size`.
- `run_check_outputs(self, out_files=False, n_workers=1, **kwargs)` — CLI `--check` wrapper; when `out_files=True` (`--chkoutfiles`), writes timestamped `processing_errs_scns_<ts>.txt` and `non_complete_errs_<ts>.json`.
- `remove_job_outputs(self, all_jobs=False, error_jobs=False, pids=None, process_tools_path=None, process_tools_mod=None, process_tools_cls=None, batch_size=500, **kwargs)` — Calls `worker.remove_outputs(**kwargs)` and resets matching rows (`Started=False, Completed=False, Checked=False, Error=False, ErrorInfo=None, Start=None, End=None`).
- `run_remove_outputs(self, all_jobs=False, error_jobs=False, pids=None, **kwargs)` — CLI `--rmouts` wrapper.
- `reset_jobs(self, all_jobs=False, error_jobs=False, pids=None) -> int` — Resets matching DB rows to pending without deleting output files. Returns count reset. (`error_jobs=True` matches `Error=True` OR `Started=True AND Completed=False`.)
- `run_reset_jobs(self, all_jobs=False, error_jobs=False, pids=None) -> int` — CLI `--reset` wrapper.
- `get_jobs_status(self, _dispose: bool = True) -> dict` — Returns:
  - `total` (`int`), `pending` (`int`), `running` (`int`), `completed` (`int`), `checked` (`int`), `errors` (`int`)
  - `pct_complete` (`float`, `0.0`–`100.0`)
  - `throughput_per_hour` (`float | None`)
  - `eta_seconds` (`float | None`)
- `print_jobs_status(self, watch: bool = False, interval_secs: float = 5.0)` — Prints live summary once, or polls until `total == 0` or `(pending == 0 and running == 0)`.
- `inspect_job(self, pid: int, print_output: bool = True) -> dict` — Returns `PID`, `Started`, `Completed`, `Checked`, `Error`, `Start`, `End`, `JobParams`, `ErrorInfo` (raises `LookupError` if PID not found).
- `get_job_errors(self, limit: Optional[int] = None, print_output: bool = True) -> list[dict]` — Lists rows where `Error=True` or (`Started=True` and `Completed=False`).
- `create_jobs_report(self, out_report_file=None)` — Pretty-prints or writes JSON stats dict with keys `job_n` (`n_completed`, `n_errs`, `n_started`, `n_ended`, `total_n_jobs`), `job_times` (`time_mean_secs`, `time_min_secs`, `time_max_secs`, `time_median_secs`, plus `time_stdev_secs` and `time_quartiles_secs` when > 1 completed job), `status`, and `xerrors` (empty `{}` when `total == 0`).
- `drop_params_db()` — Drops this task's queue table (`checkfirst=True`).
- `parse_cmds(self, argv=None)` — Standard CLI parser for `GenCmds` scripts.

Execution script generation methods:
- `create_shell_exe(self, run_script, cmds_sh_file, n_cores, db_info_file=None)`:
  - Writes `db_info_file` (`chmod 0o600`, defaults to `process_db_info_<uid>.json`), `cmds_sh_file` (`n_cores` copies of `<cmd> --dbinfo <db_info_file>`), and executable `run_script` (`chmod 0o755`) invoking `parallel -j <n_cores> < <cmds_sh_file>`. Honors `self.dry_run`.
- `create_slurm_sub_sh(self, jobname, mem_per_core_mb, log_dir, run_script="run_exe_analysis.sh", job_dir="job_scripts", db_info_file=None, account_name=None, n_cores_per_job=10, n_xtr_cmds=0, n_jobs=10, job_time_limit="2-23:59", module_load="module load parallel singularity", partition="compute", use_array=False, max_concurrent=None, mem_total_mb=None, qos=None, constraint=None, gres=None, extra_sbatch_args=None)`:
  - Sanitizes `jobname` (`rm_non_ascii=True, rm_dashs=True, rm_spaces=True, rm_punc=True`).
  - Writes `db_info_file` (`0o600`), `<job_dir>/jobcmds.sh` with `n_cores_per_job + n_xtr_cmds` worker commands, `.sbatch` script(s) running `parallel -N 1 --delay .2 -j $SLURM_NTASKS < <jobcmds.sh>`, and executable `run_script` containing `sbatch` submission command(s).
  - When `use_array=True`, writes single `<job_dir>/job_array.sbatch` with `#SBATCH --array=0-(n_jobs-1)[%max_concurrent]` and `%A_%a` log filenames.
  - When `mem_total_mb` is provided, emits `#SBATCH --mem=<mem_total_mb>` instead of `#SBATCH --mem-per-cpu=<mem_per_core_mb>`.
  - `extra_sbatch_args` items are prefixed with `#SBATCH ` automatically if omitted.

---

## `pbprocesstools.pbpt_process.PBPTProcessToolsBase`

Inherited by both `PBPTQProcessTool` and `PBPTGenQProcessToolCmds`:
- `uid_generator(size=6) -> str`
- `generate_readable_timestamp_str() -> str` (`YYYYMMDD_HHMMSS`)
- `find_file(dir_path, file_search) -> Optional[str]` (`dir_path` may be a `str` or iterable of directories; returns single match or `None` if 0 or >1 match)
- `find_first_file(dir_path, file_search, rtn_except=True) -> Optional[str]` (recursive search; raises if 0 or >1 match when `rtn_except=True`)
- `get_file_basename(file_path, check_valid=False, n_comps=0, rm_n_exts=1) -> str` (`rm_n_exts=0` strips all extensions; `n_comps > 0` keeps first `n_comps` `_`-separated components)
- `create_dir(dir_path, use_abs_path=True, add_uid=False) -> tuple[str, bool]` (returns `(path_str, created_bool)`)
- `remove_dir(dir_path)` (`shutil.rmtree(..., ignore_errors=True)`)
- `has_files(dir_path: str, recursive: bool = False) -> bool` (`@staticmethod`; raises `NotADirectoryError` if invalid dir)
- `check_files(files_dict) -> tuple[bool, dict]`

`check_files(files_dict)` specification values (keys are file paths):
- `"file"` — path exists (`os.path.exists`)
- `"filesize"` — regular file with `st_size > 0`
- `"gdal_image"` or dict:
  - `{"type": "gdal_image", "n_bands": 0, "chk_proj": False, "epsg_code": 0, "read_img": False, "smpl_n_pxls": 10, "calc_chk_sum": False, "max_file_size": None, "timeout": 4}`
  - Note: `epsg_code` is passed only when `chk_proj=True`; `smpl_n_pxls` is used only when `read_img=True`.
- `"gdal_vector"` or dict:
  - `{"type": "gdal_vector", "chk_proj": False, "epsg_code": 0, "max_file_size": None, "timeout": 4}`
- `"hdf5"` or dict:
  - `{"type": "hdf5", "timeout": 4}`

---

## `pbprocesstools.pbpt_utils` (Delegated to `rsgispy.tools`)

### `PBPTUtils`
- `get_file_lock(input_file, sleep_period=1, wait_iters=120, use_except=False, timeout=3600) -> bool`
- `release_file_lock(input_file, timeout=3600)`
- `clean_file_locks(dir_path, timeout=3600)`
- `readTextFileNoNewLines(file) -> str`
- `readTextFile2List(file) -> list[str]`
- `writeList2File(dataList, outFile)`
- `writeData2File(data_val, out_file)`
- `writeDict2JSON(data_dict, out_file)`
- `readJSON2Dict(input_file) -> dict | list`
- `check_str(str_val, rm_non_ascii=False, rm_dashs=False, rm_spaces=False, rm_punc=False) -> str`
- `remove_repeated_chars(str_val, repeat_char) -> str`
- `uidGenerator(size=6) -> str`

### `PBPTTextFileUtils`
- `readTextFile2List(file) -> list[str]`
- `writeList2File(data_list, out_file)`

### `PBPTJSONParseHelper`
- `readGZIPJSON(file_path)`
- `writeGZIPJSON(data, file_path)`
- `doesPathExist(json_obj, tree_sequence) -> bool`
- `getValueDict(json_obj, tree_sequence) -> dict`
- `getStrValue(json_obj, tree_sequence, valid_values=None) -> str`
- `getBooleanValue(json_obj, tree_sequence) -> bool`
- `getDateValue(json_obj, tree_sequence, date_format="%Y-%m-%d") -> datetime.date`
- `getDateTimeValue(json_obj, tree_sequence, date_time_format="%Y-%m-%d") -> datetime.datetime`
- `getStrListValue(json_obj, tree_sequence, valid_values=None) -> list[str]`
- `getNumericValue(json_obj, tree_sequence, valid_lower=None, valid_upper=None) -> int | float`
- `getListValue(json_obj, tree_sequence) -> list`

---

## `pbprocesstools.pbpt_db_clean` & `pbpt_clean_db.py`

### Programmatic API
- `resolve_db_conn_str(dbfile=None, db_conn_str=None) -> str`
- `is_pbpt_queue_table(inspector, table_name, schema=None) -> bool` — Checks table contains all `PBPT_QUEUE_COLUMNS` (`PID`, `JobParams`, `Start`, `End`, `Started`, `Completed`, `Checked`, `Error`, `ErrorInfo`).
- `PBPTDatabaseCleaner(db_conn_file=None, db_conn_str=None)`:
  - `list_queue_tables(schema=None, all_schemas=False, pattern=None, tables=None) -> list[dict]`
  - `select_tables_for_cleanup(schema=None, all_schemas=False, tables=None, pattern=None, completed=False, empty=False, finished=False, older_than_days=None, all_tables=False) -> list[dict]`
  - `drop_tables(table_infos, force=False, dry_run=False, drop_empty_schemas=False, vacuum=False) -> dict` — Returns `{"dry_run", "dropped", "skipped_active", "dropped_schemas", "vacuumed"}`. Never drops protected schemas (`information_schema`, `pg_catalog`, `pg_toast`, `public`, `main`, `pg_*`).
  - `vacuum_database(dry_run=False) -> bool` — Runs `VACUUM ANALYZE` in autocommit mode on PostgreSQL.

Each returned table summary dict contains:
- `schema`, `table`, `full_name`, `total`, `pending`, `running`, `completed`, `checked`, `errors`, `state`, `last_activity`, `age_days`, `size_bytes`.
- Table `state` classification: `EMPTY` (`total == 0`), `ACTIVE` (`pending > 0 or running > 0`), `FINISHED_WITH_ERRORS` (`errors > 0` and idle), `COMPLETED` (all finished, `errors == 0`).

### CLI Reference

#### `pbpt_clean_db.py` (or `pbpt_clean_db`)
- Connection & scope: `-d/--dbfile`, `--db-conn-str`, `--schema`, `--all-schemas`
- Mode: `-l/--list` (default), `--json`, `--drop`, `-i/--interactive`, `--vacuum`
- Selection filters (at least one required for `--drop`): `-t/--tables`, `--pattern`, `--completed`, `--empty`, `--finished`, `--older-than DAYS`, `--all`
- Safety & extras: `--force` (allow dropping `ACTIVE`), `--drop-empty-schemas`, `--dry-run`, `-y/--yes` (required for non-interactive `--drop`)

#### `pbpt_gen_template.py`
- Required: `-o/--output`, `-t/--table`
- Optional: `-d/--dbfile`, `-p/--prefix`, `--schema`, `--slurm`, `--partition` (default `"compute"`), `--array`, `--max_concurrent`, `--singularity`, `--sing_paths`, `--overwrite`
- Generates: `<prefix>_gen_cmds.py`, `<prefix>_perform_analysis.py`, `run_gen_cmds.sh`, `run_status.sh`, `run_errors.sh`, `run_report.sh`, `run_reset_errs.sh`, `run_checks.sh` (all `.sh` scripts forward `"$@"`, and `run_report.sh` pipes through `more`).

#### `GenCmds` Script (`PBPTGenQProcessToolCmds.parse_cmds()`)
- `--gen`, `--append`, `--no-dedup`, `--dry-run`
- `--status`, `--watch`, `--interval SECS` (default `5.0`)
- `--check`, `-w/--workers N` (default `1`), `--chkoutfiles`
- `--errors`, `--limit N`, `--inspect PID`
- `--report`, `-o/--output FILE`
- `--reset` / `--rmouts` with `--all`, `--error`, or `--pids PID ...`
- `--drop`, `-y/--yes`
- `--table NAME`, `--schema NAME` (CLI overrides)

#### Worker Script (`PBPTQProcessTool.std_run()`)
- `-d/--dbinfo FILE` (required JSON metadata file written by `create_shell_exe` / `create_slurm_sub_sh`)
- `-j/--job PID` (single-job debug mode; does not modify DB state)
- `-p/--params` (with `-j`: pretty-print `JobParams`)
- `-r/--rmouts` (with `-j`: run `check_required_fields()` + `remove_outputs()`)
