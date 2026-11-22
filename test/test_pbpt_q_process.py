import concurrent.futures
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import stat
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from pbprocesstools.pbpt_q_process import (
    PBPTGenQProcessToolCmds,
    PBPTQProcessTool,
    get_job_model,
)


class _DummyWorker(PBPTQProcessTool):
    def __init__(self, **kwargs):
        super().__init__(cmd_name="dummy_worker.py", **kwargs)

    def required_fields(self, **kwargs):
        return ["out_file", "mode"]

    def do_processing(self, **kwargs):
        if self.params["mode"] == "fail":
            raise RuntimeError("intentional processing failure")
        if self.params["mode"] == "partial":
            pathlib.Path(self.params["out_file"]).write_text("partial")
            raise RuntimeError("failed mid-write")
        pathlib.Path(self.params["out_file"]).write_text("done")

    def outputs_present(self, **kwargs):
        return self.check_files({self.params["out_file"]: "filesize"})

    def remove_outputs(self, **kwargs):
        if os.path.exists(self.params["out_file"]):
            os.remove(self.params["out_file"])


class _DummyGen(PBPTGenQProcessToolCmds):
    def __init__(self, jobs_params, **kwargs):
        super().__init__(cmd="python dummy_worker.py", **kwargs)
        self._jobs_params = jobs_params

    def gen_command_info(self, **kwargs):
        self.params = list(self._jobs_params)

    def run_gen_commands(self):
        self.gen_command_info()
        self.pop_params_db()


class PBPTQProcessTest(unittest.TestCase):
    def test_postgres_ddl_and_skip_locked_compilation(self):
        dialect = postgresql.dialect()
        model = get_job_model(table_name="custom_jobs", schema="batch")

        create_table_sql = str(CreateTable(model.__table__).compile(dialect=dialect))
        self.assertIn("JSONB", create_table_sql)
        self.assertIn("batch.custom_jobs", create_table_sql)

        index_sqls = [
            str(CreateIndex(idx).compile(dialect=dialect))
            for idx in model.__table__.indexes
        ]
        self.assertEqual(len(index_sqls), 3)
        self.assertTrue(any("WHERE" in sql for sql in index_sqls))

        claim_stmt = (
            sqlalchemy.select(model)
            .where(model.Completed.is_(False), model.Started.is_(False))
            .order_by(model.PID.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        compiled_claim = str(claim_stmt.compile(dialect=dialect))
        self.assertIn("FOR UPDATE SKIP LOCKED", compiled_claim)

    def test_sqlite_deprecated_rejected(self):
        with self.assertRaises(ValueError):
            _DummyGen(
                [], sqlite_db_file="legacy.sqlite", db_table_name="legacy_task"
            )

        tool = _DummyWorker(
            queue_db_info={
                "db_conn_str": "sqlite:///x.db",
                "db_table_name": "legacy_task",
                "sqlite_db_file": "x.db",
            }
        )
        with self.assertRaises(ValueError):
            tool.check_db_info()

    def test_end_to_end_queue_lifecycle_check_and_rmouts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "queue_test.db")
            db_url = f"sqlite:///{db_file}"
            conn_file = os.path.join(tmpdir, "db_conn.txt")
            pathlib.Path(conn_file).write_text(db_url + "\n")

            out_ok = os.path.join(tmpdir, "ok.txt")
            out_fail = os.path.join(tmpdir, "fail.txt")
            out_partial = os.path.join(tmpdir, "partial.txt")
            out_missing_field = os.path.join(tmpdir, "missing.txt")

            jobs = [
                {"out_file": out_ok, "mode": "ok"},
                {"out_file": out_fail, "mode": "fail"},
                {"out_file": out_partial, "mode": "partial"},
                {"out_file": out_missing_field},  # missing 'mode' required field
            ]

            gen = _DummyGen(
                jobs_params=jobs,
                db_conn_file=conn_file,
                db_table_name="task_lifecycle",
                lock_file_path=os.path.join(tmpdir, "ignored.lck"),
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen.run_gen_commands()

            # Lock file should NOT be created on disk
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "ignored.lck")))

            # Write dbinfo & scripts
            db_info_path = os.path.join(tmpdir, "dbinfo.json")
            run_sh = os.path.join(tmpdir, "run.sh")
            cmds_sh = os.path.join(tmpdir, "cmds.sh")
            gen.create_shell_exe(run_sh, cmds_sh, 2, db_info_file=db_info_path)

            self.assertTrue(os.path.exists(db_info_path))
            db_info = json.loads(pathlib.Path(db_info_path).read_text())
            self.assertNotIn("sqlite_db_file", db_info)
            self.assertNotIn("lck_file", db_info)
            # File permissions should be 0600 on POSIX
            if os.name == "posix":
                mode = stat.S_IMODE(os.stat(db_info_path).st_mode)
                self.assertEqual(mode, 0o600)

            # Run worker: missing required field must NOT crash worker before finishing remaining jobs
            worker = _DummyWorker()
            # Call parse_cmds + std_run via argv patch
            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                worker.std_run()
            finally:
                sys.argv = orig_argv

            self.assertTrue(os.path.exists(out_ok))
            self.assertFalse(os.path.exists(out_fail))
            self.assertTrue(os.path.exists(out_partial))

            # Run --check and then --rmouts --error (verifies they no longer conflict)
            err_pids_file = os.path.join(tmpdir, "err_pids.txt")
            err_info_file = os.path.join(tmpdir, "err_info.json")
            gen.check_job_outputs(
                out_err_pid_file=err_pids_file,
                out_err_info_file=err_info_file,
            )
            err_pids = pathlib.Path(err_pids_file).read_text().strip().splitlines()
            self.assertEqual(len(err_pids), 3)

            report_file = os.path.join(tmpdir, "report.json")
            gen.create_jobs_report(out_report_file=report_file)
            report = json.loads(pathlib.Path(report_file).read_text())
            self.assertEqual(report["job_n"]["total_n_jobs"], 4)
            self.assertEqual(report["job_n"]["n_completed"], 1)
            self.assertEqual(report["job_n"]["n_errs"], 3)

            gen.remove_job_outputs(error_jobs=True)
            self.assertFalse(os.path.exists(out_partial))
            self.assertTrue(os.path.exists(out_ok))

            # Verify Slurm script generation preserves digits in jobname and custom partition
            slurm_run_sh = os.path.join(tmpdir, "submit_slurm.sh")
            slurm_job_dir = os.path.join(tmpdir, "slurm_jobs")
            slurm_log_dir = os.path.join(tmpdir, "slurm_logs")
            gen.create_slurm_sub_sh(
                "sentinel2_2026",
                4096,
                slurm_log_dir,
                run_script=slurm_run_sh,
                job_dir=slurm_job_dir,
                db_info_file=db_info_path,
                n_jobs=1,
                partition="highmem",
            )
            sbatch_txt = pathlib.Path(
                os.path.join(slurm_job_dir, "job_file_0.sbatch")
            ).read_text()
            self.assertIn("#SBATCH --partition=highmem", sbatch_txt)
            self.assertIn("#SBATCH --job-name=sentinel2_2026_0", sbatch_txt)

    def test_reset_jobs_without_deleting_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "reset_test.db")
            db_url = f"sqlite:///{db_file}"

            out_1 = os.path.join(tmpdir, "out1.txt")
            out_2 = os.path.join(tmpdir, "out2.txt")
            out_3 = os.path.join(tmpdir, "out3.txt")

            jobs = [
                {"out_file": out_1, "mode": "ok"},
                {"out_file": out_2, "mode": "partial"},
                {"out_file": out_3, "mode": "fail"},
            ]

            gen = _DummyGen(
                jobs_params=jobs,
                db_conn_str=db_url,
                db_table_name="task_reset",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen.run_gen_commands()

            db_info_path = gen._write_db_info_file(os.path.join(tmpdir, "dbinfo.json"))
            worker = _DummyWorker()
            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                worker.std_run()
            finally:
                sys.argv = orig_argv

            # Verify out_1 and out_2 exist on disk
            self.assertTrue(os.path.exists(out_1))
            self.assertTrue(os.path.exists(out_2))

            # Reset error jobs via CLI (--reset --error): DB state reset, out_2 NOT deleted
            gen.parse_cmds(["--reset", "--error"])
            self.assertTrue(os.path.exists(out_2))

            report_err_reset = os.path.join(tmpdir, "report_err_reset.json")
            gen.create_jobs_report(out_report_file=report_err_reset)
            rep = json.loads(pathlib.Path(report_err_reset).read_text())
            self.assertEqual(rep["job_n"]["n_completed"], 1)
            self.assertEqual(rep["job_n"]["n_started"], 1)
            self.assertEqual(rep["job_n"]["n_errs"], 0)

            # Reset specific PID via CLI (--reset --pids 1) without deleting out_1
            gen.parse_cmds(["--reset", "--pids", "1"])
            self.assertTrue(os.path.exists(out_1))

            report_pid_reset = os.path.join(tmpdir, "report_pid_reset.json")
            gen.create_jobs_report(out_report_file=report_pid_reset)
            rep2 = json.loads(pathlib.Path(report_pid_reset).read_text())
            self.assertEqual(rep2["job_n"]["n_completed"], 0)
            self.assertEqual(rep2["job_n"]["n_started"], 0)
            self.assertEqual(rep2["job_n"]["n_errs"], 0)

            # Reset all jobs programmatically
            n_reset = gen.reset_jobs(all_jobs=True)
            self.assertEqual(n_reset, 3)
            self.assertTrue(os.path.exists(out_1))
            self.assertTrue(os.path.exists(out_2))

            # Calling reset_jobs with no selector raises ValueError
            with self.assertRaises(ValueError):
                gen.reset_jobs()

    def test_gen_template_script_singularity_spacing(self):
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "bin", "pbpt_gen_template.py")
        )
        spec = importlib.util.spec_from_file_location("pbpt_gen_template", script_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValueError):
                mod.gen_pb_process_tools_template(output_dir=tmpdir)

            mod.gen_pb_process_tools_template(
                output_dir=tmpdir,
                dbfile="/path/to/db.txt",
                db_table_name="singularity_task",
                use_slurm=True,
                singularity="/path/to/container.sif",
                singularity_paths=["/data:/data"],
            )
            run_gen = pathlib.Path(os.path.join(tmpdir, "run_gen_cmds.sh")).read_text()
            self.assertIn("container.sif python ", run_gen)
            self.assertNotIn("container.sifpython", run_gen)

            gen_cmds_py = pathlib.Path(os.path.join(tmpdir, "gen_cmds.py")).read_text()
            self.assertIn("pbprocesstools", gen_cmds_py)
            self.assertNotIn("lock_file_path", gen_cmds_py)
            self.assertIn('db_table_name="singularity_task"', gen_cmds_py)

    def test_append_params_db_and_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "append_test.db")
            db_url = f"sqlite:///{db_file}"

            out_1 = os.path.join(tmpdir, "a1.txt")
            out_2 = os.path.join(tmpdir, "a2.txt")
            out_3 = os.path.join(tmpdir, "a3.txt")

            initial_jobs = [
                {"out_file": out_1, "mode": "ok"},
                {"out_file": out_2, "mode": "ok"},
            ]
            gen = _DummyGen(
                jobs_params=initial_jobs,
                db_conn_str=db_url,
                db_table_name="task_append",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            # Appending on a brand-new database creates the table and inserts initial jobs
            gen.parse_cmds(["--append"])

            db_info_path = gen._write_db_info_file(os.path.join(tmpdir, "dbinfo.json"))
            worker = _DummyWorker()
            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                worker.std_run()
            finally:
                sys.argv = orig_argv

            self.assertTrue(os.path.exists(out_1))
            self.assertTrue(os.path.exists(out_2))

            # Append overlapping + internal duplicate + 1 brand-new job via --gen --append
            gen._jobs_params = [
                {"out_file": out_1, "mode": "ok"},
                {"out_file": out_3, "mode": "ok"},
                {"out_file": out_3, "mode": "ok"},
            ]
            gen.parse_cmds(["--gen", "--append"])

            report_file = os.path.join(tmpdir, "append_report.json")
            gen.create_jobs_report(out_report_file=report_file)
            rep = json.loads(pathlib.Path(report_file).read_text())
            # 2 completed preserved + 1 new pending = 3 total
            self.assertEqual(rep["job_n"]["total_n_jobs"], 3)
            self.assertEqual(rep["job_n"]["n_completed"], 2)

            # With --no-dedup, duplicate job is inserted without filtering
            gen._jobs_params = [{"out_file": out_1, "mode": "ok"}]
            gen.parse_cmds(["--append", "--no-dedup"])
            gen.create_jobs_report(out_report_file=report_file)
            rep2 = json.loads(pathlib.Path(report_file).read_text())
            self.assertEqual(rep2["job_n"]["total_n_jobs"], 4)

    def test_parallel_check_job_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "parallel_check.db")
            db_url = f"sqlite:///{db_file}"

            jobs = []
            for i in range(10):
                jobs.append(
                    {"out_file": os.path.join(tmpdir, f"item_{i}.txt"), "mode": "ok"}
                )
            # Two jobs that will fail processing (no output created)
            jobs.append({"out_file": os.path.join(tmpdir, "bad_1.txt"), "mode": "fail"})
            jobs.append({"out_file": os.path.join(tmpdir, "bad_2.txt"), "mode": "fail"})

            gen = _DummyGen(
                jobs_params=jobs,
                db_conn_str=db_url,
                db_table_name="task_parallel_check",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen.run_gen_commands()

            db_info_path = gen._write_db_info_file(os.path.join(tmpdir, "dbinfo.json"))
            worker = _DummyWorker()
            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                worker.std_run()
            finally:
                sys.argv = orig_argv

            # Delete one completed output file to verify parallel check catches missing file
            os.remove(os.path.join(tmpdir, "item_3.txt"))

            err_pids_file = os.path.join(tmpdir, "p_err_pids.txt")
            err_info_file = os.path.join(tmpdir, "p_err_info.json")
            gen.check_job_outputs(
                out_err_pid_file=err_pids_file,
                out_err_info_file=err_info_file,
                n_workers=4,
            )

            err_pids = [
                int(x)
                for x in pathlib.Path(err_pids_file).read_text().strip().splitlines()
            ]
            # item_3 (PID 4), bad_1 (PID 11), bad_2 (PID 12) should be sorted
            self.assertEqual(err_pids, [4, 11, 12])

            # Running --check -w 4 again only checks remaining unchecked (the 3 errored ones)
            gen.parse_cmds(["--check", "-w", "4"])

            with self.assertRaises(ValueError):
                gen.check_job_outputs(n_workers=0)

    def test_status_watch_inspect_errors_dry_run_and_drop(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "ops_test.db")
            db_url = f"sqlite:///{db_file}"

            run_sh = os.path.join(tmpdir, "dry_run.sh")
            cmds_sh = os.path.join(tmpdir, "dry_cmds.sh")
            slurm_run_sh = os.path.join(tmpdir, "dry_slurm.sh")
            slurm_job_dir = os.path.join(tmpdir, "dry_slurm_jobs")
            slurm_log_dir = os.path.join(tmpdir, "dry_slurm_logs")

            class _DryRunGen(_DummyGen):
                def run_gen_commands(self):
                    self.gen_command_info()
                    self.pop_params_db()
                    self.create_shell_exe(run_sh, cmds_sh, 2)
                    self.create_slurm_sub_sh(
                        "dry_job", 2048, slurm_log_dir, run_script=slurm_run_sh, job_dir=slurm_job_dir, n_jobs=2
                    )

            jobs = [
                {"out_file": os.path.join(tmpdir, "ok.txt"), "mode": "ok"},
                {"out_file": os.path.join(tmpdir, "fail.txt"), "mode": "fail"},
                {"out_file": os.path.join(tmpdir, "partial.txt"), "mode": "partial"},
            ]

            gen = _DryRunGen(
                jobs_params=jobs,
                db_conn_str=db_url,
                db_table_name="task_ops",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )

            # 1) --gen --dry-run and --append --dry-run do not create DB or scripts
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                gen.parse_cmds(["--gen", "--dry-run"])
                gen.parse_cmds(["--append", "--dry-run"])
            dry_out = buf.getvalue()
            self.assertIn("[DRY RUN] Would recreate table", dry_out)
            self.assertIn("[DRY RUN] Would append up to 3 job(s)", dry_out)
            self.assertIn("[DRY RUN] Would create shell scripts", dry_out)
            self.assertIn("[DRY RUN] Would create Slurm scripts", dry_out)
            self.assertFalse(os.path.exists(db_file))
            self.assertFalse(os.path.exists(run_sh))
            self.assertFalse(os.path.exists(slurm_run_sh))
            self.assertFalse(gen.dry_run)

            # 2) Populate queue normally using simple _DummyGen and run worker
            gen_live = _DummyGen(
                jobs_params=jobs,
                db_conn_str=db_url,
                db_table_name="task_ops",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen_live.run_gen_commands()
            db_info_path = gen_live._write_db_info_file(
                os.path.join(tmpdir, "dbinfo.json")
            )
            worker = _DummyWorker()
            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                worker.std_run()
            finally:
                sys.argv = orig_argv

            # 3) --status and --watch
            status = gen_live.get_jobs_status()
            self.assertEqual(status["total"], 3)
            self.assertEqual(status["completed"], 1)
            self.assertEqual(status["errors"], 2)
            self.assertEqual(status["pending"], 0)
            self.assertEqual(status["running"], 0)
            self.assertAlmostEqual(status["pct_complete"], 100.0 / 3.0, places=2)

            status_buf = io.StringIO()
            with contextlib.redirect_stdout(status_buf):
                gen_live.parse_cmds(["--status"])
                gen_live.parse_cmds(["--watch", "--interval", "0.01"])
            status_out = status_buf.getvalue()
            self.assertIn("Total: 3", status_out)
            self.assertIn("Completed: 1", status_out)
            self.assertIn("Errors: 2", status_out)

            with self.assertRaises(ValueError):
                gen_live.print_jobs_status(interval_secs=0)

            # 4) --inspect
            inspect_buf = io.StringIO()
            with contextlib.redirect_stdout(inspect_buf):
                gen_live.parse_cmds(["--inspect", "2"])
            inspected = json.loads(inspect_buf.getvalue())
            self.assertEqual(inspected["PID"], 2)
            self.assertTrue(inspected["Error"])
            self.assertEqual(inspected["JobParams"]["mode"], "fail")
            self.assertIn("intentional processing failure", str(inspected["ErrorInfo"]))

            with self.assertRaises(LookupError):
                gen_live.inspect_job(999, print_output=False)

            # 5) --errors and --limit
            all_errs = gen_live.get_job_errors(print_output=False)
            self.assertEqual([e["PID"] for e in all_errs], [2, 3])

            err_buf = io.StringIO()
            with contextlib.redirect_stdout(err_buf):
                gen_live.parse_cmds(["--errors", "--limit", "1"])
            err_out = err_buf.getvalue()
            self.assertIn("=== PID 2", err_out)
            self.assertNotIn("=== PID 3", err_out)

            # 6) --drop requires --yes non-interactively, and drops when --yes passed
            with unittest.mock.patch("sys.stdin.isatty", return_value=False):
                with self.assertRaises(SystemExit):
                    gen_live.parse_cmds(["--drop"])

            engine = sqlalchemy.create_engine(db_url)
            try:
                self.assertIn(
                    gen_live.db_table_name,
                    sqlalchemy.inspect(engine).get_table_names(),
                )
            finally:
                engine.dispose()

            drop_buf = io.StringIO()
            with contextlib.redirect_stdout(drop_buf):
                gen_live.parse_cmds(["--drop", "--yes"])
            self.assertIn("Dropped queue table", drop_buf.getvalue())

            engine = sqlalchemy.create_engine(db_url)
            try:
                self.assertNotIn(
                    gen_live.db_table_name,
                    sqlalchemy.inspect(engine).get_table_names(),
                )
            finally:
                engine.dispose()

    def test_slurm_job_array_and_sbatch_options(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_url = f"sqlite:///{os.path.join(tmpdir, 'slurm_array.db')}"
            gen = _DummyGen(
                jobs_params=[{"out_file": "a.txt", "mode": "ok"}],
                db_conn_str=db_url,
                db_table_name="task_slurm",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )

            run_sh = os.path.join(tmpdir, "submit_array.sh")
            job_dir = os.path.join(tmpdir, "array_jobs")
            log_dir = os.path.join(tmpdir, "array_logs")
            db_info_file = os.path.join(tmpdir, "dbinfo.json")

            gen.create_slurm_sub_sh(
                "arr_2026",
                4096,
                log_dir,
                run_script=run_sh,
                job_dir=job_dir,
                db_info_file=db_info_file,
                account_name="myacct",
                n_cores_per_job=8,
                n_xtr_cmds=2,
                n_jobs=6,
                partition="gpu",
                use_array=True,
                max_concurrent=3,
                mem_total_mb=32000,
                qos="high",
                constraint="avx2",
                gres="gpu:1",
                extra_sbatch_args=["--exclusive", "#SBATCH --mail-type=FAIL"],
            )

            array_sbatch = os.path.join(job_dir, "job_array.sbatch")
            self.assertTrue(os.path.exists(array_sbatch))
            sbatch_txt = pathlib.Path(array_sbatch).read_text()

            self.assertIn("#SBATCH --account=myacct", sbatch_txt)
            self.assertIn("#SBATCH --partition=gpu", sbatch_txt)
            self.assertIn("#SBATCH --qos=high", sbatch_txt)
            self.assertIn("#SBATCH --constraint=avx2", sbatch_txt)
            self.assertIn("#SBATCH --gres=gpu:1", sbatch_txt)
            self.assertIn("#SBATCH --job-name=arr_2026", sbatch_txt)
            self.assertIn("%A_%a_log.out", sbatch_txt)
            self.assertIn("%A_%a_log.err", sbatch_txt)
            self.assertIn("#SBATCH --ntasks=8", sbatch_txt)
            self.assertIn("#SBATCH --mem=32000", sbatch_txt)
            self.assertNotIn("--mem-per-cpu", sbatch_txt)
            self.assertIn("#SBATCH --exclusive", sbatch_txt)
            self.assertIn("#SBATCH --mail-type=FAIL", sbatch_txt)
            self.assertIn("#SBATCH --array=0-5%3", sbatch_txt)

            run_lines = pathlib.Path(run_sh).read_text().strip().splitlines()
            self.assertEqual(len(run_lines), 2)
            self.assertEqual(run_lines[0], "#!/bin/bash")
            self.assertIn("job_array.sbatch", run_lines[1])
            self.assertTrue(os.access(run_sh, os.X_OK))

            cmds_lines = (
                pathlib.Path(os.path.join(job_dir, "jobcmds.sh"))
                .read_text()
                .strip()
                .splitlines()
            )
            self.assertEqual(len(cmds_lines), 10)  # 8 + 2

            with self.assertRaises(ValueError):
                gen.create_slurm_sub_sh("bad", 1024, log_dir, n_jobs=0)

    def test_gen_template_exposes_table_schema_partition_array_and_optional_dbfile(self):
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "bin", "pbpt_gen_template.py")
        )
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        with tempfile.TemporaryDirectory() as tmpdir:
            env = dict(os.environ)
            env["PYTHONPATH"] = repo_root + (
                os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
            )

            # Omitting --table fails cleanly
            proc_missing = subprocess.run(
                [sys.executable, script_path, "-o", tmpdir],
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(proc_missing.returncode, 0)

            # With --prefix and --table, helper script/dbinfo names are prefixed so multiple tasks can share tmpdir
            proc = subprocess.run(
                [
                    sys.executable,
                    script_path,
                    "-o",
                    tmpdir,
                    "-p",
                    "t1",
                    "--slurm",
                    "--partition",
                    "highmem",
                    "--array",
                    "--max_concurrent",
                    "4",
                    "--table",
                    "my_jobs",
                    "--schema",
                    "eo_batch",
                ],
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertEqual(proc.returncode, 0)

            gen_cmds_py = pathlib.Path(os.path.join(tmpdir, "t1_gen_cmds.py")).read_text()
            compile(gen_cmds_py, os.path.join(tmpdir, "t1_gen_cmds.py"), "exec")

            self.assertNotIn("db_conn_file=", gen_cmds_py)
            self.assertIn('db_table_name="my_jobs"', gen_cmds_py)
            self.assertIn('db_schema="eo_batch"', gen_cmds_py)
            self.assertIn('partition="highmem"', gen_cmds_py)
            self.assertIn("use_array=True", gen_cmds_py)
            self.assertIn("max_concurrent=4", gen_cmds_py)
            self.assertIn("os.path.dirname(os.path.abspath(__file__))", gen_cmds_py)
            self.assertIn('db_info_file="t1_pbpt_lcl_db_info.json"', gen_cmds_py)
            self.assertIn('run_script="t1_run_exe_analysis.sh"', gen_cmds_py)
            self.assertIn('job_dir="t1_job_scripts"', gen_cmds_py)

            expected_sh_flags = {
                "t1_run_gen_cmds.sh": '--gen "$@"',
                "t1_run_status.sh": '--status "$@"',
                "t1_run_errors.sh": '--errors "$@"',
                "t1_run_report.sh": '--report "$@" | more',
                "t1_run_reset_errs.sh": '--rmouts --error "$@"',
                "t1_run_checks.sh": '--check "$@"',
            }
            for sh_name, flag_snippet in expected_sh_flags.items():
                sh_path = os.path.join(tmpdir, sh_name)
                self.assertTrue(os.path.exists(sh_path), f"Missing {sh_name}")
                self.assertTrue(os.access(sh_path, os.X_OK), f"Not executable: {sh_name}")
                sh_text = pathlib.Path(sh_path).read_text()
                self.assertIn(flag_snippet, sh_text)

    def test_multiple_tasks_concurrent_same_database(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "shared_multi_task.db")
            db_url = f"sqlite:///{db_file}"

            # Omitting or blank db_table_name is rejected
            with self.assertRaises(ValueError):
                get_job_model("")
            with self.assertRaises(ValueError):
                _DummyGen(jobs_params=[], db_conn_str=db_url)
            with self.assertRaises(ValueError):
                _DummyGen(jobs_params=[], db_conn_str=db_url, db_table_name="   ")

            worker_no_table = _DummyWorker(queue_db_info={"db_conn_str": db_url})
            with self.assertRaises(ValueError):
                worker_no_table.check_db_info()

            alpha_jobs = [
                {"out_file": os.path.join(tmpdir, f"alpha_{i}.txt"), "mode": "ok"}
                for i in range(4)
            ]
            beta_jobs = [
                {"out_file": os.path.join(tmpdir, f"beta_{i}.txt"), "mode": "ok"}
                for i in range(3)
            ] + [{"out_file": os.path.join(tmpdir, "beta_err.txt"), "mode": "fail"}]

            gen_alpha = _DummyGen(
                jobs_params=alpha_jobs,
                db_conn_str=db_url,
                db_table_name="task_alpha",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen_beta = _DummyGen(
                jobs_params=beta_jobs,
                db_conn_str=db_url,
                db_table_name="task_beta",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )

            gen_alpha.run_gen_commands()
            gen_beta.run_gen_commands()

            db_info_alpha_path = gen_alpha._write_db_info_file(
                os.path.join(tmpdir, "alpha_dbinfo.json")
            )
            db_info_beta_path = gen_beta._write_db_info_file(
                os.path.join(tmpdir, "beta_dbinfo.json")
            )

            alpha_info = json.loads(pathlib.Path(db_info_alpha_path).read_text())
            beta_info = json.loads(pathlib.Path(db_info_beta_path).read_text())
            self.assertEqual(alpha_info["db_table_name"], "task_alpha")
            self.assertEqual(beta_info["db_table_name"], "task_beta")

            # Run both tasks' workers concurrently against the same database
            def _run_worker_in_thread(dbinfo_json_path):
                w = _DummyWorker()
                # Use parse_cmds(argv=...) + manual claim/process loop to avoid mutating shared sys.argv across threads
                self.assertTrue(w.parse_cmds(argv=["--dbinfo", dbinfo_json_path]))
                try:
                    while True:
                        claimed = w._claim_next_job()
                        if claimed is None:
                            break
                        w.job_pid, w.params = claimed
                        try:
                            w.check_required_fields()
                            w.do_processing()
                        except Exception as exc:
                            w.record_process_error({"error": str(exc)})
                            continue
                        w.completed_processing()
                finally:
                    w.dispose_engine()

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                f_alpha = pool.submit(_run_worker_in_thread, db_info_alpha_path)
                f_beta = pool.submit(_run_worker_in_thread, db_info_beta_path)
                f_alpha.result()
                f_beta.result()

            status_alpha = gen_alpha.get_jobs_status()
            status_beta = gen_beta.get_jobs_status()
            self.assertEqual(status_alpha["total"], 4)
            self.assertEqual(status_alpha["completed"], 4)
            self.assertEqual(status_alpha["errors"], 0)
            self.assertEqual(status_beta["total"], 4)
            self.assertEqual(status_beta["completed"], 3)
            self.assertEqual(status_beta["errors"], 1)

            # Dropping task_alpha does NOT drop or disturb task_beta
            gen_alpha.parse_cmds(["--drop", "--yes"])
            engine = sqlalchemy.create_engine(db_url)
            try:
                tables = sqlalchemy.inspect(engine).get_table_names()
                self.assertNotIn("task_alpha", tables)
                self.assertIn("task_beta", tables)
            finally:
                engine.dispose()

            # --table CLI override allows managing task_beta from another GenCmds instance
            gen_alpha.parse_cmds(["--table", "task_beta", "--check"])
            self.assertEqual(gen_alpha.db_table_name, "task_beta")
            self.assertEqual(gen_alpha.get_jobs_status()["checked"], 3)

    def test_db_engine_env_conn_and_worker_debug_modes(self):
        from pbprocesstools.pbpt_q_process import (
            _DBSessionMixin,
            _create_db_engine,
            _sanitize_json,
        )

        self.assertIsNone(_sanitize_json(None))

        with unittest.mock.patch("sqlalchemy.create_engine") as mock_ce:
            _create_db_engine(
                "postgresql+psycopg2://u:p@localhost/db", pool_size=3, max_overflow=1
            )
            mock_ce.assert_called_once_with(
                "postgresql+psycopg2://u:p@localhost/db",
                pool_pre_ping=True,
                pool_size=3,
                max_overflow=1,
            )

        base_mixin = _DBSessionMixin()
        base_mixin._init_db_state()
        self.assertEqual(base_mixin._pool_params(), (1, 0))
        with self.assertRaises(NotImplementedError):
            base_mixin._get_conn_str()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "worker_debug.db")
            db_url = f"sqlite:///{db_file}"

            # Env fallback in worker and _get_conn_str error when missing
            w_empty = _DummyWorker(db_table_name="dbg_tbl")
            orig_env = os.environ.pop("PBPT_DB_CONN_STR", None)
            try:
                with self.assertRaises(ValueError):
                    w_empty._get_conn_str()
                with self.assertRaises(ValueError):
                    w_empty.check_db_info()

                os.environ["PBPT_DB_CONN_STR"] = db_url
                self.assertEqual(w_empty._get_conn_str(), db_url)
                self.assertTrue(w_empty.check_db_info())
                w_empty.dispose_engine()
            finally:
                if orig_env is not None:
                    os.environ["PBPT_DB_CONN_STR"] = orig_env
                else:
                    os.environ.pop("PBPT_DB_CONN_STR", None)

            # Unreachable DB raises ConnectionError
            w_bad = _DummyWorker(
                queue_db_info={
                    "db_conn_str": "postgresql://nonexistent:bad@127.0.0.1:1/nodb",
                    "db_table_name": "dbg_tbl",
                }
            )
            with unittest.mock.patch.object(
                w_bad, "_session_scope", side_effect=RuntimeError("boom")
            ):
                with self.assertRaises(ConnectionError):
                    w_bad.check_db_info()

            # check_required_fields edge cases
            self.assertIsNone(w_empty.params)
            with self.assertRaises(ValueError):
                w_empty.check_required_fields()
            w_empty.set_params({"out_file": "x.txt"})
            with self.assertRaises(KeyError):
                w_empty.check_required_fields()
            w_empty.set_params({})
            with self.assertRaises(KeyError):
                w_empty.check_required_fields()

            # parse_cmds missing dbinfo returns False
            self.assertFalse(
                w_empty.parse_cmds(argv=["--dbinfo", os.path.join(tmpdir, "nope.json")])
            )

            # Populate queue and exercise debug -j, -p, -r, out_cmp_file, and engine switch
            out_cmp = os.path.join(tmpdir, "done.cmp")
            out_1 = os.path.join(tmpdir, "dbg_out1.txt")
            gen = _DummyGen(
                jobs_params=[
                    {"out_file": out_1, "mode": "ok", "out_cmp_file": out_cmp},
                ],
                db_conn_str=db_url,
                db_table_name="dbg_tbl",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen.run_gen_commands()
            db_info_path = gen._write_db_info_file(os.path.join(tmpdir, "dbg.json"))

            # Change conn_str on _DBSessionMixin triggers dispose_engine()
            db_file_2 = os.path.join(tmpdir, "worker_debug_2.db")
            gen._get_engine()
            gen.db_conn_str = f"sqlite:///{db_file_2}"
            gen._get_engine()
            gen.dispose_engine()
            gen.db_conn_str = db_url

            # Debug -j 1 -p (prints params), -j 1 (executes without updating DB), -j 1 -r (removes outputs)
            orig_argv = sys.argv
            try:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path, "-j", "1", "-p"]
                    _DummyWorker().std_run()
                self.assertIn("dbg_out1.txt", buf.getvalue())

                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path, "-j", "1"]
                _DummyWorker().std_run()
                self.assertTrue(os.path.exists(out_1))

                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path, "-j", "1", "-r"]
                _DummyWorker().std_run()
                self.assertFalse(os.path.exists(out_1))

                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path, "-j", "999"]
                with self.assertRaises(Exception):
                    _DummyWorker().std_run()
            finally:
                sys.argv = orig_argv

            # Normal run with transient DB failure recovery + out_cmp_file touched
            w_retry = _DummyWorker()
            orig_claim = w_retry._claim_next_job
            calls = {"n": 0}

            def _flaky_claim():
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("transient connection drop")
                return orig_claim()

            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                with unittest.mock.patch.object(
                    w_retry, "_claim_next_job", side_effect=_flaky_claim
                ):
                    with unittest.mock.patch("time.sleep"):
                        w_retry.std_run(max_db_retries=3)
            finally:
                sys.argv = orig_argv

            self.assertTrue(os.path.exists(out_1))
            self.assertTrue(os.path.exists(out_cmp))

            # Exceeding max_db_retries raises
            w_fail = _DummyWorker()
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                with unittest.mock.patch.object(
                    w_fail, "_claim_next_job", side_effect=RuntimeError("always down")
                ):
                    with unittest.mock.patch("time.sleep"):
                        with self.assertRaises(RuntimeError):
                            w_fail.std_run(max_db_retries=2)
            finally:
                sys.argv = orig_argv

    def test_gen_cmds_edge_cases_and_cli_branches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "edge_cases.db")
            db_url = f"sqlite:///{db_file}"

            orig_env = os.environ.pop("PBPT_DB_CONN_STR", None)
            try:
                with self.assertRaises(ValueError):
                    _DummyGen([], db_table_name="t_err")
                with self.assertRaises(ValueError):
                    _DummyGen([], db_conn_str="   ", db_table_name="t_err")

                os.environ["PBPT_DB_CONN_STR"] = db_url
                gen_env = _DummyGen([], db_table_name="t_env")
                self.assertEqual(gen_env.db_conn_str, db_url)
            finally:
                if orig_env is not None:
                    os.environ["PBPT_DB_CONN_STR"] = orig_env
                else:
                    os.environ.pop("PBPT_DB_CONN_STR", None)

            gen = _DummyGen(
                jobs_params=[
                    {"out_file": os.path.join(tmpdir, "e1.txt"), "mode": "ok"},
                    {"out_file": os.path.join(tmpdir, "e2.txt"), "mode": "ok"},
                    {"out_file": os.path.join(tmpdir, "e3.txt"), "mode": "ok"},
                ],
                db_conn_str=db_url,
                db_table_name="edge_tbl",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )

            # _load_process_tool_instance validation
            gen_no_mod = _DummyGen([], db_conn_str=db_url, db_table_name="t_nomod")
            with self.assertRaises(ValueError):
                gen_no_mod._load_process_tool_instance()
            gen_no_cls = _DummyGen(
                [],
                db_conn_str=db_url,
                db_table_name="t_nocls",
                process_tools_mod="pbprocesstools.pbpt_q_process",
            )
            with self.assertRaises(ValueError):
                gen_no_cls._load_process_tool_instance()

            # Empty queue report and empty errors
            gen._ensure_schema_and_table(gen._get_engine(), drop_existing=True)
            gen.dispose_engine()
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                gen.parse_cmds(["--report"])
                gen.parse_cmds(["--errors"])
                gen.parse_cmds([])  # prints help
            out_txt = buf.getvalue()
            self.assertIn("{}", out_txt)
            self.assertIn("No errors or incomplete jobs found.", out_txt)

            # Populate 3 jobs: 1 completed+file present, 1 started incomplete, 1 never started
            gen.run_gen_commands()
            JobModel = gen._get_job_model()
            pathlib.Path(os.path.join(tmpdir, "e1.txt")).write_text("ok")
            with gen._session_scope() as ses:
                ses.execute(
                    sqlalchemy.update(JobModel)
                    .where(JobModel.PID == 1)
                    .values(
                        Started=True,
                        Completed=True,
                        Start=sqlalchemy.func.now(),
                        End=sqlalchemy.func.now(),
                    )
                )
                ses.execute(
                    sqlalchemy.update(JobModel)
                    .where(JobModel.PID == 2)
                    .values(Started=True, Completed=False, ErrorInfo={"custom": "msg"})
                )
                ses.commit()
            gen.dispose_engine()

            # check_job_outputs with small batch_size=1 catches Started-incomplete & Never-started
            err_pids_f = os.path.join(tmpdir, "edge_errs.txt")
            err_info_f = os.path.join(tmpdir, "edge_errs.json")
            gen.check_job_outputs(
                out_err_pid_file=err_pids_f,
                out_err_info_file=err_info_f,
                batch_size=1,
            )
            err_info = json.loads(pathlib.Path(err_info_f).read_text())
            self.assertEqual(err_info["2"], "Started but did not complete.")
            self.assertEqual(err_info["3"], "Never Started.")

            # Print custom dict ErrorInfo (without 'error'/'traceback'), Started-incomplete status, & console report
            with gen._session_scope() as ses:
                ses.execute(
                    sqlalchemy.update(JobModel)
                    .where(JobModel.PID == 2)
                    .values(Error=True, ErrorInfo={"custom": "msg"})
                )
                ses.execute(
                    sqlalchemy.update(JobModel)
                    .where(JobModel.PID == 3)
                    .values(Started=True, Completed=False, Error=False)
                )
                ses.commit()
            gen.dispose_engine()

            buf2 = io.StringIO()
            with contextlib.redirect_stdout(buf2):
                gen.get_job_errors(print_output=True)
                gen.create_jobs_report(out_report_file=None)
            self.assertIn('"custom": "msg"', buf2.getvalue())
            self.assertIn("Started but did not complete.", buf2.getvalue())

            # Zero-error check_job_outputs creates empty pid file and {} json
            with gen._session_scope() as ses:
                ses.execute(
                    sqlalchemy.update(JobModel).values(
                        Started=True, Completed=True, Checked=False, Error=False
                    )
                )
                ses.commit()
            gen.dispose_engine()
            pathlib.Path(os.path.join(tmpdir, "e2.txt")).write_text("ok")
            pathlib.Path(os.path.join(tmpdir, "e3.txt")).write_text("ok")
            zero_pids_f = os.path.join(tmpdir, "zero_errs.txt")
            zero_info_f = os.path.join(tmpdir, "zero_errs.json")
            gen.check_job_outputs(
                out_err_pid_file=zero_pids_f,
                out_err_info_file=zero_info_f,
                n_workers=2,
                batch_size=1,
            )
            self.assertTrue(os.path.exists(zero_pids_f))
            self.assertEqual(json.loads(pathlib.Path(zero_info_f).read_text()), {})

            # remove_job_outputs errors & pids & all_jobs with batch_size=1
            with self.assertRaises(ValueError):
                gen.remove_job_outputs()
            gen.remove_job_outputs(pids=[1], batch_size=1)
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "e1.txt")))
            gen.remove_job_outputs(all_jobs=True, batch_size=1)
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "e2.txt")))

            # CLI --rmouts / --reset without selector raise SystemExit
            with self.assertRaises(SystemExit):
                gen.parse_cmds(["--rmouts"])
            with self.assertRaises(SystemExit):
                gen.parse_cmds(["--reset"])

            # _format_duration with days > 0
            self.assertEqual(gen._format_duration(90061), "1d 01:01:01")

            # --watch sleep loop transitioning from pending to completed
            poll_seq = iter(
                [
                    {
                        "total": 2,
                        "pending": 1,
                        "running": 0,
                        "completed": 1,
                        "checked": 1,
                        "errors": 0,
                        "pct_complete": 50.0,
                        "throughput_per_hour": 120.0,
                        "eta_seconds": 30.0,
                    },
                    {
                        "total": 2,
                        "pending": 0,
                        "running": 0,
                        "completed": 2,
                        "checked": 2,
                        "errors": 0,
                        "pct_complete": 100.0,
                        "throughput_per_hour": 120.0,
                        "eta_seconds": None,
                    },
                ]
            )
            with unittest.mock.patch.object(
                gen, "get_jobs_status", side_effect=lambda _dispose=False: next(poll_seq)
            ):
                with unittest.mock.patch("time.sleep") as mock_sleep:
                    gen.print_jobs_status(watch=True, interval_secs=0.5)
                    mock_sleep.assert_called_once_with(0.5)

            # Interactive --drop: decline then confirm
            with unittest.mock.patch("sys.stdin.isatty", return_value=True):
                with unittest.mock.patch("builtins.input", return_value="n"):
                    gen.parse_cmds(["--drop"])
                eng = sqlalchemy.create_engine(db_url)
                try:
                    self.assertIn("edge_tbl", sqlalchemy.inspect(eng).get_table_names())
                finally:
                    eng.dispose()

                with unittest.mock.patch("builtins.input", return_value="y"):
                    gen.parse_cmds(["--drop"])
                eng = sqlalchemy.create_engine(db_url)
                try:
                    self.assertNotIn("edge_tbl", sqlalchemy.inspect(eng).get_table_names())
                finally:
                    eng.dispose()

            # _ensure_schema_and_table PostgreSQL schema creation branch
            gen_schema = _DummyGen(
                [],
                db_conn_str=db_url,
                db_table_name="schema_tbl",
                db_schema="my_pg_schema",
            )
            fake_engine = unittest.mock.MagicMock()
            fake_engine.dialect.name = "postgresql"
            fake_conn = unittest.mock.MagicMock()
            fake_engine.begin.return_value.__enter__.return_value = fake_conn
            fake_model = unittest.mock.MagicMock()
            fake_model.__table__ = unittest.mock.MagicMock()
            with unittest.mock.patch.object(
                gen_schema, "_get_job_model", return_value=fake_model
            ):
                gen_schema._ensure_schema_and_table(fake_engine, drop_existing=False)
                fake_conn.execute.assert_called_once()
                fake_model.__table__.create.assert_called_once_with(
                    fake_engine, checkfirst=True
                )

            # create_slurm_sub_sh with use_array=False, n_jobs=2, module_load=None
            s_run = os.path.join(tmpdir, "sub_multi.sh")
            s_dir = os.path.join(tmpdir, "multi_jobs")
            s_log = os.path.join(tmpdir, "multi_logs")
            gen.create_slurm_sub_sh(
                "multi_job",
                2048,
                s_log,
                run_script=s_run,
                job_dir=s_dir,
                db_info_file=os.path.join(tmpdir, "m_dbinfo.json"),
                account_name="acct1",
                n_jobs=2,
                module_load=None,
                use_array=False,
            )
            s_run_lines = pathlib.Path(s_run).read_text().strip().splitlines()
            self.assertEqual(len(s_run_lines), 3)
            self.assertEqual(s_run_lines[0], "#!/bin/bash")
            self.assertTrue(os.access(s_run, os.X_OK))
            sbatch0 = pathlib.Path(os.path.join(s_dir, "job_file_0.sbatch")).read_text()
            self.assertIn("#SBATCH --account=acct1", sbatch0)
            self.assertNotIn("module load", sbatch0)

    def test_v4_bugfixes_and_regressions(self):
        from pbprocesstools.pbpt_q_process import _mask_db_url

        # 1) Password masking helper
        self.assertEqual(
            _mask_db_url("postgresql+psycopg2://alice:s3cr3t@db.example.com:5432/q"),
            "postgresql+psycopg2://alice:***@db.example.com:5432/q",
        )
        self.assertEqual(_mask_db_url(":::not-a-url"), "<invalid-db-url>")

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "regress.db")
            db_url = f"sqlite:///{db_file}"

            jobs = [
                {"out_file": os.path.join(tmpdir, "ok1.txt"), "mode": "ok"},
                {"out_file": os.path.join(tmpdir, "ok2.txt"), "mode": "ok"},
                {"out_file": os.path.join(tmpdir, "fail1.txt"), "mode": "fail"},
            ]
            gen = _DummyGen(
                jobs_params=jobs,
                db_conn_str=db_url,
                db_table_name="task_regress",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen.run_gen_commands()
            db_info_path = gen._write_db_info_file(
                os.path.join(tmpdir, "dbinfo.json")
            )

            # 2) create_shell_exe writes #!/bin/bash and sets executable bit
            sh_run = os.path.join(tmpdir, "run_local.sh")
            sh_cmds = os.path.join(tmpdir, "cmds_local.sh")
            gen.create_shell_exe(sh_run, sh_cmds, 2, db_info_file=db_info_path)
            sh_lines = pathlib.Path(sh_run).read_text().splitlines()
            self.assertEqual(sh_lines[0], "#!/bin/bash")
            self.assertTrue(os.access(sh_run, os.X_OK))

            # 3) Empty-string Slurm options are omitted from sbatch script
            s_run = os.path.join(tmpdir, "sub_empty.sh")
            s_dir = os.path.join(tmpdir, "empty_opts_jobs")
            s_log = os.path.join(tmpdir, "empty_opts_logs")
            gen.create_slurm_sub_sh(
                "empty_opts",
                1024,
                s_log,
                run_script=s_run,
                job_dir=s_dir,
                db_info_file=db_info_path,
                account_name="",
                partition="",
                qos="",
                constraint="",
                gres="",
                module_load="",
                n_jobs=1,
            )
            sbatch_txt = pathlib.Path(
                os.path.join(s_dir, "job_file_0.sbatch")
            ).read_text()
            for flag in (
                "--account=",
                "--partition=",
                "--qos=",
                "--constraint=",
                "--gres=",
                "module load",
            ):
                self.assertNotIn(flag, sbatch_txt)

            # 4) Run worker and verify quartiles in create_jobs_report() (Python 3.10+)
            worker = _DummyWorker()
            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path]
                worker.std_run()
            finally:
                sys.argv = orig_argv

            rep_file = os.path.join(tmpdir, "report.json")
            gen.create_jobs_report(out_report_file=rep_file)
            rep = json.loads(pathlib.Path(rep_file).read_text())
            self.assertIn("time_quartiles_secs", rep["job_times"])
            self.assertEqual(len(rep["job_times"]["time_quartiles_secs"]), 3)

            # 5) check_job_outputs preserves worker ErrorInfo traceback & Started=True
            err_before = gen.inspect_job(3, print_output=False)
            self.assertTrue(err_before["Error"])
            self.assertTrue(err_before["Started"])
            self.assertIn("traceback", err_before["ErrorInfo"])

            gen.check_job_outputs()
            err_after = gen.inspect_job(3, print_output=False)
            self.assertTrue(err_after["Error"])
            self.assertTrue(err_after["Started"])
            self.assertIn("traceback", err_after["ErrorInfo"])
            self.assertIn(
                "intentional processing failure", err_after["ErrorInfo"]["error"]
            )

            # 6) _claim_next_job skips jobs with Started=False, Completed=False, Error=True
            job_model = get_job_model("task_regress")
            eng = sqlalchemy.create_engine(db_url)
            try:
                with eng.begin() as conn:
                    conn.execute(
                        sqlalchemy.update(job_model)
                        .where(job_model.PID == 3)
                        .values(Started=False, Completed=False, Error=True)
                    )
            finally:
                eng.dispose()

            claim_worker = _DummyWorker()
            claim_worker.queue_db_info = {
                "db_conn_str": db_url,
                "db_table_name": "task_regress",
            }
            try:
                self.assertIsNone(claim_worker._claim_next_job())
            finally:
                claim_worker.dispose_engine()

            # 7) std_run raises SystemExit(1) when parse_cmds() returns False
            bad_worker = _DummyWorker()
            with unittest.mock.patch.object(
                bad_worker, "parse_cmds", return_value=False
            ):
                with self.assertRaises(SystemExit) as ctx:
                    bad_worker.std_run()
                self.assertEqual(ctx.exception.code, 1)

            # 8) std_run -j with non-existent PID raises LookupError
            dbg_worker = _DummyWorker()
            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", db_info_path, "-j", "9999"]
                with self.assertRaises(LookupError):
                    dbg_worker.std_run()
            finally:
                sys.argv = orig_argv

            # 9) std_run retries transient DB failure on completed_processing and record_process_error
            retry_jobs = [
                {"out_file": os.path.join(tmpdir, "r_ok.txt"), "mode": "ok"},
                {"out_file": os.path.join(tmpdir, "r_fail.txt"), "mode": "fail"},
            ]
            gen_retry = _DummyGen(
                jobs_params=retry_jobs,
                db_conn_str=db_url,
                db_table_name="task_retry",
                process_tools_path=os.path.dirname(__file__),
                process_tools_mod=os.path.splitext(os.path.basename(__file__))[0],
                process_tools_cls="_DummyWorker",
            )
            gen_retry.run_gen_commands()
            retry_info = gen_retry._write_db_info_file(
                os.path.join(tmpdir, "retry_dbinfo.json")
            )

            retry_worker = _DummyWorker()
            orig_complete = retry_worker.completed_processing
            orig_record_err = retry_worker.record_process_error
            comp_calls = {"n": 0}
            err_calls = {"n": 0}

            def flaky_complete(**kwargs):
                comp_calls["n"] += 1
                if comp_calls["n"] == 1:
                    raise sqlalchemy.exc.OperationalError("stmt", {}, Exception("drop"))
                return orig_complete(**kwargs)

            def flaky_record_err(err_dict):
                err_calls["n"] += 1
                if err_calls["n"] == 1:
                    raise sqlalchemy.exc.OperationalError("stmt", {}, Exception("drop"))
                return orig_record_err(err_dict)

            orig_argv = sys.argv
            try:
                sys.argv = ["dummy_worker.py", "--dbinfo", retry_info]
                with unittest.mock.patch.object(
                    retry_worker, "completed_processing", side_effect=flaky_complete
                ):
                    with unittest.mock.patch.object(
                        retry_worker,
                        "record_process_error",
                        side_effect=flaky_record_err,
                    ):
                        with unittest.mock.patch("time.sleep"):
                            retry_worker.std_run()
            finally:
                sys.argv = orig_argv

            self.assertEqual(comp_calls["n"], 2)
            self.assertEqual(err_calls["n"], 2)
            retry_status = gen_retry.get_jobs_status()
            self.assertEqual(retry_status["completed"], 1)
            self.assertEqual(retry_status["errors"], 1)

            # 10) check_db_info masks password in ConnectionError message
            bad_conn_worker = _DummyWorker()
            bad_conn_worker.queue_db_info = {
                "db_conn_str": "postgresql+psycopg2://user:topsecret@127.0.0.1:1/missing",
                "db_table_name": "task_regress",
            }
            with unittest.mock.patch.object(
                bad_conn_worker,
                "_get_engine",
                side_effect=sqlalchemy.exc.OperationalError(
                    "connect", {}, Exception("refused")
                ),
            ):
                with self.assertRaises(ConnectionError) as conn_ctx:
                    bad_conn_worker.check_db_info()
                self.assertNotIn("topsecret", str(conn_ctx.exception))
                self.assertIn("***", str(conn_ctx.exception))

    def test_gen_template_all_variants_and_overwrite(self):
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "bin", "pbpt_gen_template.py")
        )
        spec = importlib.util.spec_from_file_location("pbpt_gen_template", script_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        with self.assertRaises(FileNotFoundError):
            mod.gen_pb_process_tools_template(
                output_dir="/nonexistent/template/out/dir",
                db_table_name="missing_dir_tbl",
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValueError):
                mod.gen_pb_process_tools_template(
                    output_dir=tmpdir,
                    db_table_name="   ",
                )

            for idx, (use_slurm, sing) in enumerate(
                [
                    (False, False),
                    (False, "/opt/img.sif"),
                    (True, False),
                    (True, "/opt/img.sif"),
                ]
            ):
                sub = os.path.join(tmpdir, f"variant_{idx}")
                os.makedirs(sub)
                mod.gen_pb_process_tools_template(
                    output_dir=sub,
                    dbfile="/db/conn.txt",
                    out_file_prefix=f"v{idx}",
                    use_slurm=use_slurm,
                    singularity=sing,
                    singularity_paths=["/data:/data"] if sing else None,
                    db_table_name=f"tbl_{idx}",
                )
                gen_file = os.path.join(sub, f"v{idx}_gen_cmds.py")
                ana_file = os.path.join(sub, f"v{idx}_perform_analysis.py")
                status_sh = os.path.join(sub, f"v{idx}_run_status.sh")
                errors_sh = os.path.join(sub, f"v{idx}_run_errors.sh")
                gen_txt = pathlib.Path(gen_file).read_text()
                ana_txt = pathlib.Path(ana_file).read_text()
                compile(gen_txt, gen_file, "exec")
                compile(ana_txt, ana_file, "exec")
                self.assertTrue(os.path.exists(status_sh))
                self.assertTrue(os.path.exists(errors_sh))
                if sing:
                    self.assertIn(
                        "singularity exec --bind /data:/data /opt/img.sif python",
                        gen_txt,
                    )
                    self.assertIn(
                        "singularity exec --bind /data:/data /opt/img.sif python",
                        pathlib.Path(status_sh).read_text(),
                    )
                if use_slurm:
                    self.assertIn("create_slurm_sub_sh", gen_txt)
                    self.assertIn("max_concurrent=None", gen_txt)
                else:
                    self.assertIn("create_shell_exe", gen_txt)

            # Existing files without overwrite=True raises FileExistsError; overwrite=True succeeds
            v0_dir = os.path.join(tmpdir, "variant_0")
            with self.assertRaises(FileExistsError):
                mod.gen_pb_process_tools_template(
                    output_dir=v0_dir,
                    out_file_prefix="v0",
                    db_table_name="tbl_0",
                    overwrite=False,
                )
            mod.gen_pb_process_tools_template(
                output_dir=v0_dir,
                out_file_prefix="v0",
                db_table_name="tbl_0_new",
                overwrite=True,
            )
            self.assertIn(
                'db_table_name="tbl_0_new"',
                pathlib.Path(os.path.join(v0_dir, "v0_gen_cmds.py")).read_text(),
            )
            self.assertIn(
                '--status "$@"',
                pathlib.Path(os.path.join(v0_dir, "v0_run_status.sh")).read_text(),
            )
            self.assertIn(
                '--errors "$@"',
                pathlib.Path(os.path.join(v0_dir, "v0_run_errors.sh")).read_text(),
            )


if __name__ == "__main__":
    unittest.main()



