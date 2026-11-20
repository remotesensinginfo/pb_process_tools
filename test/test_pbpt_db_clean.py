import contextlib
import datetime
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

import sqlalchemy

from pbprocesstools.pbpt_db_clean import (
    PBPTDatabaseCleaner,
    _format_bytes,
    _format_table_report,
    main,
    resolve_db_conn_str,
)
from pbprocesstools.pbpt_q_process import get_job_model


def _create_queue_table_with_rows(engine, table_name, rows):
    model = get_job_model(table_name)
    model.__table__.create(engine, checkfirst=True)
    if rows:
        with engine.begin() as conn:
            conn.execute(sqlalchemy.insert(model), rows)


class PBPTDatabaseCleanerTest(unittest.TestCase):
    def test_resolve_db_conn_str_and_formatters(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn_file = os.path.join(tmpdir, "db_conn.txt")
            pathlib.Path(conn_file).write_text(
                "postgresql+psycopg2://u:p@host:5432/mydb\n"
            )
            self.assertEqual(
                resolve_db_conn_str(dbfile=conn_file),
                "postgresql+psycopg2://u:p@host:5432/mydb",
            )
            self.assertEqual(
                resolve_db_conn_str(db_conn_str="  sqlite:///abc.db  "),
                "sqlite:///abc.db",
            )

            with self.assertRaises(ValueError):
                resolve_db_conn_str(db_conn_str="   ")

            orig_env = os.environ.pop("PBPT_DB_CONN_STR", None)
            try:
                with self.assertRaises(ValueError):
                    resolve_db_conn_str()
                os.environ["PBPT_DB_CONN_STR"] = "postgresql://env:pw@db/q"
                self.assertEqual(
                    resolve_db_conn_str(), "postgresql://env:pw@db/q"
                )
            finally:
                if orig_env is not None:
                    os.environ["PBPT_DB_CONN_STR"] = orig_env
                else:
                    os.environ.pop("PBPT_DB_CONN_STR", None)

        self.assertEqual(_format_bytes(None), "-")
        self.assertEqual(_format_bytes(512), "512 B")
        self.assertEqual(_format_bytes(2048), "2.0 KB")
        self.assertEqual(_format_bytes(5 * 1024**4), "5.0 TB")
        self.assertEqual(_format_table_report([]), "No pb_process_tools job queue tables found.")

    def test_list_filter_and_drop_safety_against_non_queue_and_active_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "clean_test.db")
            db_url = f"sqlite:///{db_file}"
            engine = sqlalchemy.create_engine(db_url)

            now = datetime.datetime.now()
            old_ts = now - datetime.timedelta(days=10)

            try:
                # 1. Completed old queue table
                _create_queue_table_with_rows(
                    engine,
                    "job_old_done",
                    [
                        {
                            "JobParams": {"id": 1},
                            "Start": old_ts - datetime.timedelta(hours=1),
                            "End": old_ts,
                            "Started": True,
                            "Completed": True,
                            "Checked": True,
                            "Error": False,
                        }
                    ],
                )
                # 2. Empty queue table
                _create_queue_table_with_rows(engine, "job_empty", [])
                # 3. Finished with errors
                _create_queue_table_with_rows(
                    engine,
                    "job_errored",
                    [
                        {
                            "JobParams": {"id": 2},
                            "Start": now - datetime.timedelta(days=2),
                            "End": None,
                            "Started": True,
                            "Completed": False,
                            "Checked": False,
                            "Error": True,
                            "ErrorInfo": {"error": "boom"},
                        }
                    ],
                )
                # 4. Active queue table (pending job)
                _create_queue_table_with_rows(
                    engine,
                    "job_active",
                    [
                        {
                            "JobParams": {"id": 3},
                            "Start": now,
                            "End": None,
                            "Started": True,
                            "Completed": False,
                            "Checked": False,
                            "Error": False,
                        }
                    ],
                )
                # 5. Unrelated user application table (must never be listed/dropped)
                with engine.begin() as conn:
                    conn.execute(
                        sqlalchemy.text(
                            "CREATE TABLE app_users (id INTEGER PRIMARY KEY, name TEXT)"
                        )
                    )
            finally:
                engine.dispose()

            cleaner = PBPTDatabaseCleaner(db_conn_str=db_url)
            listed = cleaner.list_queue_tables(all_schemas=True)
            names_to_info = {item["table"]: item for item in listed}

            self.assertNotIn("app_users", names_to_info)
            self.assertEqual(
                set(names_to_info.keys()),
                {"job_old_done", "job_empty", "job_errored", "job_active"},
            )
            self.assertEqual(names_to_info["job_old_done"]["state"], "COMPLETED")
            self.assertEqual(names_to_info["job_empty"]["state"], "EMPTY")
            self.assertEqual(names_to_info["job_errored"]["state"], "FINISHED_WITH_ERRORS")
            self.assertEqual(names_to_info["job_active"]["state"], "ACTIVE")

            # Format report includes table summary
            report_txt = _format_table_report(listed)
            self.assertIn("job_old_done", report_txt)
            self.assertIn("COMPLETED", report_txt)

            # Pattern & table filters
            self.assertEqual(
                [i["table"] for i in cleaner.list_queue_tables(pattern="job_e*")],
                ["job_empty", "job_errored"],
            )
            self.assertEqual(
                [i["table"] for i in cleaner.list_queue_tables(tables=["job_active"])],
                ["job_active"],
            )

            # select_tables_for_cleanup requires at least one criterion
            with self.assertRaises(ValueError):
                cleaner.select_tables_for_cleanup()

            # Older-than + completed selection
            old_completed = cleaner.select_tables_for_cleanup(
                completed=True, older_than_days=5
            )
            self.assertEqual([i["table"] for i in old_completed], ["job_old_done"])

            # Empty + finished selection
            empty_only = cleaner.select_tables_for_cleanup(empty=True)
            self.assertEqual([i["table"] for i in empty_only], ["job_empty"])

            finished = cleaner.select_tables_for_cleanup(finished=True)
            self.assertEqual(
                [i["table"] for i in finished],
                ["job_empty", "job_errored", "job_old_done"],
            )

            # Selecting all without --force skips job_active
            all_cands = cleaner.select_tables_for_cleanup(all_tables=True)
            dry_res = cleaner.drop_tables(all_cands, force=False, dry_run=True)
            self.assertTrue(dry_res["dry_run"])
            self.assertEqual(dry_res["skipped_active"], ["job_active"])
            self.assertEqual(
                dry_res["dropped"],
                ["job_empty", "job_errored", "job_old_done"],
            )

            # Drop completed & empty only
            to_drop = cleaner.select_tables_for_cleanup(completed=True, empty=True)
            drop_res = cleaner.drop_tables(to_drop, vacuum=True)
            self.assertEqual(drop_res["dropped"], ["job_empty", "job_old_done"])
            self.assertFalse(drop_res["vacuumed"])  # skipped gracefully on sqlite

            # Verify job_active, job_errored, and app_users remain intact
            verify_engine = sqlalchemy.create_engine(db_url)
            try:
                remaining = set(sqlalchemy.inspect(verify_engine).get_table_names())
                self.assertEqual(remaining, {"app_users", "job_active", "job_errored"})
            finally:
                verify_engine.dispose()

            # Dropping active table with force=True succeeds
            active_cands = cleaner.select_tables_for_cleanup(tables=["job_active"])
            force_res = cleaner.drop_tables(active_cands, force=True)
            self.assertEqual(force_res["dropped"], ["job_active"])

    def test_postgres_schemas_size_and_vacuum_mocked(self):
        cleaner = PBPTDatabaseCleaner(
            db_conn_str="postgresql+psycopg2://u:p@localhost:5432/db"
        )

        mock_engine = unittest.mock.MagicMock()
        mock_engine.dialect.name = "postgresql"
        mock_inspector = unittest.mock.MagicMock()
        mock_inspector.get_schema_names.return_value = [
            "information_schema",
            "pg_catalog",
            "pg_toast",
            "public",
            "eo_batch",
        ]
        # After dropping the table in eo_batch, schema is empty
        mock_inspector.get_table_names.return_value = []

        # _get_target_schemas filters out pg_*/information_schema
        schemas = cleaner._get_target_schemas(
            mock_engine, mock_inspector, all_schemas=True
        )
        self.assertEqual(schemas, ["public", "eo_batch"])

        # _format_table_report renders Size column when size_bytes is present
        sample_tables = [
            {
                "schema": "eo_batch",
                "table": "run1",
                "full_name": "eo_batch.run1",
                "total": 10,
                "pending": 0,
                "running": 0,
                "completed": 10,
                "checked": 10,
                "errors": 0,
                "state": "COMPLETED",
                "last_activity": "2026-11-19 10:00:00",
                "age_days": 1.0,
                "size_bytes": 65536,
            }
        ]
        txt = _format_table_report(sample_tables)
        self.assertIn("64.0 KB", txt)
        self.assertIn("eo_batch.run1", txt)

        # drop_tables with drop_empty_schemas=True and vacuum=True on PostgreSQL
        mock_conn = unittest.mock.MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_autocommit_conn = unittest.mock.MagicMock()
        mock_engine.connect.return_value.execution_options.return_value.__enter__.return_value = (
            mock_autocommit_conn
        )

        with unittest.mock.patch.object(cleaner, "_get_engine", return_value=mock_engine):
            with unittest.mock.patch("sqlalchemy.inspect", return_value=mock_inspector):
                with unittest.mock.patch("sqlalchemy.Table.drop") as mock_tbl_drop:
                    res = cleaner.drop_tables(
                        sample_tables,
                        drop_empty_schemas=True,
                        vacuum=True,
                    )

        mock_tbl_drop.assert_called_once_with(mock_engine, checkfirst=True)
        mock_conn.execute.assert_called_once()  # DropSchema("eo_batch", if_exists=True)
        mock_autocommit_conn.execute.assert_called_once()  # VACUUM ANALYZE
        self.assertEqual(res["dropped"], ["eo_batch.run1"])
        self.assertEqual(res["dropped_schemas"], ["eo_batch"])
        self.assertTrue(res["vacuumed"])

    def test_cli_script_and_interactive_modes(self):
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "bin", "pbpt_clean_db.py")
        )
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        env = dict(os.environ)
        env["PYTHONPATH"] = repo_root + (
            os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
        )

        help_proc = subprocess.run(
            [sys.executable, script_path, "--help"],
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn("--completed", help_proc.stdout)
        self.assertIn("--drop-empty-schemas", help_proc.stdout)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "cli_clean.db")
            db_url = f"sqlite:///{db_file}"
            conn_file = os.path.join(tmpdir, "conn.txt")
            pathlib.Path(conn_file).write_text(db_url)

            engine = sqlalchemy.create_engine(db_url)
            now = datetime.datetime.now()
            try:
                _create_queue_table_with_rows(
                    engine,
                    "t_done",
                    [
                        {
                            "JobParams": {"k": 1},
                            "Start": now,
                            "End": now,
                            "Started": True,
                            "Completed": True,
                            "Checked": True,
                            "Error": False,
                        }
                    ],
                )
                _create_queue_table_with_rows(engine, "t_empty", [])
                _create_queue_table_with_rows(
                    engine,
                    "t_running",
                    [
                        {
                            "JobParams": {"k": 2},
                            "Start": now,
                            "End": None,
                            "Started": True,
                            "Completed": False,
                            "Checked": False,
                            "Error": False,
                        }
                    ],
                )
            finally:
                engine.dispose()

            # Subprocess --list --json
            list_proc = subprocess.run(
                [sys.executable, script_path, "-d", conn_file, "--list", "--json"],
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                check=True,
            )
            listed = json.loads(list_proc.stdout)
            self.assertEqual([item["table"] for item in listed], ["t_done", "t_empty", "t_running"])

            # Non-interactive --drop without --yes or --dry-run fails cleanly
            fail_proc = subprocess.run(
                [sys.executable, script_path, "-d", conn_file, "--drop", "--completed"],
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(fail_proc.returncode, 0)
            self.assertIn("--yes", fail_proc.stderr)

            # Non-interactive --drop --completed --dry-run --json
            dry_proc = subprocess.run(
                [
                    sys.executable,
                    script_path,
                    "-d",
                    conn_file,
                    "--drop",
                    "--completed",
                    "--dry-run",
                    "--json",
                ],
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertEqual(json.loads(dry_proc.stdout)["dropped"], ["t_done"])

            # Subprocess --drop --completed --yes removes t_done
            drop_proc = subprocess.run(
                [
                    sys.executable,
                    script_path,
                    "-d",
                    conn_file,
                    "--drop",
                    "--completed",
                    "--yes",
                ],
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("Dropped 1 table(s): t_done", drop_proc.stdout)

            # In-process default --list text output and --drop validation errors
            list_buf = io.StringIO()
            with contextlib.redirect_stdout(list_buf):
                self.assertEqual(main(["-d", conn_file]), 0)
            self.assertIn("t_empty", list_buf.getvalue())

            with self.assertRaises(SystemExit):
                main(["-d", conn_file, "--drop"])

            # In-process --drop --completed when none match
            none_buf = io.StringIO()
            with contextlib.redirect_stdout(none_buf):
                self.assertEqual(main(["-d", conn_file, "--drop", "--completed", "-y"]), 0)
            self.assertIn("No matching pb_process_tools queue tables found to drop.", none_buf.getvalue())

            # In-process --drop --all --dry-run shows skipped ACTIVE and dropped t_empty
            dry_all_buf = io.StringIO()
            with contextlib.redirect_stdout(dry_all_buf):
                self.assertEqual(main(["-d", conn_file, "--drop", "--all", "--dry-run"]), 0)
            self.assertIn("Skipped ACTIVE table(s)", dry_all_buf.getvalue())
            self.assertIn("[DRY RUN] Would drop 1 table(s): t_empty", dry_all_buf.getvalue())

            # In-process --drop targeting only ACTIVE table without --force skips it
            act_buf = io.StringIO()
            with contextlib.redirect_stdout(act_buf):
                self.assertEqual(main(["-d", conn_file, "--drop", "-t", "t_running"]), 0)
            self.assertIn("All matching tables are ACTIVE and were skipped", act_buf.getvalue())

            # In-process interactive -i with 'q' (quit) then 'n' (declined), then 'y' (drop t_empty)
            quit_buf = io.StringIO()
            with contextlib.redirect_stdout(quit_buf):
                with unittest.mock.patch("builtins.input", return_value="q"):
                    self.assertEqual(main(["-d", conn_file, "-i"]), 0)
            self.assertIn("No tables selected for removal.", quit_buf.getvalue())

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                with unittest.mock.patch("builtins.input", return_value="y"):
                    rc = main(["-d", conn_file, "-i"])
            self.assertEqual(rc, 0)
            self.assertIn("Skipping active table 't_running'", buf.getvalue())
            self.assertIn("Dropped 1 table(s): t_empty", buf.getvalue())

            # Interactive -i when no matching tables exist
            empty_i_buf = io.StringIO()
            with contextlib.redirect_stdout(empty_i_buf):
                self.assertEqual(main(["-d", conn_file, "-i", "--completed"]), 0)
            self.assertIn("No matching pb_process_tools queue tables found.", empty_i_buf.getvalue())

            # Standalone --vacuum --dry-run and --vacuum --json on sqlite
            vac_buf = io.StringIO()
            with contextlib.redirect_stdout(vac_buf):
                self.assertEqual(main(["-d", conn_file, "--vacuum", "--dry-run"]), 0)
                self.assertEqual(main(["-d", conn_file, "--vacuum"]), 0)
                self.assertEqual(main(["-d", conn_file, "--vacuum", "--json"]), 0)
            self.assertIn("[DRY RUN] Would run VACUUM ANALYZE.", vac_buf.getvalue())
            self.assertIn("VACUUM ANALYZE skipped", vac_buf.getvalue())

            # Interactive TTY --drop abort then confirm on t_running with --force
            tty_buf = io.StringIO()
            with contextlib.redirect_stdout(tty_buf):
                with unittest.mock.patch("sys.stdin.isatty", return_value=True):
                    with unittest.mock.patch("builtins.input", return_value="n"):
                        rc_abort = main(["-d", conn_file, "--drop", "--all", "--force"])
                    with unittest.mock.patch("builtins.input", return_value="y"):
                        rc_yes = main(["-d", conn_file, "--drop", "--all", "--force"])
            self.assertEqual(rc_abort, 0)
            self.assertEqual(rc_yes, 0)
            self.assertIn("Aborted.", tty_buf.getvalue())
            self.assertIn("Dropped 1 table(s): t_running", tty_buf.getvalue())


if __name__ == "__main__":
    unittest.main()
