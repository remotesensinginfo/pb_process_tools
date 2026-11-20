#!/usr/bin/env python
"""
pb_process_tools - this file has classes for batch processing data using a queue
"""
# This file is part of 'pb_process_tools'
# A set of utilities for batch processing data
#
# Copyright 2018 Pete Bunting
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Purpose:  Tool for batch processing data using a PostgreSQL queue.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 13/05/2020
# Version: 4.0.0

import argparse
import concurrent.futures
import datetime
import importlib
import json
import logging
import os
import pathlib
import shlex
import statistics
import sys
import threading
import time
import traceback
from abc import abstractmethod
from contextlib import contextmanager
from typing import Any, Optional

import sqlalchemy
import tqdm
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from pbprocesstools import configure_logging, py_sys_version_flt
from pbprocesstools.pbpt_process import PBPTProcessToolsBase
from pbprocesstools.pbpt_utils import PBPTUtils

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


def _sanitize_json(value: Any) -> Any:
    """Ensure a Python value is JSON-serializable (converting unsupported types via str)."""
    if value is None:
        return None
    return json.loads(json.dumps(value, default=str))


def _validate_table_name(table_name: Optional[str]) -> str:
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError(
            "A non-empty db_table_name must be specified for each task so "
            "multiple tasks do not share the same database table."
        )
    return table_name.strip()


def _normalize_schema(schema: Optional[str]) -> Optional[str]:
    if schema is None:
        return None
    cleaned = str(schema).strip()
    return cleaned if cleaned else None


_JOB_MODEL_CACHE: dict[tuple[str, Optional[str]], type] = {}


def get_job_model(table_name: str, schema: Optional[str] = None):
    """
    Return a SQLAlchemy ORM model mapped to the specified table/schema, with
    PostgreSQL JSONB columns and partial indexes on queue status columns.
    """
    table_name = _validate_table_name(table_name)
    schema = _normalize_schema(schema)

    cache_key = (table_name, schema)
    if cache_key in _JOB_MODEL_CACHE:
        return _JOB_MODEL_CACHE[cache_key]

    safe_table_prefix = "".join(
        c if (c.isalnum() or c == "_") else "_" for c in table_name
    )
    if schema:
        safe_schema_prefix = "".join(
            c if (c.isalnum() or c == "_") else "_" for c in schema
        )
        safe_idx_prefix = f"{safe_schema_prefix}_{safe_table_prefix}"
    else:
        safe_idx_prefix = safe_table_prefix

    pid_col = sqlalchemy.Column(
        "PID", sqlalchemy.Integer, primary_key=True, autoincrement=True
    )
    job_params_col = sqlalchemy.Column(
        "JobParams",
        sqlalchemy.JSON().with_variant(JSONB, "postgresql"),
        nullable=True,
    )
    start_col = sqlalchemy.Column("Start", sqlalchemy.DateTime, nullable=True)
    end_col = sqlalchemy.Column("End", sqlalchemy.DateTime, nullable=True)
    started_col = sqlalchemy.Column(
        "Started", sqlalchemy.Boolean, nullable=False, default=False
    )
    completed_col = sqlalchemy.Column(
        "Completed", sqlalchemy.Boolean, nullable=False, default=False
    )
    checked_col = sqlalchemy.Column(
        "Checked", sqlalchemy.Boolean, nullable=False, default=False
    )
    error_col = sqlalchemy.Column(
        "Error", sqlalchemy.Boolean, nullable=False, default=False
    )
    error_info_col = sqlalchemy.Column(
        "ErrorInfo",
        sqlalchemy.JSON().with_variant(JSONB, "postgresql"),
        nullable=True,
    )

    indexes = (
        sqlalchemy.Index(
            f"ix_{safe_idx_prefix}_pending",
            pid_col,
            postgresql_where=sqlalchemy.and_(
                completed_col.is_(False), started_col.is_(False)
            ),
        ),
        sqlalchemy.Index(
            f"ix_{safe_idx_prefix}_unchecked",
            pid_col,
            postgresql_where=checked_col.is_(False),
        ),
        sqlalchemy.Index(
            f"ix_{safe_idx_prefix}_incomplete",
            pid_col,
            postgresql_where=sqlalchemy.and_(
                started_col.is_(True), completed_col.is_(False)
            ),
        ),
    )

    table_args = indexes + ({"schema": schema},) if schema else indexes

    cls_name = f"PBPTProcessJob_{safe_idx_prefix}_{schema or 'default'}"

    model_cls = type(
        cls_name,
        (Base,),
        {
            "__tablename__": table_name,
            "__table_args__": table_args,
            "PID": pid_col,
            "JobParams": job_params_col,
            "Start": start_col,
            "End": end_col,
            "Started": started_col,
            "Completed": completed_col,
            "Checked": checked_col,
            "Error": error_col,
            "ErrorInfo": error_info_col,
        },
    )
    _JOB_MODEL_CACHE[cache_key] = model_cls
    return model_cls


def _create_db_engine(
    db_conn_str: str, pool_size: int = 1, max_overflow: int = 0
) -> sqlalchemy.Engine:
    """Create a SQLAlchemy engine tuned for PostgreSQL (or compatible URL)."""
    url = sqlalchemy.engine.make_url(db_conn_str)
    if url.get_backend_name() == "sqlite":
        return sqlalchemy.create_engine(db_conn_str, pool_pre_ping=True)
    return sqlalchemy.create_engine(
        db_conn_str,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
    )


class _DBSessionMixin:
    """Shared engine & session lifecycle management to avoid leaking pools/connections."""

    def _init_db_state(self):
        self._db_engine: Optional[sqlalchemy.Engine] = None
        self._engine_conn_str: Optional[str] = None
        self._session_factory: Optional[sessionmaker] = None

    def _get_conn_str(self) -> str:
        raise NotImplementedError

    def _pool_params(self) -> tuple[int, int]:
        return 1, 0

    def _get_engine(self) -> sqlalchemy.Engine:
        conn_str = self._get_conn_str()
        if (self._db_engine is None) or (self._engine_conn_str != conn_str):
            self.dispose_engine()
            pool_size, max_overflow = self._pool_params()
            logger.debug("Creating Database Engine.")
            self._db_engine = _create_db_engine(
                conn_str, pool_size=pool_size, max_overflow=max_overflow
            )
            self._engine_conn_str = conn_str
            self._session_factory = sessionmaker(bind=self._db_engine)
        return self._db_engine

    def dispose_engine(self):
        """Dispose of the cached SQLAlchemy engine and release pooled connections."""
        if self._db_engine is not None:
            self._db_engine.dispose()
            self._db_engine = None
            self._engine_conn_str = None
            self._session_factory = None
            logger.debug("Disposed Database Engine.")

    @contextmanager
    def _session_scope(self):
        self._get_engine()
        ses = self._session_factory()
        try:
            yield ses
        except Exception:
            ses.rollback()
            raise
        finally:
            ses.close()


class PBPTQProcessTool(_DBSessionMixin, PBPTProcessToolsBase):
    """
    An abstract class for aiding the quick creation
    of data processing scripts which take their
    parameters from a PostgreSQL job queue.
    """

    def __init__(
        self,
        queue_db_info=None,
        cmd_name=None,
        descript=None,
        params=None,
        uid_len=6,
        db_table_name=None,
        db_schema=None,
    ):
        """
        A class to implement a processing tool for batch processing data analysis
        using a PostgreSQL queue.

        :param queue_db_info: The database dict info. Requires fields: db_conn_str
                              and db_table_name (and optionally db_schema).
        :param cmd_name: optionally provide the name of the command (i.e., the
                         python script name).
        :param descript: optionally provide a description of the command file.
        :param params: optionally provide a dict which will be the options for
                       the processing to execute (e.g., the input and output files).
        :param uid_len: length of unique ID string.
        :param db_table_name: PostgreSQL table name for this task's job queue.
        :param db_schema: Optional PostgreSQL schema name for the job queue table.
        """
        self.queue_db_info = queue_db_info
        self.cmd_name = cmd_name
        self.descript = descript
        self.params = params
        self.job_pid = None
        self.debug_job_id = None
        self.debug_job_id_params = False
        self.debug_job_id_rmouts = False
        self.db_table_name = (
            _validate_table_name(db_table_name)
            if db_table_name is not None
            else None
        )
        self.db_schema = _normalize_schema(db_schema)
        self._init_db_state()
        super().__init__(uid_len)

    def _get_conn_str(self) -> str:
        if self.queue_db_info and ("db_conn_str" in self.queue_db_info):
            return self.queue_db_info["db_conn_str"]
        env_conn = os.getenv("PBPT_DB_CONN_STR")
        if env_conn:
            return env_conn
        raise ValueError("db_conn_str is not configured.")

    def _get_table_and_schema(self) -> tuple[str, Optional[str]]:
        table_name = self.db_table_name
        schema = self.db_schema
        if self.queue_db_info:
            if self.queue_db_info.get("db_table_name") is not None:
                table_name = self.queue_db_info["db_table_name"]
            if "db_schema" in self.queue_db_info:
                schema = self.queue_db_info["db_schema"]
        if table_name is None:
            raise ValueError(
                "db_table_name must be specified in queue_db_info or PBPTQProcessTool constructor."
            )
        return _validate_table_name(table_name), _normalize_schema(schema)

    def _get_job_model(self):
        table_name, schema = self._get_table_and_schema()
        return get_job_model(table_name=table_name, schema=schema)

    def set_params(self, params):
        """
        A function to set the parameters used for the analysis without setting them
        within the constructor or via a command line JSON file.

        :param params: provide a dict which will be the options for the processing to
                       execute (e.g., the input and output files).
        """
        self.params = params

    def set_queue_db_info(self, queue_db_info):
        """
        Set the queue database info.

        :param queue_db_info: The database dict info. Requires fields: db_conn_str and db_table_name.
        """
        self.queue_db_info = queue_db_info
        self.dispose_engine()

    def check_db_info(self):
        """
        Test whether the required database info is present and connectivity works.
        """
        if self.queue_db_info is None:
            self.queue_db_info = {}

        if ("db_conn_str" not in self.queue_db_info) or (
            not self.queue_db_info["db_conn_str"]
        ):
            env_conn = os.getenv("PBPT_DB_CONN_STR")
            if env_conn:
                self.queue_db_info["db_conn_str"] = env_conn
            else:
                raise ValueError("db_conn_str key is not present or is empty.")

        if self.queue_db_info.get("sqlite_db_file") is not None:
            raise ValueError(
                "SQLite support has been removed in v4.0.0; provide a PostgreSQL db_conn_str."
            )

        table_name, schema = self._get_table_and_schema()
        self.queue_db_info["db_table_name"] = table_name
        if schema is not None:
            self.queue_db_info["db_schema"] = schema

        try:
            with self._session_scope() as ses:
                ses.execute(sqlalchemy.text("SELECT 1"))
            logger.debug("Verified database connection.")
        except Exception as e:
            raise ConnectionError(
                "The database could not be connected to: '{}'".format(
                    self.queue_db_info["db_conn_str"]
                )
            ) from e

        return True

    @abstractmethod
    def do_processing(self, **kwargs):
        """
        An abstract function to undertake processing.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.g., obj.do_processing(option_a=True, option_b=100)).
        """
        pass

    def completed_processing(self, **kwargs):
        """
        Mark the current job as completed in the database and optionally touch
        'out_cmp_file' if present in self.params.
        """
        JobModel = self._get_job_model()
        with self._session_scope() as ses:
            stmt = (
                sqlalchemy.update(JobModel)
                .where(JobModel.PID == self.job_pid)
                .values(
                    Completed=True,
                    Error=False,
                    End=datetime.datetime.now(),
                )
            )
            ses.execute(stmt)
            ses.commit()

        if self.params and ("out_cmp_file" in self.params):
            pathlib.Path(self.params["out_cmp_file"]).touch()

    def record_process_error(self, err_info, **kwargs):
        """
        Record an error in the database for the current job. Usually called
        instead of completed_processing.

        :param err_info: a dict with information on the error to be stored in the
                         database.
        """
        JobModel = self._get_job_model()
        safe_err_info = _sanitize_json(err_info)
        with self._session_scope() as ses:
            stmt = (
                sqlalchemy.update(JobModel)
                .where(JobModel.PID == self.job_pid)
                .values(
                    Completed=False,
                    Error=True,
                    ErrorInfo=safe_err_info,
                    End=None,
                )
            )
            ses.execute(stmt)
            ses.commit()

    def _claim_next_job(self) -> Optional[tuple[int, Any]]:
        """Atomically claim the next pending job using FOR UPDATE SKIP LOCKED."""
        JobModel = self._get_job_model()
        with self._session_scope() as ses:
            stmt = (
                sqlalchemy.select(JobModel)
                .where(
                    JobModel.Completed.is_(False),
                    JobModel.Started.is_(False),
                )
                .order_by(JobModel.PID.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job_info = ses.execute(stmt).scalar_one_or_none()
            if job_info is None:
                return None

            job_info.Started = True
            job_info.Start = datetime.datetime.now()
            pid = job_info.PID
            params = job_info.JobParams
            ses.commit()
            return pid, params

    @abstractmethod
    def required_fields(self, **kwargs):
        """
        An abstract function which is required to return a list of strings. The list
        are dict keys which are required by the function.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.g., obj.required_fields(mod_version=True)).
        :return: list of strings
        """
        pass

    @abstractmethod
    def outputs_present(self, **kwargs):
        """
        An abstract function which is required to return a tuple (boolean, dict with
        outputs as keys and error message as the value).

        :return: tuple (boolean, dict with outputs as keys and error message as value)
        """
        pass

    @abstractmethod
    def remove_outputs(self, **kwargs):
        """
        An abstract function which checks if outputs are present and removes them.
        """
        pass

    def check_required_fields(self, **kwargs):
        """
        Check that the required fields are present within the self.params dict.
        """
        if self.params is None:
            raise ValueError("Job params is None; cannot validate required fields.")
        fields = self.required_fields(**kwargs)
        err_fields = [field for field in fields if field not in self.params]
        if err_fields:
            if len(err_fields) > 1:
                fields_str = ", ".join(err_fields)
                raise KeyError(
                    "The following fields '{}' should have been provided "
                    "but were not within the params dict.".format(fields_str)
                )
            raise KeyError(
                "The field '{}' should have been provided but was not "
                "within the params dict.".format(err_fields[0])
            )
        return True

    def parse_cmds(self, argv=None, **kwargs):
        """
        Parse the command line arguments to retrieve the database info and debug flags.
        """
        try:
            parser = argparse.ArgumentParser(
                prog=self.cmd_name, description=self.descript
            )
            parser.add_argument(
                "-d",
                "--dbinfo",
                type=str,
                required=True,
                help="Specify a file path for a JSON file "
                "containing the database info",
            )
            parser.add_argument(
                "-j",
                "--job",
                type=int,
                required=False,
                help="Specify a job ID for the job to be "
                "executed. This is a debug tool and "
                "therefore the database will not be "
                "updated and the job will be run "
                "regardless of the database status.",
            )
            parser.add_argument(
                "-p",
                "--params",
                action="store_true",
                default=False,
                help="If a job is specified then rather than running the parameters "
                "will be printed to the console.",
            )
            parser.add_argument(
                "-r",
                "--rmouts",
                action="store_true",
                default=False,
                help="If a job is specified then rather than running the job "
                "the outputs will be removed.",
            )
            if argv is None:
                argv = sys.argv[1:]
            args = parser.parse_args(argv)
            self.debug_job_id = args.job
            self.debug_job_id_params = args.params
            self.debug_job_id_rmouts = args.rmouts
            with open(args.dbinfo) as f:
                self.queue_db_info = json.load(f)
            self.check_db_info()
        except Exception:
            traceback.print_exception(*sys.exc_info())
            return False
        return True

    def std_run(self, max_db_retries: int = 5, **kwargs):
        """
        Run the standard processing sequence: parse CLI args, claim jobs atomically
        from PostgreSQL via FOR UPDATE SKIP LOCKED, execute do_processing(), and
        record completion or errors.
        """
        configure_logging()
        logger.debug("Starting to execute the 'std_run' function.")
        if not self.parse_cmds(**kwargs):
            return

        try:
            if self.debug_job_id is None:
                db_conn_str = self._get_conn_str()
                logger.debug("Database connection info: '{}'.".format(db_conn_str))
                consecutive_db_errors = 0

                while True:
                    try:
                        claimed = self._claim_next_job()
                        consecutive_db_errors = 0
                    except Exception as e:
                        consecutive_db_errors += 1
                        logger.exception(
                            "Database error while claiming job (attempt %d/%d): %s",
                            consecutive_db_errors,
                            max_db_retries,
                            e,
                        )
                        if consecutive_db_errors >= max_db_retries:
                            raise
                        time.sleep(min(2 ** consecutive_db_errors, 30))
                        continue

                    if claimed is None:
                        logger.debug("No job found to process - finishing.")
                        break

                    self.job_pid, self.params = claimed
                    logger.debug(
                        "Found the next scene to process. PID: {}".format(self.job_pid)
                    )

                    try:
                        self.check_required_fields(**kwargs)
                        self.do_processing(**kwargs)
                    except Exception as e:
                        err_dict = {
                            "error": str(e),
                            "traceback": traceback.format_exc(),
                        }
                        try:
                            self.record_process_error(err_dict)
                        except Exception as db_err:
                            logger.exception(
                                "Failed to record process error for PID %s: %s",
                                self.job_pid,
                                db_err,
                            )
                        continue

                    try:
                        self.completed_processing(**kwargs)
                    except Exception as db_err:
                        logger.exception(
                            "Job PID %s finished processing but failed to update completion status: %s",
                            self.job_pid,
                            db_err,
                        )
            else:
                JobModel = self._get_job_model()
                with self._session_scope() as ses:
                    logger.debug(
                        "Searching the database for job ID: '{}'".format(
                            self.debug_job_id
                        )
                    )
                    job_info = (
                        ses.query(JobModel)
                        .filter(JobModel.PID == self.debug_job_id)
                        .one_or_none()
                    )
                    if job_info is None:
                        raise Exception(
                            f"No job was found; does the specified job ({self.debug_job_id}) exist?"
                        )
                    self.job_pid = self.debug_job_id
                    self.params = job_info.JobParams

                logger.debug("Found the job to process, PID: {}".format(self.job_pid))
                if self.debug_job_id_params:
                    import pprint

                    pprint.pprint(self.params)
                elif self.debug_job_id_rmouts:
                    self.check_required_fields(**kwargs)
                    self.remove_outputs(**kwargs)
                else:
                    self.check_required_fields(**kwargs)
                    self.do_processing(**kwargs)

                logger.debug(
                    "Finished processing the job, PID: {}".format(self.job_pid)
                )
        finally:
            self.dispose_engine()


class PBPTGenQProcessToolCmds(_DBSessionMixin, PBPTProcessToolsBase):
    def __init__(
        self,
        cmd,
        sqlite_db_file=None,
        db_conn_file=None,
        lock_file_path=None,
        uid_len=6,
        process_tools_path=None,
        process_tools_mod=None,
        process_tools_cls=None,
        db_conn_str=None,
        db_table_name=None,
        db_schema=None,
    ):
        """
        A class to implement the generation of commands and management of a
        PostgreSQL batch processing queue.

        :param cmd: the command to be executed (e.g., python run_analysis.py).
        :param sqlite_db_file: Deprecated/unsupported in v4.0.0. Must be None.
        :param db_conn_file: Path to text file containing PostgreSQL connection string.
        :param lock_file_path: Deprecated and ignored (PostgreSQL uses row-level locking).
        :param uid_len: Length of UID string.
        :param process_tools_path: Path containing the PBPTQProcessTool implementation.
        :param process_tools_mod: Module name containing the PBPTQProcessTool implementation.
        :param process_tools_cls: Class name implementing PBPTQProcessTool.
        :param db_conn_str: Direct PostgreSQL SQLAlchemy connection string (optional alternative to db_conn_file).
        :param db_table_name: PostgreSQL table name for this task's job queue (required).
        :param db_schema: Optional PostgreSQL schema name for the job queue table.
        """
        super().__init__(uid_len)
        self._init_db_state()

        if sqlite_db_file is not None:
            raise ValueError(
                "SQLite support has been removed in v4.0.0; please provide db_conn_file or db_conn_str for PostgreSQL."
            )

        if lock_file_path is not None:
            logger.debug(
                "lock_file_path is deprecated and ignored; PostgreSQL row locking is used instead."
            )

        pbpt_utils = PBPTUtils()
        self.params = []
        self.cmd = cmd
        self.db_table_name = _validate_table_name(db_table_name)
        self.db_schema = _normalize_schema(db_schema)
        self.append_db = False
        self.deduplicate_db = True
        self.dry_run = False

        if db_conn_str is not None:
            self.db_conn_str = db_conn_str.strip()
        elif db_conn_file is not None:
            self.db_conn_str = pbpt_utils.readTextFileNoNewLines(db_conn_file).strip()
        elif os.getenv("PBPT_DB_CONN_STR"):
            self.db_conn_str = os.getenv("PBPT_DB_CONN_STR").strip()
        else:
            raise ValueError("A database connection string or file must be specified.")

        if not self.db_conn_str:
            raise ValueError("The database connection string is empty.")

        self.process_tools_path = process_tools_path
        self.process_tools_mod = process_tools_mod
        self.process_tools_cls = process_tools_cls

    def _get_conn_str(self) -> str:
        return self.db_conn_str

    def _pool_params(self) -> tuple[int, int]:
        return 2, 2

    def _get_job_model(self):
        return get_job_model(table_name=self.db_table_name, schema=self.db_schema)

    def _build_queue_db_info(self) -> dict:
        db_info = {
            "db_conn_str": self.db_conn_str,
            "db_table_name": self.db_table_name,
        }
        if self.db_schema is not None:
            db_info["db_schema"] = self.db_schema
        return db_info

    def _write_db_info_file(self, db_info_file=None) -> str:
        pbpt_utils = PBPTUtils()
        if db_info_file is None:
            db_info_file = os.path.abspath("process_db_info_{}.json".format(self.uid))
        else:
            db_info_file = os.path.abspath(db_info_file)

        pbpt_utils.writeDict2JSON(self._build_queue_db_info(), db_info_file)
        try:
            os.chmod(db_info_file, 0o600)
        except OSError:
            pass
        return db_info_file

    def _load_process_tool_instance(
        self, process_tools_path=None, process_tools_mod=None, process_tools_cls=None
    ):
        if process_tools_path is None:
            process_tools_path = self.process_tools_path

        if process_tools_mod is None:
            if self.process_tools_mod is None:
                raise ValueError(
                    "A PBPTQProcessTool implementation module has "
                    "not been provided to the constructor."
                )
            process_tools_mod = self.process_tools_mod

        if process_tools_cls is None:
            if self.process_tools_cls is None:
                raise ValueError(
                    "A PBPTQProcessTool implementation class has not "
                    "been provided to the constructor."
                )
            process_tools_cls = self.process_tools_cls

        if process_tools_path is not None:
            abs_path = os.path.abspath(process_tools_path)
            if abs_path not in sys.path:
                sys.path.insert(0, abs_path)

        process_tools_mod_inst = importlib.import_module(process_tools_mod)
        if process_tools_mod_inst is None:
            raise ImportError("Could not load the module: '{}'".format(process_tools_mod))

        process_tools_cls_inst = getattr(process_tools_mod_inst, process_tools_cls)()
        if process_tools_cls_inst is None:
            raise RuntimeError(
                "Could not create instance of '{}'".format(process_tools_cls)
            )
        process_tools_cls_inst.set_queue_db_info(self._build_queue_db_info())
        return process_tools_cls_inst

    @abstractmethod
    def gen_command_info(self, **kwargs):
        """
        An abstract function to create the list of dicts for the individual commands,
        populating self.params.
        """
        pass

    def check_job_outputs(
        self,
        out_err_pid_file=None,
        out_err_info_file=None,
        process_tools_path=None,
        process_tools_mod=None,
        process_tools_cls=None,
        batch_size=500,
        n_workers=1,
        **kwargs,
    ):
        """
        Check whether all outputs for unchecked jobs were created, recording any
        errors in batches.

        :param n_workers: Number of worker threads for parallel output file checks (default: 1).
        """
        if n_workers < 1:
            raise ValueError("n_workers must be >= 1.")

        JobModel = self._get_job_model()
        process_tools_cls_inst = self._load_process_tool_instance(
            process_tools_path=process_tools_path,
            process_tools_mod=process_tools_mod,
            process_tools_cls=process_tools_cls,
        )

        pbpt_utils = PBPTUtils()
        err_pids = []
        err_info = dict()

        try:
            with self._session_scope() as ses:
                jobs = (
                    ses.execute(
                        sqlalchemy.select(
                            JobModel.PID,
                            JobModel.JobParams,
                            JobModel.Completed,
                            JobModel.Started,
                        )
                        .where(JobModel.Checked.is_(False))
                        .order_by(JobModel.PID.asc())
                    )
                    .all()
                )

                checked_pids = []
                err_rows = []
                incomplete_pids = []

                def _flush_batch():
                    if checked_pids:
                        ses.execute(
                            sqlalchemy.update(JobModel)
                            .where(JobModel.PID.in_(checked_pids))
                            .values(Checked=True)
                        )
                        checked_pids.clear()
                    if incomplete_pids:
                        ses.execute(
                            sqlalchemy.update(JobModel)
                            .where(JobModel.PID.in_(incomplete_pids))
                            .values(
                                Started=False,
                                Error=True,
                                ErrorInfo={"error": "Started but did not complete."},
                            )
                        )
                        incomplete_pids.clear()
                    for err_pid, err_dict in err_rows:
                        ses.execute(
                            sqlalchemy.update(JobModel)
                            .where(JobModel.PID == err_pid)
                            .values(
                                Completed=False,
                                Error=True,
                                ErrorInfo=_sanitize_json(err_dict),
                            )
                        )
                    err_rows.clear()
                    ses.commit()

                completed_jobs = []
                for pid, job_params, completed, started in jobs:
                    if completed:
                        completed_jobs.append((pid, job_params))
                    else:
                        err_pids.append(pid)
                        if started:
                            incomplete_pids.append(pid)
                            err_info[pid] = "Started but did not complete."
                        else:
                            err_info[pid] = "Never Started."

                        if len(incomplete_pids) >= batch_size:
                            _flush_batch()

                if n_workers == 1:
                    for pid, job_params in tqdm.tqdm(completed_jobs):
                        process_tools_cls_inst.set_params(job_params)
                        files_present, errs_dict = (
                            process_tools_cls_inst.outputs_present(**kwargs)
                        )
                        if not files_present:
                            err_pids.append(pid)
                            err_info[pid] = errs_dict
                            err_rows.append((pid, errs_dict))
                        else:
                            checked_pids.append(pid)

                        if (len(checked_pids) + len(err_rows)) >= batch_size:
                            _flush_batch()
                else:
                    tool_cls = type(process_tools_cls_inst)
                    queue_db_info = self._build_queue_db_info()
                    thread_local = threading.local()

                    def _check_one(pid_and_params):
                        c_pid, c_params = pid_and_params
                        inst = getattr(thread_local, "inst", None)
                        if inst is None:
                            inst = tool_cls()
                            inst.set_queue_db_info(queue_db_info)
                            thread_local.inst = inst
                        inst.set_params(c_params)
                        files_present, errs_dict = inst.outputs_present(**kwargs)
                        return c_pid, files_present, errs_dict

                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=n_workers
                    ) as executor:
                        futures = [
                            executor.submit(_check_one, item)
                            for item in completed_jobs
                        ]
                        for fut in tqdm.tqdm(
                            concurrent.futures.as_completed(futures),
                            total=len(futures),
                        ):
                            c_pid, files_present, errs_dict = fut.result()
                            if not files_present:
                                err_pids.append(c_pid)
                                err_info[c_pid] = errs_dict
                                err_rows.append((c_pid, errs_dict))
                            else:
                                checked_pids.append(c_pid)

                            if (len(checked_pids) + len(err_rows)) >= batch_size:
                                _flush_batch()

                _flush_batch()
        finally:
            self.dispose_engine()

        err_pids.sort()
        if len(err_pids) > 0:
            if out_err_pid_file is not None:
                pbpt_utils.writeList2File(err_pids, out_err_pid_file)
            if out_err_info_file is not None:
                pbpt_utils.writeDict2JSON(err_info, out_err_info_file)
            print("{} errors".format(len(err_pids)))
        else:
            if out_err_pid_file is not None:
                pathlib.Path(out_err_pid_file).touch()
            if out_err_info_file is not None:
                pbpt_utils.writeDict2JSON({}, out_err_info_file)
            print("Checks complete and all jobs passed.")

    def remove_job_outputs(
        self,
        all_jobs=False,
        error_jobs=False,
        pids=None,
        process_tools_path=None,
        process_tools_mod=None,
        process_tools_cls=None,
        batch_size=500,
        **kwargs,
    ):
        """
        Remove outputs and reset job states in the database for all jobs,
        error/incomplete jobs, or specific PIDs.
        """
        if (not all_jobs) and (not error_jobs) and (not pids):
            raise ValueError(
                "Must specify all_jobs, error_jobs, or pids "
                "for outputs to be removed."
            )

        JobModel = self._get_job_model()
        process_tools_cls_inst = self._load_process_tool_instance(
            process_tools_path=process_tools_path,
            process_tools_mod=process_tools_mod,
            process_tools_cls=process_tools_cls,
        )

        try:
            with self._session_scope() as ses:
                stmt = sqlalchemy.select(JobModel.PID, JobModel.JobParams).order_by(
                    JobModel.PID.asc()
                )
                if not all_jobs:
                    filters = []
                    if error_jobs:
                        filters.append(
                            sqlalchemy.or_(
                                JobModel.Error.is_(True),
                                sqlalchemy.and_(
                                    JobModel.Started.is_(True),
                                    JobModel.Completed.is_(False),
                                ),
                            )
                        )
                    if pids:
                        filters.append(JobModel.PID.in_(list(pids)))
                    stmt = stmt.where(sqlalchemy.or_(*filters))

                jobs = ses.execute(stmt).all()
                reset_pids = []

                def _flush_reset():
                    if reset_pids:
                        ses.execute(
                            sqlalchemy.update(JobModel)
                            .where(JobModel.PID.in_(reset_pids))
                            .values(
                                Started=False,
                                Error=False,
                                Completed=False,
                                Checked=False,
                                ErrorInfo=None,
                                Start=None,
                                End=None,
                            )
                        )
                        ses.commit()
                        reset_pids.clear()

                for pid, job_params in tqdm.tqdm(jobs):
                    process_tools_cls_inst.set_params(job_params)
                    process_tools_cls_inst.remove_outputs(**kwargs)
                    reset_pids.append(pid)
                    if len(reset_pids) >= batch_size:
                        _flush_reset()

                _flush_reset()
        finally:
            self.dispose_engine()

    def reset_jobs(
        self,
        all_jobs=False,
        error_jobs=False,
        pids=None,
    ) -> int:
        """
        Reset job states in the database without deleting any output files.

        :param all_jobs: Reset all jobs in the queue.
        :param error_jobs: Reset jobs that errored or started without completing.
        :param pids: Optional iterable of specific job PIDs to reset.
        :return: Number of jobs reset.
        """
        if (not all_jobs) and (not error_jobs) and (not pids):
            raise ValueError(
                "Must specify all_jobs, error_jobs, or pids to reset jobs."
            )

        JobModel = self._get_job_model()
        try:
            with self._session_scope() as ses:
                stmt = sqlalchemy.update(JobModel).values(
                    Started=False,
                    Error=False,
                    Completed=False,
                    Checked=False,
                    ErrorInfo=None,
                    Start=None,
                    End=None,
                )
                if not all_jobs:
                    filters = []
                    if error_jobs:
                        filters.append(
                            sqlalchemy.or_(
                                JobModel.Error.is_(True),
                                sqlalchemy.and_(
                                    JobModel.Started.is_(True),
                                    JobModel.Completed.is_(False),
                                ),
                            )
                        )
                    if pids:
                        filters.append(JobModel.PID.in_(list(pids)))
                    stmt = stmt.where(sqlalchemy.or_(*filters))

                result = ses.execute(stmt)
                ses.commit()
                n_reset = int(result.rowcount or 0)
                logger.info("Reset %d job(s) in database.", n_reset)
                return n_reset
        finally:
            self.dispose_engine()

    def create_jobs_report(self, out_report_file=None):
        """
        Generate a JSON report of job counts, timing statistics, and errors using
        SQL aggregation and minimal column projection.
        """
        JobModel = self._get_job_model()

        try:
            with self._session_scope() as ses:
                counts_stmt = sqlalchemy.select(
                    sqlalchemy.func.count(JobModel.PID).label("total"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case((JobModel.Completed.is_(True), 1), else_=0)
                    ).label("n_completed"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case((JobModel.Started.is_(True), 1), else_=0)
                    ).label("n_started"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case(
                            (
                                sqlalchemy.and_(
                                    JobModel.Completed.is_(False),
                                    JobModel.Error.is_(True),
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("n_errs"),
                )
                counts = ses.execute(counts_stmt).one()
                n_jobs = int(counts.total or 0)
                n_completed = int(counts.n_completed or 0)
                n_started = int(counts.n_started or 0)
                n_errs = int(counts.n_errs or 0)
                n_ended = n_completed

                job_times = []
                if n_completed > 0:
                    times_rows = ses.execute(
                        sqlalchemy.select(JobModel.Start, JobModel.End).where(
                            JobModel.Completed.is_(True),
                            JobModel.Start.is_not(None),
                            JobModel.End.is_not(None),
                        )
                    ).all()
                    job_times = [
                        (end_t - start_t).total_seconds()
                        for start_t, end_t in times_rows
                    ]

                err_info = dict()
                status_info = dict()
                problem_rows = ses.execute(
                    sqlalchemy.select(
                        JobModel.PID,
                        JobModel.Error,
                        JobModel.ErrorInfo,
                        JobModel.Started,
                    ).where(
                        JobModel.Completed.is_(False),
                        sqlalchemy.or_(
                            JobModel.Error.is_(True),
                            JobModel.Started.is_(True),
                        ),
                    )
                ).all()
                for pid, is_err, err_val, is_started in problem_rows:
                    if is_err:
                        err_info[pid] = {"info": err_val}
                    if is_started:
                        status_info[pid] = "Started but did not complete."
        finally:
            self.dispose_engine()

        out_info_dict = dict()
        if n_jobs > 0:
            out_info_dict["job_n"] = {
                "n_completed": n_completed,
                "n_errs": n_errs,
                "n_started": n_started,
                "n_ended": n_ended,
                "total_n_jobs": n_jobs,
            }

            if len(job_times) > 0:
                job_times_dict = {
                    "time_mean_secs": statistics.mean(job_times),
                    "time_min_secs": min(job_times),
                    "time_max_secs": max(job_times),
                    "time_median_secs": statistics.median(job_times),
                }
                if len(job_times) > 1:
                    job_times_dict["time_stdev_secs"] = statistics.stdev(job_times)
                    if py_sys_version_flt >= 3.8:
                        job_times_dict["time_quartiles_secs"] = statistics.quantiles(
                            job_times
                        )
                out_info_dict["job_times"] = job_times_dict

            out_info_dict["status"] = status_info
            out_info_dict["xerrors"] = err_info

        if out_report_file is None:
            import pprint

            pprint.pprint(out_info_dict)
        else:
            pbpt_utils = PBPTUtils()
            pbpt_utils.writeDict2JSON(out_info_dict, out_report_file)

    @staticmethod
    def _format_duration(seconds: float) -> str:
        secs = max(0, int(round(seconds)))
        days, rem = divmod(secs, 86400)
        hours, rem = divmod(rem, 3600)
        mins, secs = divmod(rem, 60)
        if days > 0:
            return f"{days}d {hours:02d}:{mins:02d}:{secs:02d}"
        return f"{hours:02d}:{mins:02d}:{secs:02d}"

    def get_jobs_status(self, _dispose: bool = True) -> dict:
        """
        Return a compact queue status summary dictionary using SQL aggregation.
        """
        JobModel = self._get_job_model()
        try:
            with self._session_scope() as ses:
                stmt = sqlalchemy.select(
                    sqlalchemy.func.count(JobModel.PID).label("total"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case((JobModel.Completed.is_(True), 1), else_=0)
                    ).label("completed"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case((JobModel.Checked.is_(True), 1), else_=0)
                    ).label("checked"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case(
                            (
                                sqlalchemy.and_(
                                    JobModel.Started.is_(True),
                                    JobModel.Completed.is_(False),
                                    JobModel.Error.is_(False),
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("running"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case((JobModel.Error.is_(True), 1), else_=0)
                    ).label("errors"),
                    sqlalchemy.func.sum(
                        sqlalchemy.case(
                            (
                                sqlalchemy.and_(
                                    JobModel.Started.is_(False),
                                    JobModel.Completed.is_(False),
                                    JobModel.Error.is_(False),
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("pending"),
                    sqlalchemy.func.min(JobModel.Start).label("first_start"),
                    sqlalchemy.func.max(JobModel.End).label("last_end"),
                )
                row = ses.execute(stmt).one()
        finally:
            if _dispose:
                self.dispose_engine()

        total = int(row.total or 0)
        completed = int(row.completed or 0)
        checked = int(row.checked or 0)
        running = int(row.running or 0)
        errors = int(row.errors or 0)
        pending = int(row.pending or 0)
        pct_complete = (completed / total * 100.0) if total > 0 else 0.0

        throughput_per_hour = None
        eta_seconds = None
        if completed > 0 and row.first_start is not None:
            end_ref = (
                row.last_end
                if (pending == 0 and running == 0 and row.last_end is not None)
                else datetime.datetime.now()
            )
            elapsed = (end_ref - row.first_start).total_seconds()
            if elapsed > 0:
                rate_per_sec = completed / elapsed
                throughput_per_hour = rate_per_sec * 3600.0
                remaining = pending + running
                if remaining > 0 and rate_per_sec > 0:
                    eta_seconds = remaining / rate_per_sec

        return {
            "total": total,
            "pending": pending,
            "running": running,
            "completed": completed,
            "checked": checked,
            "errors": errors,
            "pct_complete": pct_complete,
            "throughput_per_hour": throughput_per_hour,
            "eta_seconds": eta_seconds,
        }

    def print_jobs_status(self, watch: bool = False, interval_secs: float = 5.0):
        """
        Print a live summary of the job queue, optionally polling until complete.
        """
        if interval_secs <= 0:
            raise ValueError("interval_secs must be > 0.")

        try:
            while True:
                status = self.get_jobs_status(_dispose=False)
                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rate_str = (
                    f"{status['throughput_per_hour']:.1f}/hr"
                    if status["throughput_per_hour"] is not None
                    else "N/A"
                )
                eta_str = (
                    self._format_duration(status["eta_seconds"])
                    if status["eta_seconds"] is not None
                    else ("00:00:00" if status["pending"] == 0 and status["running"] == 0 else "N/A")
                )
                print(
                    f"[{ts}] Total: {status['total']} | Pending: {status['pending']} | "
                    f"Running: {status['running']} | Completed: {status['completed']} "
                    f"({status['pct_complete']:.1f}%, Checked: {status['checked']}) | "
                    f"Errors: {status['errors']} | Rate: {rate_str} | ETA: {eta_str}",
                    flush=True,
                )
                if not watch:
                    break
                if status["total"] == 0 or (
                    status["pending"] == 0 and status["running"] == 0
                ):
                    break
                time.sleep(interval_secs)
        except KeyboardInterrupt:
            print("\nStopped watching queue status.")
        finally:
            self.dispose_engine()

    def inspect_job(self, pid: int, print_output: bool = True) -> dict:
        """
        Inspect a single job row (status, timestamps, parameters, and error info) by PID.
        """
        JobModel = self._get_job_model()
        try:
            with self._session_scope() as ses:
                job_info = (
                    ses.execute(
                        sqlalchemy.select(JobModel).where(JobModel.PID == pid)
                    )
                    .scalar_one_or_none()
                )
                if job_info is None:
                    raise LookupError(
                        "No job found with PID {} in table '{}'.".format(
                            pid, self.db_table_name
                        )
                    )
                details = {
                    "PID": job_info.PID,
                    "Started": job_info.Started,
                    "Completed": job_info.Completed,
                    "Checked": job_info.Checked,
                    "Error": job_info.Error,
                    "Start": job_info.Start.isoformat() if job_info.Start else None,
                    "End": job_info.End.isoformat() if job_info.End else None,
                    "JobParams": job_info.JobParams,
                    "ErrorInfo": job_info.ErrorInfo,
                }
        finally:
            self.dispose_engine()

        if print_output:
            print(json.dumps(details, indent=2, default=str))
        return details

    def get_job_errors(
        self, limit: Optional[int] = None, print_output: bool = True
    ) -> list[dict]:
        """
        Retrieve and optionally print errored and incomplete jobs from the queue.
        """
        JobModel = self._get_job_model()
        try:
            with self._session_scope() as ses:
                stmt = (
                    sqlalchemy.select(
                        JobModel.PID,
                        JobModel.Started,
                        JobModel.Completed,
                        JobModel.Error,
                        JobModel.ErrorInfo,
                    )
                    .where(
                        sqlalchemy.or_(
                            JobModel.Error.is_(True),
                            sqlalchemy.and_(
                                JobModel.Started.is_(True),
                                JobModel.Completed.is_(False),
                            ),
                        )
                    )
                    .order_by(JobModel.PID.asc())
                )
                if limit is not None:
                    stmt = stmt.limit(limit)
                rows = ses.execute(stmt).all()
        finally:
            self.dispose_engine()

        err_list = [
            {
                "PID": pid,
                "Started": started,
                "Completed": completed,
                "Error": is_err,
                "ErrorInfo": err_val
                if err_val is not None
                else "Started but did not complete.",
            }
            for pid, started, completed, is_err, err_val in rows
        ]

        if print_output:
            if not err_list:
                print("No errors or incomplete jobs found.")
            else:
                for item in err_list:
                    print(f"=== PID {item['PID']} (Error={item['Error']}, Started={item['Started']}) ===")
                    err_val = item["ErrorInfo"]
                    if isinstance(err_val, dict):
                        if "error" in err_val:
                            print(f"Error: {err_val['error']}")
                        if "traceback" in err_val:
                            print(err_val["traceback"].rstrip())
                        if "error" not in err_val and "traceback" not in err_val:
                            print(json.dumps(err_val, indent=2, default=str))
                    else:
                        print(err_val)
        return err_list

    def drop_params_db(self):
        """
        Drop the job queue table from the database if it exists.
        """
        JobModel = self._get_job_model()
        try:
            db_engine = self._get_engine()
            JobModel.__table__.drop(db_engine, checkfirst=True)
            logger.info("Dropped table '%s'.", self.db_table_name)
        finally:
            self.dispose_engine()

    def _ensure_schema_and_table(self, db_engine, drop_existing: bool = False):
        JobModel = self._get_job_model()
        if (self.db_schema is not None) and (db_engine.dialect.name == "postgresql"):
            with db_engine.begin() as conn:
                conn.execute(
                    sqlalchemy.schema.CreateSchema(
                        self.db_schema, if_not_exists=True
                    )
                )

        if drop_existing:
            logger.debug("Drop queue table if within the existing database.")
            JobModel.__table__.drop(db_engine, checkfirst=True)
            logger.debug("Creating queue table.")
            JobModel.__table__.create(db_engine)
        else:
            logger.debug("Ensuring queue table exists.")
            JobModel.__table__.create(db_engine, checkfirst=True)

    def append_params_db(self, batch_size=5000, deduplicate=True) -> int:
        """
        Create the job queue table if it does not exist and append jobs from
        self.params without deleting existing queue entries.

        :param batch_size: Number of rows per bulk insert chunk.
        :param deduplicate: If True, skip params already present in the database
                            or duplicated within self.params.
        :return: Number of newly inserted jobs.
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would append up to {len(self.params)} job(s) to "
                f"table '{self.db_table_name}' (deduplicate={deduplicate})."
            )
            if self.params:
                print(f"[DRY RUN] Sample job[0]: {self.params[0]}")
            return 0

        JobModel = self._get_job_model()
        try:
            db_engine = self._get_engine()
            self._ensure_schema_and_table(db_engine, drop_existing=False)

            params_to_insert = [_sanitize_json(p) for p in self.params]
            if deduplicate and params_to_insert:
                with self._session_scope() as ses:
                    existing_rows = ses.execute(
                        sqlalchemy.select(JobModel.JobParams)
                    ).scalars()
                    seen = {
                        json.dumps(row, sort_keys=True)
                        for row in existing_rows
                        if row is not None
                    }
                filtered = []
                for p in params_to_insert:
                    key = json.dumps(p, sort_keys=True)
                    if key not in seen:
                        seen.add(key)
                        filtered.append(p)
                params_to_insert = filtered

            logger.info(
                "Appending {} new jobs (out of {} total) to the database.".format(
                    len(params_to_insert), len(self.params)
                )
            )
            if len(params_to_insert) > 0:
                with self._session_scope() as ses:
                    for start_idx in range(0, len(params_to_insert), batch_size):
                        chunk = params_to_insert[start_idx : start_idx + batch_size]
                        rows = [{"JobParams": p} for p in chunk]
                        ses.execute(sqlalchemy.insert(JobModel), rows)
                    ses.commit()
            logger.info("Finished appending jobs to the database.")
            return len(params_to_insert)
        finally:
            self.dispose_engine()

    def pop_params_db(self, batch_size=5000, append=None, deduplicate=None):
        """
        Populate the job queue table with self.params.

        By default (append=False), drops and recreates the queue table before
        bulk-inserting all parameter dicts. When append=True (or via --gen --append),
        creates the table if missing and inserts only new jobs.
        """
        if append is None:
            append = self.append_db
        if deduplicate is None:
            deduplicate = self.deduplicate_db

        if append:
            return self.append_params_db(
                batch_size=batch_size, deduplicate=deduplicate
            )

        if self.dry_run:
            print(
                f"[DRY RUN] Would recreate table '{self.db_table_name}' and "
                f"insert {len(self.params)} job(s)."
            )
            if self.params:
                print(f"[DRY RUN] Sample job[0]: {self.params[0]}")
            return 0

        JobModel = self._get_job_model()
        try:
            db_engine = self._get_engine()
            self._ensure_schema_and_table(db_engine, drop_existing=True)

            logger.info(
                "There are {} jobs to be written to the database.".format(
                    len(self.params)
                )
            )
            if len(self.params) > 0:
                with self._session_scope() as ses:
                    for start_idx in range(0, len(self.params), batch_size):
                        chunk = self.params[start_idx : start_idx + batch_size]
                        rows = [{"JobParams": _sanitize_json(p)} for p in chunk]
                        ses.execute(sqlalchemy.insert(JobModel), rows)
                    ses.commit()
            logger.info("Written jobs to the database.")
            return len(self.params)
        finally:
            self.dispose_engine()

    def create_shell_exe(self, run_script, cmds_sh_file, n_cores, db_info_file=None):
        """
        Generate the scripts to execute on a local machine using GNU parallel.
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would create shell scripts '{run_script}' and "
                f"'{cmds_sh_file}' for {n_cores} core(s)."
            )
            return

        logger.info("Create database info file.")
        pbpt_utils = PBPTUtils()
        db_info_file = self._write_db_info_file(db_info_file)

        logger.info("Create commands shell scripts.")
        cmd_line = "{} --dbinfo {}".format(self.cmd, shlex.quote(db_info_file))
        lst_cmds = [cmd_line for _ in range(n_cores)]
        pbpt_utils.writeList2File(lst_cmds, cmds_sh_file)

        parallel_cmd = "parallel -j {} < {}".format(
            n_cores, shlex.quote(cmds_sh_file)
        )
        pbpt_utils.writeData2File(parallel_cmd, run_script)
        logger.info("Finished creating shell scripts.")

    def create_slurm_sub_sh(
        self,
        jobname,
        mem_per_core_mb,
        log_dir,
        run_script="run_exe_analysis.sh",
        job_dir="job_scripts",
        db_info_file=None,
        account_name=None,
        n_cores_per_job=10,
        n_xtr_cmds=0,
        n_jobs=10,
        job_time_limit="2-23:59",
        module_load="module load parallel singularity",
        partition="compute",
        use_array=False,
        max_concurrent=None,
        mem_total_mb=None,
        qos=None,
        constraint=None,
        gres=None,
        extra_sbatch_args=None,
    ):
        """
        Generate the scripts needed to run an analysis using Slurm.

        :param use_array: If True, generate a single Slurm job array script
                          (#SBATCH --array=0-(n_jobs-1)[%max_concurrent]) instead of
                          n_jobs separate sbatch files.
        :param max_concurrent: Optional concurrency limit for Slurm job arrays (%N).
        :param mem_total_mb: Optional total memory per Slurm job (--mem) instead of
                             --mem-per-cpu.
        :param qos: Optional Slurm QOS (#SBATCH --qos=...).
        :param constraint: Optional Slurm node constraint (#SBATCH --constraint=...).
        :param gres: Optional Slurm generic resources (#SBATCH --gres=...).
        :param extra_sbatch_args: Optional iterable of extra '#SBATCH ...' strings.
        """
        if n_jobs < 1:
            raise ValueError("n_jobs must be >= 1.")

        if self.dry_run:
            mode_desc = (
                f"array 0-{n_jobs - 1}"
                + (f"%{max_concurrent}" if max_concurrent else "")
                if use_array
                else f"{n_jobs} sbatch job(s)"
            )
            print(
                f"[DRY RUN] Would create Slurm scripts in '{job_dir}' and "
                f"'{run_script}' ({mode_desc}, partition={partition})."
            )
            return

        logger.info("Create database info file.")
        pbpt_utils = PBPTUtils()
        db_info_file = self._write_db_info_file(db_info_file)

        logger.info("Create commands slurm scripts.")
        job_dir = os.path.abspath(job_dir)
        os.makedirs(job_dir, exist_ok=True)

        cmd_line = "{} --dbinfo {}".format(self.cmd, shlex.quote(db_info_file))
        lst_cmds = [cmd_line for _ in range(n_cores_per_job + n_xtr_cmds)]
        cmds_sh_file = os.path.join(job_dir, "jobcmds.sh")
        pbpt_utils.writeList2File(lst_cmds, cmds_sh_file)

        log_dir = os.path.abspath(log_dir)
        os.makedirs(log_dir, exist_ok=True)

        jobname = pbpt_utils.check_str(
            jobname, rm_non_ascii=True, rm_dashs=True, rm_spaces=True, rm_punc=True
        )

        def _write_common_sbatch_headers(sbatch_file_obj, s_jobname, out_log, err_log):
            sbatch_file_obj.write("#!/bin/bash --login\n\n")
            if account_name is not None:
                sbatch_file_obj.write("#SBATCH --account={}\n".format(account_name))
            if partition is not None:
                sbatch_file_obj.write("#SBATCH --partition={}\n".format(partition))
            if qos is not None:
                sbatch_file_obj.write("#SBATCH --qos={}\n".format(qos))
            if constraint is not None:
                sbatch_file_obj.write("#SBATCH --constraint={}\n".format(constraint))
            if gres is not None:
                sbatch_file_obj.write("#SBATCH --gres={}\n".format(gres))
            sbatch_file_obj.write("#SBATCH --job-name={}\n".format(s_jobname))
            sbatch_file_obj.write("#SBATCH --output={}\n".format(out_log))
            sbatch_file_obj.write("#SBATCH --error={}\n".format(err_log))
            sbatch_file_obj.write("#SBATCH --time={}\n".format(job_time_limit))
            sbatch_file_obj.write("#SBATCH --ntasks={}\n".format(n_cores_per_job))
            if mem_total_mb is not None:
                sbatch_file_obj.write("#SBATCH --mem={}\n".format(mem_total_mb))
            elif mem_per_core_mb is not None:
                sbatch_file_obj.write(
                    "#SBATCH --mem-per-cpu={}\n".format(mem_per_core_mb)
                )
            if extra_sbatch_args:
                for extra_arg in extra_sbatch_args:
                    line = extra_arg.strip()
                    if not line.startswith("#SBATCH"):
                        line = f"#SBATCH {line}"
                    sbatch_file_obj.write(f"{line}\n")

        sbatch_cmds = list()
        if use_array:
            array_spec = f"0-{n_jobs - 1}"
            if max_concurrent is not None:
                array_spec = f"{array_spec}%{max_concurrent}"
            sbatch_file = os.path.join(job_dir, "job_array.sbatch")
            out_log = os.path.join(log_dir, "{}_%A_%a_log.out".format(jobname))
            err_log = os.path.join(log_dir, "{}_%A_%a_log.err".format(jobname))
            with open(sbatch_file, "w") as sbatch_file_obj:
                _write_common_sbatch_headers(
                    sbatch_file_obj, jobname, out_log, err_log
                )
                sbatch_file_obj.write("#SBATCH --array={}\n".format(array_spec))
                if module_load is not None:
                    sbatch_file_obj.write("\n{}\n".format(module_load))
                sbatch_file_obj.write(
                    "\nparallel -N 1 --delay .2 -j $SLURM_NTASKS < {}\n\n".format(
                        shlex.quote(cmds_sh_file)
                    )
                )
            sbatch_cmds.append("sbatch {}".format(shlex.quote(sbatch_file)))
        else:
            for n in range(n_jobs):
                sbatch_file = os.path.join(job_dir, "job_file_{}.sbatch".format(n))
                c_jobname = "{}_{}".format(jobname, n)
                out_log = os.path.join(log_dir, "{}_log.out.%J".format(c_jobname))
                err_log = os.path.join(log_dir, "{}_log.err.%J".format(c_jobname))
                with open(sbatch_file, "w") as sbatch_file_obj:
                    _write_common_sbatch_headers(
                        sbatch_file_obj, c_jobname, out_log, err_log
                    )
                    if module_load is not None:
                        sbatch_file_obj.write("\n{}\n".format(module_load))
                    sbatch_file_obj.write(
                        "\nparallel -N 1 --delay .2 -j $SLURM_NTASKS < {}\n\n".format(
                            shlex.quote(cmds_sh_file)
                        )
                    )
                    sbatch_cmds.append("sbatch {}".format(shlex.quote(sbatch_file)))

        pbpt_utils.writeList2File(sbatch_cmds, run_script)
        logger.info("Finished creating slurm scripts.")

    @abstractmethod
    def run_gen_commands(self):
        """
        An abstract function which needs to be implemented with the functions and inputs
        you want run to generate the various commands and scripts to be executed.
        """
        pass

    def run_check_outputs(self, out_files=False, n_workers=1, **kwargs):
        """
        Run output checks (--check).
        """
        if out_files:
            time_sample_str = self.generate_readable_timestamp_str()
            out_err_pid_file = "processing_errs_scns_{}.txt".format(time_sample_str)
            out_err_info_file = "non_complete_errs_{}.json".format(time_sample_str)
        else:
            out_err_pid_file = None
            out_err_info_file = None
        self.check_job_outputs(
            out_err_pid_file, out_err_info_file, n_workers=n_workers, **kwargs
        )

    def run_remove_outputs(self, all_jobs=False, error_jobs=False, pids=None, **kwargs):
        """
        Remove output files and reset jobs (--rmouts).
        """
        self.remove_job_outputs(
            all_jobs=all_jobs, error_jobs=error_jobs, pids=pids, **kwargs
        )

    def run_reset_jobs(self, all_jobs=False, error_jobs=False, pids=None):
        """
        Reset database job states without deleting output files (--reset).
        """
        return self.reset_jobs(all_jobs=all_jobs, error_jobs=error_jobs, pids=pids)

    def parse_cmds(self, argv=None):
        """
        Parse command line arguments (--gen, --append, --check, --report, --status,
        --watch, --inspect, --errors, --rmouts, --reset, --drop).
        """
        configure_logging()
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--gen",
            action="store_true",
            default=False,
            help="Execute run_gen_commands() function.",
        )
        parser.add_argument(
            "--append",
            action="store_true",
            default=False,
            help="Append new jobs without dropping the existing table (can be used with or without --gen).",
        )
        parser.add_argument(
            "--no-dedup",
            action="store_true",
            default=False,
            help="When using --append, insert all generated jobs without checking for duplicates.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="With --gen or --append, preview job generation without modifying the database or writing scripts.",
        )
        parser.add_argument(
            "--check",
            action="store_true",
            default=False,
            help="Execute run_check_outputs() function.",
        )
        parser.add_argument(
            "-w",
            "--workers",
            type=int,
            default=1,
            help="Number of worker threads for parallel output checking with --check (default: 1).",
        )
        parser.add_argument(
            "--report",
            action="store_true",
            default=False,
            help="Execute create_jobs_report() function.",
        )
        parser.add_argument(
            "--status",
            action="store_true",
            default=False,
            help="Print a concise live summary of queue counts, throughput, and ETA.",
        )
        parser.add_argument(
            "--watch",
            action="store_true",
            default=False,
            help="Poll and print queue status continuously until all jobs finish.",
        )
        parser.add_argument(
            "--interval",
            type=float,
            default=5.0,
            help="Polling interval in seconds for --watch (default: 5.0).",
        )
        parser.add_argument(
            "--inspect",
            type=int,
            default=None,
            metavar="PID",
            help="Inspect status, parameters, and error details for a single job PID.",
        )
        parser.add_argument(
            "--errors",
            action="store_true",
            default=False,
            help="Print errored and incomplete jobs from the queue.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Optional maximum number of errors to print with --errors.",
        )
        parser.add_argument(
            "--rmouts",
            action="store_true",
            default=False,
            help="Execute run_remove_outputs() function (deletes outputs and resets DB state).",
        )
        parser.add_argument(
            "--reset",
            action="store_true",
            default=False,
            help="Reset DB job state without deleting output files (requires --all, --error, or --pids).",
        )
        parser.add_argument(
            "--drop",
            action="store_true",
            default=False,
            help="Drop the queue table from the database (use --yes to skip confirmation).",
        )
        parser.add_argument(
            "-y",
            "--yes",
            action="store_true",
            default=False,
            help="Confirm destructive operations such as --drop without prompting.",
        )
        parser.add_argument(
            "--all",
            action="store_true",
            default=False,
            help="Apply --rmouts or --reset to all jobs.",
        )
        parser.add_argument(
            "--error",
            action="store_true",
            default=False,
            help="Apply --rmouts or --reset to jobs with errors or incomplete runs.",
        )
        parser.add_argument(
            "--pids",
            nargs="+",
            type=int,
            default=None,
            help="Apply --rmouts or --reset to specific job PID(s).",
        )
        parser.add_argument(
            "-o",
            "--output",
            type=str,
            required=False,
            help="Specify a report output JSON file. If not provided "
            "then report written to console.",
        )
        parser.add_argument(
            "--chkoutfiles",
            action="store_true",
            default=False,
            help="Produce output files when run --check for user information.",
        )
        parser.add_argument(
            "--table",
            type=str,
            default=None,
            help="Override the PostgreSQL table name for this task's job queue.",
        )
        parser.add_argument(
            "--schema",
            type=str,
            default=None,
            help="Override the PostgreSQL schema name for this task's job queue table.",
        )
        if argv is None:
            argv = sys.argv[1:]
        args = parser.parse_args(argv)

        if args.table is not None:
            self.db_table_name = _validate_table_name(args.table)
        if args.schema is not None:
            self.db_schema = _normalize_schema(args.schema)

        if args.gen or args.append:
            self.append_db = bool(args.append)
            self.deduplicate_db = not bool(args.no_dedup)
            self.dry_run = bool(args.dry_run)
            try:
                self.run_gen_commands()
            finally:
                self.dry_run = False
        elif args.check:
            self.run_check_outputs(
                out_files=args.chkoutfiles, n_workers=args.workers
            )
        elif args.report:
            self.create_jobs_report(args.output)
        elif args.status or args.watch:
            self.print_jobs_status(
                watch=bool(args.watch), interval_secs=args.interval
            )
        elif args.inspect is not None:
            self.inspect_job(args.inspect, print_output=True)
        elif args.errors:
            self.get_job_errors(limit=args.limit, print_output=True)
        elif args.rmouts:
            if (not args.all) and (not args.error) and (not args.pids):
                parser.error("--rmouts requires --all, --error, or --pids.")
            self.run_remove_outputs(
                all_jobs=args.all, error_jobs=args.error, pids=args.pids
            )
        elif args.reset:
            if (not args.all) and (not args.error) and (not args.pids):
                parser.error("--reset requires --all, --error, or --pids.")
            n_reset = self.run_reset_jobs(
                all_jobs=args.all, error_jobs=args.error, pids=args.pids
            )
            print("Reset {} job(s) in database.".format(n_reset))
        elif args.drop:
            if not args.yes:
                if sys.stdin.isatty():
                    resp = input(
                        f"Drop queue table '{self.db_table_name}'? [y/N]: "
                    ).strip().lower()
                    if resp not in ("y", "yes"):
                        print("Aborted.")
                        return
                else:
                    parser.error("--drop requires --yes (-y) when run non-interactively.")
            self.drop_params_db()
            print("Dropped queue table '{}'.".format(self.db_table_name))
        else:
            parser.print_help()
