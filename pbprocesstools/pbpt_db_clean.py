#!/usr/bin/env python
"""
pb_process_tools - CLI and programmatic utility for inspecting and cleaning
up leftover PostgreSQL job queue tables created by pb_process_tools.
"""
# This file is part of 'pb_process_tools'
# A set of utilities for batch processing data
#
# Copyright 2023 Pete Bunting
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
# Purpose:  Tool for inspecting and removing leftover job queue tables.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 20/11/2026
# Version: 4.0.0

import argparse
import datetime
import fnmatch
import json
import logging
import os
import sys
from typing import Optional

import sqlalchemy

from pbprocesstools import configure_logging
from pbprocesstools.pbpt_q_process import _DBSessionMixin, _normalize_schema
from pbprocesstools.pbpt_utils import PBPTUtils

logger = logging.getLogger(__name__)

PBPT_QUEUE_COLUMNS = frozenset(
    {
        "PID",
        "JobParams",
        "Start",
        "End",
        "Started",
        "Completed",
        "Checked",
        "Error",
        "ErrorInfo",
    }
)

SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog", "pg_toast"})
PROTECTED_SCHEMAS = SYSTEM_SCHEMAS | frozenset({"public", "main"})


def resolve_db_conn_str(
    dbfile: Optional[str] = None, db_conn_str: Optional[str] = None
) -> str:
    """
    Resolve a database connection string from an explicit string, a connection
    file, or the PBPT_DB_CONN_STR environment variable.
    """
    if db_conn_str is not None:
        conn = db_conn_str.strip()
    elif dbfile is not None:
        pbpt_utils = PBPTUtils()
        conn = pbpt_utils.readTextFileNoNewLines(dbfile).strip()
    else:
        env_conn = os.getenv("PBPT_DB_CONN_STR")
        if not env_conn:
            raise ValueError(
                "A database connection string, --dbfile, or PBPT_DB_CONN_STR "
                "environment variable must be provided."
            )
        conn = env_conn.strip()

    if not conn:
        raise ValueError("The resolved database connection string is empty.")
    return conn


def is_pbpt_queue_table(
    inspector: sqlalchemy.Inspector,
    table_name: str,
    schema: Optional[str] = None,
) -> bool:
    """Return True if the inspected table contains all pb_process_tools queue columns."""
    cols = inspector.get_columns(table_name, schema=schema)
    col_names = {col["name"] for col in cols}
    return PBPT_QUEUE_COLUMNS.issubset(col_names)


def _make_queue_table_ref(
    table_name: str, schema: Optional[str] = None
) -> sqlalchemy.Table:
    """Create a detached SQLAlchemy Table object for querying/dropping a queue table."""
    meta = sqlalchemy.MetaData()
    return sqlalchemy.Table(
        table_name,
        meta,
        sqlalchemy.Column("PID", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("Start", sqlalchemy.DateTime),
        sqlalchemy.Column("End", sqlalchemy.DateTime),
        sqlalchemy.Column("Started", sqlalchemy.Boolean),
        sqlalchemy.Column("Completed", sqlalchemy.Boolean),
        sqlalchemy.Column("Checked", sqlalchemy.Boolean),
        sqlalchemy.Column("Error", sqlalchemy.Boolean),
        schema=schema,
    )


def _format_bytes(num_bytes: Optional[int]) -> str:
    if num_bytes is None:
        return "-"
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(size)} B"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


class PBPTDatabaseCleaner(_DBSessionMixin):
    """
    Inspect, filter, and clean up leftover pb_process_tools queue tables in
    a PostgreSQL (or compatible SQLAlchemy) database.
    """

    def __init__(
        self,
        db_conn_file: Optional[str] = None,
        db_conn_str: Optional[str] = None,
    ):
        self.db_conn_str = resolve_db_conn_str(
            dbfile=db_conn_file, db_conn_str=db_conn_str
        )
        self._init_db_state()

    def _get_conn_str(self) -> str:
        return self.db_conn_str

    def _pool_params(self) -> tuple[int, int]:
        return 1, 0

    def _get_target_schemas(
        self,
        engine: sqlalchemy.Engine,
        inspector: sqlalchemy.Inspector,
        schema: Optional[str] = None,
        all_schemas: bool = False,
    ) -> list[Optional[str]]:
        if all_schemas:
            raw_schemas = inspector.get_schema_names()
            if engine.dialect.name == "sqlite":
                return [None if s == "main" else s for s in raw_schemas]
            return [
                s
                for s in raw_schemas
                if (s not in SYSTEM_SCHEMAS) and (not s.startswith("pg_"))
            ]

        clean_schema = _normalize_schema(schema)
        if clean_schema is not None:
            return [clean_schema]
        return [None]

    def _matches_filters(
        self,
        table_name: str,
        schema: Optional[str],
        tables: Optional[list[str]] = None,
        pattern: Optional[str] = None,
    ) -> bool:
        full_name = f"{schema}.{table_name}" if schema else table_name
        if tables:
            matched = False
            for target in tables:
                target_clean = target.strip()
                if "." in target_clean:
                    if target_clean == full_name:
                        matched = True
                        break
                elif target_clean == table_name:
                    matched = True
                    break
            if not matched:
                return False

        if pattern:
            if not (
                fnmatch.fnmatch(table_name, pattern)
                or fnmatch.fnmatch(full_name, pattern)
            ):
                return False
        return True

    def list_queue_tables(
        self,
        schema: Optional[str] = None,
        all_schemas: bool = False,
        pattern: Optional[str] = None,
        tables: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Discover all pb_process_tools job queue tables in the target database
        and return summary statistics and status for each table.
        """
        results = []
        now = datetime.datetime.now()
        try:
            engine = self._get_engine()
            inspector = sqlalchemy.inspect(engine)
            target_schemas = self._get_target_schemas(
                engine, inspector, schema=schema, all_schemas=all_schemas
            )

            with self._session_scope() as ses:
                for sch in target_schemas:
                    try:
                        table_names = sorted(inspector.get_table_names(schema=sch))
                    except Exception:
                        continue

                    for tbl_name in table_names:
                        if not self._matches_filters(
                            tbl_name, sch, tables=tables, pattern=pattern
                        ):
                            continue
                        if not is_pbpt_queue_table(inspector, tbl_name, schema=sch):
                            continue

                        tbl_ref = _make_queue_table_ref(tbl_name, schema=sch)
                        stats_stmt = sqlalchemy.select(
                            sqlalchemy.func.count(tbl_ref.c.PID),
                            sqlalchemy.func.sum(
                                sqlalchemy.case(
                                    (
                                        sqlalchemy.and_(
                                            tbl_ref.c.Started.is_(False),
                                            tbl_ref.c.Completed.is_(False),
                                        ),
                                        1,
                                    ),
                                    else_=0,
                                )
                            ),
                            sqlalchemy.func.sum(
                                sqlalchemy.case(
                                    (
                                        sqlalchemy.and_(
                                            tbl_ref.c.Started.is_(True),
                                            tbl_ref.c.Completed.is_(False),
                                            tbl_ref.c.Error.is_(False),
                                        ),
                                        1,
                                    ),
                                    else_=0,
                                )
                            ),
                            sqlalchemy.func.sum(
                                sqlalchemy.case(
                                    (tbl_ref.c.Completed.is_(True), 1), else_=0
                                )
                            ),
                            sqlalchemy.func.sum(
                                sqlalchemy.case(
                                    (tbl_ref.c.Checked.is_(True), 1), else_=0
                                )
                            ),
                            sqlalchemy.func.sum(
                                sqlalchemy.case(
                                    (tbl_ref.c.Error.is_(True), 1), else_=0
                                )
                            ),
                            sqlalchemy.func.max(tbl_ref.c.Start),
                            sqlalchemy.func.max(tbl_ref.c.End),
                        )
                        (
                            total,
                            pending,
                            running,
                            completed,
                            checked,
                            errors,
                            max_start,
                            max_end,
                        ) = ses.execute(stats_stmt).one()

                        total = int(total or 0)
                        pending = int(pending or 0)
                        running = int(running or 0)
                        completed = int(completed or 0)
                        checked = int(checked or 0)
                        errors = int(errors or 0)

                        candidates = [t for t in (max_start, max_end) if t is not None]
                        last_activity = max(candidates) if candidates else None
                        age_days = (
                            (now - last_activity).total_seconds() / 86400.0
                            if last_activity is not None
                            else None
                        )

                        if total == 0:
                            state = "EMPTY"
                        elif pending > 0 or running > 0:
                            state = "ACTIVE"
                        elif errors > 0:
                            state = "FINISHED_WITH_ERRORS"
                        else:
                            state = "COMPLETED"

                        size_bytes = None
                        if engine.dialect.name == "postgresql":
                            try:
                                qual = (
                                    f'"{sch}"."{tbl_name}"'
                                    if sch
                                    else f'"{tbl_name}"'
                                )
                                size_bytes = ses.execute(
                                    sqlalchemy.text(
                                        "SELECT pg_total_relation_size(:relname)"
                                    ),
                                    {"relname": qual},
                                ).scalar()
                                if size_bytes is not None:
                                    size_bytes = int(size_bytes)
                            except Exception:
                                size_bytes = None

                        full_name = f"{sch}.{tbl_name}" if sch else tbl_name
                        results.append(
                            {
                                "schema": sch,
                                "table": tbl_name,
                                "full_name": full_name,
                                "total": total,
                                "pending": pending,
                                "running": running,
                                "completed": completed,
                                "checked": checked,
                                "errors": errors,
                                "state": state,
                                "last_activity": (
                                    last_activity.isoformat(sep=" ", timespec="seconds")
                                    if last_activity is not None
                                    else None
                                ),
                                "age_days": (
                                    round(age_days, 2)
                                    if age_days is not None
                                    else None
                                ),
                                "size_bytes": size_bytes,
                            }
                        )
        finally:
            self.dispose_engine()

        return results

    def select_tables_for_cleanup(
        self,
        schema: Optional[str] = None,
        all_schemas: bool = False,
        tables: Optional[list[str]] = None,
        pattern: Optional[str] = None,
        completed: bool = False,
        empty: bool = False,
        finished: bool = False,
        older_than_days: Optional[float] = None,
        all_tables: bool = False,
    ) -> list[dict]:
        """
        Select queue tables matching the given cleanup filters.
        """
        has_state_filter = any((completed, empty, finished))
        has_explicit_filter = any(
            (has_state_filter, tables, pattern, all_tables, older_than_days is not None)
        )
        if not has_explicit_filter:
            raise ValueError(
                "At least one table selection criterion must be specified "
                "(--tables, --pattern, --completed, --empty, --finished, "
                "--older-than, or --all)."
            )

        discovered = self.list_queue_tables(
            schema=schema,
            all_schemas=all_schemas,
            pattern=pattern,
            tables=tables,
        )

        selected = []
        for item in discovered:
            if has_state_filter:
                match_state = False
                if completed and item["state"] == "COMPLETED":
                    match_state = True
                if empty and item["state"] == "EMPTY":
                    match_state = True
                if finished and item["state"] in (
                    "COMPLETED",
                    "FINISHED_WITH_ERRORS",
                    "EMPTY",
                ):
                    match_state = True
                if not match_state:
                    continue

            if older_than_days is not None:
                if item["age_days"] is None or item["age_days"] < older_than_days:
                    continue

            selected.append(item)

        return selected

    def drop_tables(
        self,
        table_infos: list[dict],
        force: bool = False,
        dry_run: bool = False,
        drop_empty_schemas: bool = False,
        vacuum: bool = False,
    ) -> dict:
        """
        Drop the specified queue tables from the database.

        Active tables (with pending or running jobs) are skipped unless
        force=True is specified.
        """
        to_drop = []
        skipped_active = []
        for item in table_infos:
            if item["state"] == "ACTIVE" and not force:
                skipped_active.append(item)
            else:
                to_drop.append(item)

        if dry_run:
            return {
                "dry_run": True,
                "dropped": [item["full_name"] for item in to_drop],
                "skipped_active": [item["full_name"] for item in skipped_active],
                "dropped_schemas": [],
                "vacuumed": False,
            }

        dropped_names = []
        dropped_schemas = []
        touched_schemas = set()
        vacuumed = False

        try:
            engine = self._get_engine()
            for item in to_drop:
                tbl_ref = _make_queue_table_ref(item["table"], schema=item["schema"])
                tbl_ref.drop(engine, checkfirst=True)
                dropped_names.append(item["full_name"])
                logger.info("Dropped queue table '%s'.", item["full_name"])
                if item["schema"]:
                    touched_schemas.add(item["schema"])

            if drop_empty_schemas and touched_schemas and (
                engine.dialect.name == "postgresql"
            ):
                inspector = sqlalchemy.inspect(engine)
                for sch in sorted(touched_schemas):
                    if sch in PROTECTED_SCHEMAS or sch.startswith("pg_"):
                        continue
                    remaining = inspector.get_table_names(schema=sch)
                    if not remaining:
                        with engine.begin() as conn:
                            conn.execute(
                                sqlalchemy.schema.DropSchema(sch, if_exists=True)
                            )
                        dropped_schemas.append(sch)
                        logger.info("Dropped empty schema '%s'.", sch)

            if vacuum:
                vacuumed = self._run_vacuum(engine)
        finally:
            self.dispose_engine()

        return {
            "dry_run": False,
            "dropped": dropped_names,
            "skipped_active": [item["full_name"] for item in skipped_active],
            "dropped_schemas": dropped_schemas,
            "vacuumed": vacuumed,
        }

    def vacuum_database(self, dry_run: bool = False) -> bool:
        """Run VACUUM ANALYZE on PostgreSQL to reclaim space and refresh planner stats."""
        if dry_run:
            return False
        try:
            engine = self._get_engine()
            return self._run_vacuum(engine)
        finally:
            self.dispose_engine()

    @staticmethod
    def _run_vacuum(engine: sqlalchemy.Engine) -> bool:
        if engine.dialect.name != "postgresql":
            logger.info(
                "Skipping VACUUM ANALYZE for non-PostgreSQL dialect '%s'.",
                engine.dialect.name,
            )
            return False
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(sqlalchemy.text("VACUUM ANALYZE"))
        logger.info("Completed PostgreSQL VACUUM ANALYZE.")
        return True


def _format_table_report(tables: list[dict]) -> str:
    if not tables:
        return "No pb_process_tools job queue tables found."

    has_sizes = any(t["size_bytes"] is not None for t in tables)
    headers = [
        "Table",
        "State",
        "Total",
        "Pending",
        "Running",
        "Completed",
        "Errors",
        "Last Activity",
    ]
    if has_sizes:
        headers.append("Size")

    rows = []
    for item in tables:
        row = [
            item["full_name"],
            item["state"],
            str(item["total"]),
            str(item["pending"]),
            str(item["running"]),
            str(item["completed"]),
            str(item["errors"]),
            item["last_activity"] or "-",
        ]
        if has_sizes:
            row.append(_format_bytes(item["size_bytes"]))
        rows.append(row)

    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    fmt = "  ".join(
        f"{{:<{w}}}" if i in (0, 1, 7) else f"{{:>{w}}}" for i, w in enumerate(widths)
    )
    lines = [fmt.format(*headers), fmt.format(*("-" * w for w in widths))]
    for row in rows:
        lines.append(fmt.format(*row))
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pbpt_clean_db.py",
        description=(
            "Inspect and clean up leftover pb_process_tools job queue tables "
            "in a PostgreSQL database."
        ),
    )
    parser.add_argument(
        "-d",
        "--dbfile",
        type=str,
        default=None,
        help=(
            "Path to a file containing the PostgreSQL connection string "
            "(optional if --db-conn-str or PBPT_DB_CONN_STR is set)."
        ),
    )
    parser.add_argument(
        "--db-conn-str",
        type=str,
        default=None,
        help="Direct SQLAlchemy PostgreSQL connection string.",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="PostgreSQL schema name to inspect (default: default/public schema).",
    )
    parser.add_argument(
        "--all-schemas",
        action="store_true",
        default=False,
        help="Scan all non-system database schemas.",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        default=False,
        help="List discovered job queue tables and exit (default action).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output results as JSON.",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        default=False,
        help="Drop matching pb_process_tools job queue tables.",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        default=False,
        help="Interactively confirm each matching queue table to drop [y/N/q].",
    )
    parser.add_argument(
        "-t",
        "--tables",
        nargs="+",
        type=str,
        default=None,
        help="Specific queue table name(s) (or schema.table) to inspect or drop.",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=None,
        help="Glob pattern to filter queue table names (e.g., 'test_*').",
    )
    parser.add_argument(
        "--completed",
        action="store_true",
        default=False,
        help="Select queue tables where all jobs completed with zero errors.",
    )
    parser.add_argument(
        "--empty",
        action="store_true",
        default=False,
        help="Select empty queue tables (0 jobs).",
    )
    parser.add_argument(
        "--finished",
        action="store_true",
        default=False,
        help="Select idle/finished queue tables with no pending or running jobs.",
    )
    parser.add_argument(
        "--older-than",
        type=float,
        default=None,
        metavar="DAYS",
        help="Only select tables whose latest job activity is at least DAYS old.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Select all discovered pb_process_tools queue tables.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Allow dropping ACTIVE tables that still have pending or running jobs.",
    )
    parser.add_argument(
        "--drop-empty-schemas",
        action="store_true",
        default=False,
        help="Drop custom PostgreSQL schemas left empty after removing queue tables.",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        default=False,
        help="Run PostgreSQL VACUUM ANALYZE after cleanup (or standalone).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview which tables would be dropped without modifying the database.",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        default=False,
        help="Confirm non-interactive --drop without prompting.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    cleaner = PBPTDatabaseCleaner(
        db_conn_file=args.dbfile, db_conn_str=args.db_conn_str
    )

    if args.interactive:
        use_all = not any(
            (
                args.tables,
                args.pattern,
                args.completed,
                args.empty,
                args.finished,
                args.older_than is not None,
                args.all,
            )
        )
        candidates = cleaner.select_tables_for_cleanup(
            schema=args.schema,
            all_schemas=args.all_schemas,
            tables=args.tables,
            pattern=args.pattern,
            completed=args.completed,
            empty=args.empty,
            finished=args.finished,
            older_than_days=args.older_than,
            all_tables=args.all or use_all,
        )
        if not candidates:
            print("No matching pb_process_tools queue tables found.")
            if args.vacuum and not args.dry_run:
                cleaner.vacuum_database()
            return 0

        chosen = []
        for item in candidates:
            if item["state"] == "ACTIVE" and not args.force:
                print(
                    f"Skipping active table '{item['full_name']}' "
                    f"(pending={item['pending']}, running={item['running']}; "
                    f"use --force to allow dropping)."
                )
                continue
            resp = (
                input(
                    f"Drop '{item['full_name']}' "
                    f"[state={item['state']}, total={item['total']}, "
                    f"errors={item['errors']}, last={item['last_activity'] or '-'}]? "
                    "[y/N/q]: "
                )
                .strip()
                .lower()
            )
            if resp in ("q", "quit"):
                break
            if resp in ("y", "yes"):
                chosen.append(item)

        if not chosen:
            print("No tables selected for removal.")
            if args.vacuum and not args.dry_run:
                cleaner.vacuum_database()
            return 0

        summary = cleaner.drop_tables(
            chosen,
            force=args.force,
            dry_run=args.dry_run,
            drop_empty_schemas=args.drop_empty_schemas,
            vacuum=args.vacuum,
        )
        if args.json:
            print(json.dumps(summary, indent=2))
        elif args.dry_run:
            print("[DRY RUN] Would drop: " + ", ".join(summary["dropped"]))
        else:
            print(f"Dropped {len(summary['dropped'])} table(s): {', '.join(summary['dropped'])}")
            if summary["dropped_schemas"]:
                print(
                    "Dropped empty schema(s): "
                    + ", ".join(summary["dropped_schemas"])
                )
            if summary["vacuumed"]:
                print("Completed VACUUM ANALYZE.")
        return 0

    if args.drop:
        try:
            candidates = cleaner.select_tables_for_cleanup(
                schema=args.schema,
                all_schemas=args.all_schemas,
                tables=args.tables,
                pattern=args.pattern,
                completed=args.completed,
                empty=args.empty,
                finished=args.finished,
                older_than_days=args.older_than,
                all_tables=args.all,
            )
        except ValueError as err:
            parser.error(str(err))

        if not candidates:
            summary = {
                "dry_run": bool(args.dry_run),
                "dropped": [],
                "skipped_active": [],
                "dropped_schemas": [],
                "vacuumed": cleaner.vacuum_database(dry_run=args.dry_run)
                if args.vacuum
                else False,
            }
            if args.json:
                print(json.dumps(summary, indent=2))
            else:
                print("No matching pb_process_tools queue tables found to drop.")
            return 0

        if not args.dry_run and not args.yes:
            droppable = [
                c for c in candidates if (c["state"] != "ACTIVE" or args.force)
            ]
            if not droppable:
                skipped_names = ", ".join(c["full_name"] for c in candidates)
                print(
                    f"All matching tables are ACTIVE and were skipped (use --force): "
                    f"{skipped_names}"
                )
                return 0
            if sys.stdin.isatty():
                print(_format_table_report(droppable))
                resp = (
                    input(f"Drop these {len(droppable)} queue table(s)? [y/N]: ")
                    .strip()
                    .lower()
                )
                if resp not in ("y", "yes"):
                    print("Aborted.")
                    return 0
            else:
                parser.error("--drop requires --yes (-y) or --dry-run when non-interactive.")

        summary = cleaner.drop_tables(
            candidates,
            force=args.force,
            dry_run=args.dry_run,
            drop_empty_schemas=args.drop_empty_schemas,
            vacuum=args.vacuum,
        )
        if args.json:
            print(json.dumps(summary, indent=2))
        else:
            if summary["skipped_active"]:
                print(
                    "Skipped ACTIVE table(s) (use --force to drop): "
                    + ", ".join(summary["skipped_active"])
                )
            prefix = "[DRY RUN] Would drop" if args.dry_run else "Dropped"
            if summary["dropped"]:
                print(
                    f"{prefix} {len(summary['dropped'])} table(s): "
                    + ", ".join(summary["dropped"])
                )
            else:
                print("No tables dropped.")
            if summary["dropped_schemas"]:
                print(
                    "Dropped empty schema(s): "
                    + ", ".join(summary["dropped_schemas"])
                )
            if summary["vacuumed"]:
                print("Completed VACUUM ANALYZE.")
        return 0

    if args.vacuum and not args.list:
        vacuumed = cleaner.vacuum_database(dry_run=args.dry_run)
        if args.json:
            print(json.dumps({"vacuumed": vacuumed, "dry_run": bool(args.dry_run)}, indent=2))
        else:
            if args.dry_run:
                print("[DRY RUN] Would run VACUUM ANALYZE.")
            elif vacuumed:
                print("Completed VACUUM ANALYZE.")
            else:
                print("VACUUM ANALYZE skipped (PostgreSQL only).")
        return 0

    # Default / --list mode
    tables = cleaner.list_queue_tables(
        schema=args.schema,
        all_schemas=args.all_schemas,
        pattern=args.pattern,
        tables=args.tables,
    )
    if args.json:
        print(json.dumps(tables, indent=2))
    else:
        print(_format_table_report(tables))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
