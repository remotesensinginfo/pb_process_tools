#!/usr/bin/env python
"""
pb_process_tools - this file has classes for batch processing data using a queue

See other source files for details
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
# Purpose:  Tool for batch processing data using a queue.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 13/05/2020
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import json
import sys
import pathlib
import os
import logging
import datetime
import tqdm
from abc import abstractmethod

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy import event
import sqlalchemy
from sqlalchemy.orm.attributes import flag_modified
from sqlite3 import Connection as SQLite3Connection

from pbprocesstools.pbpt_utils import PBPTUtils
from pbprocesstools.pbpt_process import PBPTProcessToolsBase
from pbprocesstools import py_sys_version_flt

logger = logging.getLogger(__name__)

Base = declarative_base()

class PBPTProcessJob(Base):
    __tablename__ = "PBPTProcessJob"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    JobParams = sqlalchemy.Column(sqlalchemy.JSON, nullable=True)
    Start = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    End = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Started = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Completed = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Checked = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Error = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ErrorInfo = sqlalchemy.Column(sqlalchemy.JSON, nullable=True)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode = MEMORY")
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA cache_size = 500000")
        cursor.close()


class PBPTQProcessTool(PBPTProcessToolsBase):
    """
    An abstract class for aiding the quick creation
    of data processing scripts which take their
    parameters from the command line using a JSON
    file as the input.
    """

    def __init__(self, queue_db_info=None, cmd_name=None, descript=None, params=None, uid_len=6):
        """
        A class to implement a processing tool for batch processing data analysis using a queue.

        :param queue_db_info: The database dict info. Require fields: sqlite_db_conn, sqlite_db_file
        :param cmd_name: optionally provide the name of the command (i.e., the python script name).
        :param descript: optionally provide a description of the command file.
        :param params: optionally provide a dict which will be the options for the processing to execute
                       (e.g., the input and output files).

        """
        self.queue_db_info = queue_db_info
        self.cmd_name = cmd_name
        self.descript = descript
        self.params = params
        self.debug_job_id = None
        self.debug_job_id_params = False
        self.debug_job_id_rmouts = False
        super().__init__(uid_len)

    def set_params(self, params):
        """
        A function to set the parameters used for the analysis without setting them within the
        constructor or via a command line JSON file.

        :param params: provide a dict which will be the options for the processing to execute
                       (e.g., the input and output files).

        """
        self.params = params

    def set_queue_db_info(self, queue_db_info):
        """
        Set the queue database info.

        :param queue_db_info: The database dict info. Require fields: sqlite_db_conn, sqlite_db_file

        """
        self.queue_db_info = queue_db_info

    def check_db_info(self):
        """
        A function which tests whether the required database info is present and
        works.

        """
        if 'sqlite_db_conn' not in self.queue_db_info:
            raise Exception("sqlite_db_conn key has is not present.")

        if 'sqlite_db_file' not in self.queue_db_info:
            raise Exception("sqlite_db_file key has is not present.")

        if not os.path.exists(self.queue_db_info['sqlite_db_file']):
            raise Exception("The SQLite database file does not exist: '{}'".format(self.queue_db_info['sqlite_db_file']))

        try:
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.queue_db_info['sqlite_db_conn'], pool_pre_ping=True)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            ses.close()
            logger.debug("Created Database Engine and Session.")
        except:
            raise Exception("The SQLite database file cannot be opened: '{}'".format(self.queue_db_info['sqlite_db']))

        return True

    @abstractmethod
    def do_processing(self, **kwargs):
        """
        An abstract function to undertake processing.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.do_processing(option_a=True, option_b=100)).

        """
        pass

    def completed_processing(self, **kwargs):
        """
        A function which will create a reference file, defined by 'confirm_exe'
        key within the self.params dict.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.completed_processing(option_a=True, option_b=100)).

        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.queue_db_info['sqlite_db_conn'], pool_pre_ping=True)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        job_info = ses.query(PBPTProcessJob).filter(PBPTProcessJob.PID == self.job_pid).one_or_none()
        if job_info is not None:
            job_info.Completed = True
            job_info.Error = False
            job_info.End = datetime.datetime.now()
            ses.commit()
        ses.close()

    def record_process_error(self, err_info, **kwargs):
        """
        A function records an error in the database. Usually called instead of completed_processing.

        :param err_info: a dict with information on the error to be stored in the
                         database.
        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.completed_processing(option_a=True, option_b=100)).

        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.queue_db_info['sqlite_db_conn'], pool_pre_ping=True)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        job_info = ses.query(PBPTProcessJob).filter(PBPTProcessJob.PID == self.job_pid).one_or_none()
        if job_info is not None:
            job_info.Completed = False
            job_info.Error = True
            job_info.ErrorInfo = err_info
            job_info.End = None
            flag_modified(job_info, "ErrorInfo")
            ses.commit()
        ses.close()

    @abstractmethod
    def required_fields(self, **kwargs):
        """
        An abstract function which is required to return a list of string. The list are
        dict keys which are required by the function.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.required_fields(mod_version=True)).
        :return: list of strings

        """
        pass

    @abstractmethod
    def outputs_present(self, **kwargs):
        """
        An abstract function which is required to returns a tuple (boolean, dict with outputs as keys and error
        message as the value). The boolean relates to whether all the outputs are present on the system; True all
        are present and the dict will be empty, False and some of the outputs will not be present and an error
        message will be available within the dict.

        Note. for many applications this function can simply call self.check_files returning it's output.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.outputs_present(mod_version=True)).
        :return: tuple (boolean, dict with outputs as keys and error message as the value)

        """
        pass

    @abstractmethod
    def remove_outputs(self, **kwargs):
        """
        An abstract function which checks if an output is present and removes them. This can be useful
        if an job failed part way through processing and needs to be reset.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.remove_outputs(mod_version=True)).

        """
        pass

    def check_required_fields(self, **kwargs):
        """
        A function which checks that the required fields are present within the
        self.params dict. If the structure is more complex than a simple list
        of variables then this function can be overridden to represent that
        structure.

        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.check_required_fields(mod_version=True)).
                       This variable is passed to the required_fields function.
        :return: Returns a boolean represents the success or otherwise of
                 the function (i.e., self.params dict as the required fields).
                 If an error occurs then an exception will be raised providing
                 an error message which can be displayed ot the user.

        """
        rtn_val = True
        fields = self.required_fields(**kwargs)
        err_fields = []
        for field in fields:
            if field not in self.params:
                rtn_val = False
                err_fields.append(field)
        if not rtn_val:
            if len(err_fields) > 1:
                fields_str = err_fields[0]
                for i in range(1, len(err_fields)):
                    fields_str = "{}, {}".format(fields_str, err_fields[i])
                raise Exception("The following fields '{}' should have been provided "
                                "but were not within the params dict.".format(fields_str))
            else:
                raise Exception("The field '{}' should have been provided but was not "
                                "within the params dict.".format(err_fields[0]))
        return rtn_val

    def parse_cmds(self, argv=None, **kwargs):
        """
        A function to parse the command line arguments to retrieve the
        processing parameters.

        :param argv: A list of the of inputs (e.g., ['-j', 'input_file.json']
        :param kwargs: allows the user to pass custom variables to the function
                       (e.q., obj.parse_cmds(mod_version=True)). These kwargs will
                       be passed to the check_required_fields function when executed.
        :return: True is successfully parsed, False is unsuccessfully parsed.

        """
        try:
            parser = argparse.ArgumentParser(prog=self.cmd_name, description=self.descript)
            parser.add_argument("-d", "--dbinfo", type=str,
                                required=True, help="Specify a file path for a JSON file "
                                                    "containing the database info")
            parser.add_argument("-j", "--job", type=int, required=False, help="Specify a job ID for the job to be "
                                                                             "executed. This is a debug tool and "
                                                                             "therefore the database will not be "
                                                                             "updated and the job will be run "
                                                                             "regardless of the database status.")
            parser.add_argument("-p", "--params", action='store_true', default=False,
                                help="If a job is specified then rather than running the parameters will be "
                                     "printed to the console.")
            parser.add_argument("-r", "--rmouts", action='store_true', default=False,
                                help="If a job is specified then rather than running the job the outputs will be "
                                     "removed.")
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
            import traceback
            traceback.print_exception(*sys.exc_info())
            return False
        return True

    def std_run(self, **kwargs):
        """
        A function which runs a standard processing sequence of parsing the command input(s),
        then called the do_processing() function and finally the completed_processing()
        functions.

        :param kwargs: allows the user to pass custom variables to the function (e.q., obj.std_run(var='blah')).
                       These options are passed to the parse_cmds(), do_processing() and completed_processing()
                       functions.

        """
        import time
        import random
        logger.debug("Starting to execute the 'std_run' function.")
        pbpt_utils = PBPTUtils()
        if self.parse_cmds(**kwargs):
            if self.debug_job_id is None:
                sqlite_db_conn = self.queue_db_info['sqlite_db_conn']
                logger.debug("Database connection info: '{}'.".format(sqlite_db_conn))
                found_job = False
                # Sleep for a random period of time to minimise clashes between multiple processes so they are offset.
                time.sleep(random.randint(1,10))
                while True:
                    try:
                        logger.debug("Creating Database Engine and Session.")
                        db_engine = sqlalchemy.create_engine(sqlite_db_conn, pool_pre_ping=True)
                        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                        ses = session_sqlalc()
                        logger.debug("Created Database Engine and Session.")

                        logger.debug("Find the next scene to process.")
                        job_info = ses.query(PBPTProcessJob).filter(PBPTProcessJob.Completed == False,
                                                                    PBPTProcessJob.Started == False).order_by(
                                                                    PBPTProcessJob.PID.asc()).first()
                        if job_info is not None:
                            found_job = True
                            self.job_pid = job_info.PID
                            self.params = job_info.JobParams
                            job_info.Started = True
                            job_info.Start = datetime.datetime.now()
                            ses.commit()
                            logger.debug("Found the next scene to process. PID: {}".format(self.job_pid))
                        else:
                            found_job = False
                            logger.debug("No job found to process - finishing.")
                        ses.close()
                        logger.debug("Closed Database Engine and Session.")
                    except Exception as e:
                        logger.debug("Failed to create the database connection: '{}'".format(sqlite_db_conn))
                        logger.exception(e)
                        found_job = False
                    if found_job:
                        self.check_required_fields(**kwargs)
                        try:
                            self.do_processing(**kwargs)
                            self.completed_processing(**kwargs)
                        except Exception as e:
                            import traceback
                            err_dict = dict()
                            err_dict['error'] = str(e)
                            err_dict['traceback'] = traceback.format_exc()
                            self.record_process_error(err_dict)
                    else:
                        break
            else:
                sqlite_db_conn = self.queue_db_info['sqlite_db_conn']
                try:
                    logger.debug("Creating Database Engine and Session.")
                    db_engine = sqlalchemy.create_engine(sqlite_db_conn, pool_pre_ping=True)
                    session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                    ses = session_sqlalc()
                    logger.debug("Created Database Engine and Session.")

                    logger.debug("Searching the database for job ID: '{}'".format(self.debug_job_id))
                    job_info = ses.query(PBPTProcessJob).filter(PBPTProcessJob.PID == self.debug_job_id).one_or_none()
                    ses.close()
                    logger.debug("Closed Database Engine and Session.")
                except Exception as e:
                    logger.debug("Failed to create the database connection: '{}'".format(sqlite_db_conn))
                    logger.exception(e)

                self.job_pid = self.debug_job_id
                self.params = job_info.JobParams
                if job_info is not None:
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

                    logger.debug("Finished processing the job, PID: {}".format(self.job_pid))
                else:
                    logger.debug("No job (PID: {}) found to process - finishing.".format(self.job_pid))


class PBPTGenQProcessToolCmds(PBPTProcessToolsBase):

    def __init__(self, cmd, sqlite_db_file, uid_len=6):
        """
        A class to implement a the generation of commands for batch processing data analysis.

        :param cmd: the command to be executed (e.g., python run_analysis.py).
        :param queue_db_info: The database dict info. Require fields: sqlite_db_conn, sqlite_db_file

        """
        self.params = []
        self.cmd = cmd
        self.sqlite_db_file = os.path.abspath(sqlite_db_file)
        self.sqlite_db_conn = "sqlite:///{}".format(self.sqlite_db_file)
        super().__init__(uid_len)

    @abstractmethod
    def gen_command_info(self, **kwargs):
        """
        An abstract function to create the list of dict's for the individual commands.

        The function should populate the self.params variable. Parameters can be passed
        to the function using the kwargs variable.

        e.g.,
        obj.gen_command_info(input='')
        kwargs['input']

        :param kwargs: allows the user to pass custom variables to the function (e.q., obj.gen_command_info(input='')).

        """
        pass

    def check_job_outputs(self, process_tools_mod, process_tools_cls, out_err_pid_file, out_err_info_file, **kwargs):
        """
        A function which following the completion of all the processing for a job tests whether all the output
        files where created (i.e., the job successfully completed).

        :param process_tools_mod: the module (i.e., path to python script) containing the implementation
                                  of the PBPTProcessTool class used for the processing to be checked.
        :param process_tools_cls: the name of the class implementing the PBPTProcessTool class used
                                  for the processing to be checked.
        :param out_err_pid_file: the output file name and path for the list of database PIDs which have not
                                 been successfully processed.
        :param out_err_info_file: the output file name and path for the output error report from this function
                                  where processing might not have fully completed.
        :param kwargs: allows the user to pass custom variables to the function (e.q., obj.gen_command_info(input='')),
                       these will be passed to the process_tools_mod outputs_present function.

        """
        import importlib

        queue_db_info = dict()
        queue_db_info['sqlite_db_file'] = self.sqlite_db_file
        queue_db_info['sqlite_db_conn'] = self.sqlite_db_conn

        process_tools_mod_inst = importlib.import_module(process_tools_mod)
        if process_tools_mod_inst is None:
            raise Exception("Could not load the module: '{}'".format(process_tools_mod))

        process_tools_cls_inst = getattr(process_tools_mod_inst, process_tools_cls)()
        if process_tools_cls_inst is None:
            raise Exception("Could not create instance of '{}'".format(process_tools_cls))
        process_tools_cls_inst.set_queue_db_info(queue_db_info)

        pbpt_utils = PBPTUtils()
        err_pids = []
        err_info = dict()

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        jobs = ses.query(PBPTProcessJob).filter(PBPTProcessJob.Checked==False).all()
        n_errs = 0
        if jobs is not None:
            for job_info in tqdm.tqdm(jobs):
                if job_info.Completed:
                    process_tools_cls_inst.set_params(job_info.JobParams)
                    files_present, errs_dict = process_tools_cls_inst.outputs_present(**kwargs)
                    if not files_present:
                        n_errs = n_errs + 1
                        err_pids.append(job_info.PID)
                        err_info[job_info.PID] = errs_dict
                    else:
                        job_info.Checked = True
                        ses.commit()
                else:
                    n_errs = n_errs + 1
                    err_pids.append(job_info.PID)
                    if job_info.Started:
                        job_info.Started = False
                        ses.commit()
                        err_info[job_info.PID] = "Started but did not complete."
                    else:
                        err_info[job_info.PID] = "Never Started."
        ses.close()

        if len(err_pids) > 0:
            pbpt_utils.writeList2File(err_pids, out_err_pid_file)
            pbpt_utils.writeDict2JSON(err_info, out_err_info_file)
        else:
            pathlib.Path(out_err_pid_file).touch()
            pathlib.Path(out_err_info_file).touch()

    def remove_job_outputs(self, process_tools_mod, process_tools_cls, all_jobs=False, error_jobs=False, **kwargs):
        """
        A function which following the completion of all the processing for a job tests whether all the output
        files where created (i.e., the job successfully completed).

        :param process_tools_mod: the module (i.e., path to python script) containing the implementation
                                  of the PBPTProcessTool class used for the processing to be checked.
        :param process_tools_cls: the name of the class implementing the PBPTProcessTool class used
                                  for the processing to be checked.
        :param all_jobs: boolean specifying that outputs should be removed for all jobs.
        :param error_jobs: boolean specifying that outputs should be removed for error jobs - either
                           logged an error or started but not finished.
        :param job_id: int for the job id for which the outputs will be removed.
        :param kwargs: allows the user to pass custom variables to the function (e.q., obj.gen_command_info(input='')),
                       these will be passed to the process_tools_mod outputs_present function.

        """
        import importlib

        if (not all_jobs) and (not error_jobs):
            raise Exception("Must specify for either all or only error jobs to have the outputs removed.")

        queue_db_info = dict()
        queue_db_info['sqlite_db_file'] = self.sqlite_db_file
        queue_db_info['sqlite_db_conn'] = self.sqlite_db_conn

        process_tools_mod_inst = importlib.import_module(process_tools_mod)
        if process_tools_mod_inst is None:
            raise Exception("Could not load the module: '{}'".format(process_tools_mod))

        process_tools_cls_inst = getattr(process_tools_mod_inst, process_tools_cls)()
        if process_tools_cls_inst is None:
            raise Exception("Could not create instance of '{}'".format(process_tools_cls))
        process_tools_cls_inst.set_queue_db_info(queue_db_info)

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        if all_jobs:
            jobs = ses.query(PBPTProcessJob).filter().all()
        elif error_jobs:
            jobs = ses.query(PBPTProcessJob).filter(PBPTProcessJob.Started == True,
                                                    PBPTProcessJob.Completed == False).all()
        else:
            raise Exception("Must specify for either all or only error jobs to have the outputs removed.")

        if jobs is not None:
            for job_info in tqdm.tqdm(jobs):
                process_tools_cls_inst.set_params(job_info.JobParams)
                process_tools_cls_inst.remove_outputs(**kwargs)
        ses.close()

    def create_jobs_report(self, out_report_file=None):
        """
        A function which generates a JSON report which can either
        be written to the console or an output file.

        :param out_report_file: Optional file path for output report file, in JSON file.
                                If None then report gets written to the console.

        """
        import statistics

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        jobs = ses.query(PBPTProcessJob).all()
        n_errs = 0
        n_completed = 0
        n_started = 0
        n_ended = 0
        n_jobs = 0
        job_times = []
        err_info = dict()
        status_info = dict()
        if jobs is not None:
            for job_info in tqdm.tqdm(jobs):
                if job_info.Completed:
                    job_times.append((job_info.End - job_info.Start).total_seconds())
                    n_completed += 1
                    n_started += 1
                    n_ended += 1
                else:
                    if job_info.Error:
                        n_errs += 1
                        err_info[job_info.PID] = dict()
                        err_info[job_info.PID]['info'] = job_info.ErrorInfo

                    if job_info.Started:
                        n_started += 1
                        status_info[job_info.PID] = "Started but did not complete."
                n_jobs += 1
        ses.close()

        out_info_dict = dict()
        if n_jobs > 0:
            out_info_dict['job_n'] = dict()
            out_info_dict['job_n']['n_completed'] = n_completed
            out_info_dict['job_n']['n_errs'] = n_errs
            out_info_dict['job_n']['n_started'] = n_started
            out_info_dict['job_n']['n_ended'] = n_ended
            out_info_dict['job_n']['total_n_jobs'] = n_jobs

            if len(job_times) > 0:
                out_info_dict['job_times'] = dict()
                out_info_dict['job_times'] = dict()
                out_info_dict['job_times']['time_mean_secs'] = statistics.mean(job_times)
                out_info_dict['job_times']['time_min_secs'] = min(job_times)
                out_info_dict['job_times']['time_max_secs'] = max(job_times)
                if len(job_times) > 1:
                    out_info_dict['job_times']['time_stdev_secs'] = statistics.stdev(job_times)
                out_info_dict['job_times']['time_median_secs'] = statistics.median(job_times)
                if (len(job_times) > 1) and (py_sys_version_flt >= 3.8):
                    out_info_dict['job_times']['time_quartiles_secs'] = statistics.quantiles(job_times)
            out_info_dict["status"] = status_info
            out_info_dict["xerrors"] = err_info

        if out_report_file is None:
            import pprint
            pprint.pprint(out_info_dict)
        else:
            pbpt_utils = PBPTUtils()
            pbpt_utils.writeDict2JSON(out_info_dict, out_report_file)

    def pop_params_db(self):
        """
        A function to write the output files for the commands.

        :param cmd: optional input to override the __init__ variable. The command to be executed
                    (e.g., python run_analysis.py).

        """
        if os.path.exists(self.sqlite_db_file):
            os.remove(self.sqlite_db_file)
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(db_engine)
        logger.debug("Creating Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        job_lst = []
        pbar = tqdm.tqdm(total=len(self.params))
        for i, param in enumerate(self.params):
            job_lst.append(PBPTProcessJob(PID=i, JobParams=param))
            pbar.update(1)

        logger.debug("There are {} jobs to be written to the database.".format(len(job_lst)))
        if len(job_lst) > 0:
            ses.add_all(job_lst)
            ses.commit()
            logger.debug("Written jobs to the database.")
        ses.close()

    def create_shell_exe(self, run_script, cmds_sh_file, n_cores, db_info_file=None):
        """
        A function which generates the scripts to execute on a local
        machine including using GNU parallel.

        :param run_script: The script which will be executed to run analysis with multiple cores
        :param cmds_sh_file: The shell script with the list of jobs to be executed
                             (i.e., equal to the number of cores specified)
        :param n_cores: The number of cores to use for processing - it is assumed that jobs will only use a
                        single core but if your jobs are going to use multiple cores then you'll need to
                        consider how many cores you are going to be using in total.
        :param db_info_file: An output file which will given to the processing commands with the
                             database connection info. If None then a unique file name will be create
                             automatically.

        """
        pbpt_utils = PBPTUtils()
        if db_info_file is None:
            uid_str = self.uid_generator(8)
            db_info_file = "process_db_info_{}.json".format(uid_str)
            db_info_file = os.path.abspath(db_info_file)
            if os.path.exists(db_info_file):
                raise Exception("Strange, the automatically generated database info file ({}) "
                                "already exists - try re-running.".format(db_info_file))

        db_info = dict()
        db_info["sqlite_db_conn"] = self.sqlite_db_conn
        db_info["sqlite_db_file"] = self.sqlite_db_file
        pbpt_utils.writeDict2JSON(db_info, db_info_file)

        lst_cmds = []
        for n in range(n_cores):
            lst_cmds.append("{0} --dbinfo {1}".format(self.cmd, db_info_file))
        pbpt_utils.writeList2File(lst_cmds, cmds_sh_file)

        # Create the run script with GNU parallel.
        parallel_cmd = "parallel -j {} < {}".format(n_cores, cmds_sh_file)
        pbpt_utils.writeData2File(parallel_cmd, run_script)

    def create_slurm_sub_sh(self, jobname, mem_per_core_mb, log_dir, run_script='run_exe_analysis.sh',
                            job_dir="job_scripts", db_info_file=None, account_name=None, n_cores_per_job=10,
                            n_jobs=10, job_time_limit='2-23:59', module_load='module load parallel singularity'):
        """
        A function which generates the scripts needed to run an analysis using slurm.

        :param jobname: The name of the job
        :param mem_per_core_mb: the amount of memory (in megabytes) for each job
        :param log_dir: a directory where the log files will be outputted.
        :param run_script: the file name and path for the script to be executed.
        :param job_dir: directory where the job scripts will be written.
        :param db_info_file: An output file which will given to the processing commands with the
                             database connection info. If None then a unique file name will be create
                             automatically.
        :param account_name: The slurm account name for the jobs to be submitted under.
        :param n_cores_per_job: the number of cores per job
        :param n_jobs: the number of jobs to split the input list of commands into.
        :param job_time_limit: The time limit for the job: Days-HH:MM e.g., 2-23:59; 2 days, 23 hours and 59 minutes.
        :param module_load: Module loads within the sbatch submission scripts. If None them ignored.

        """
        pbpt_utils = PBPTUtils()
        if db_info_file is None:
            uid_str = self.uid_generator(8)
            db_info_file = "process_db_info_{}.json".format(uid_str)
            db_info_file = os.path.abspath(db_info_file)
            if os.path.exists(db_info_file):
                raise Exception("Strange, the automatically generated database info file ({}) "
                                "already exists - try re-running.".format(db_info_file))

        db_info = dict()
        db_info["sqlite_db_conn"] = self.sqlite_db_conn
        db_info["sqlite_db_file"] = self.sqlite_db_file
        pbpt_utils.writeDict2JSON(db_info, db_info_file)

        job_dir = os.path.abspath(job_dir)
        if not os.path.exists(job_dir):
            os.mkdir(job_dir)

        lst_cmds = []
        for n in range(n_cores_per_job):
            lst_cmds.append("{0} --dbinfo {1}".format(self.cmd, db_info_file))
        cmds_sh_file = os.path.join(job_dir, "jobcmds.sh")
        pbpt_utils.writeList2File(lst_cmds, cmds_sh_file)

        log_dir = os.path.abspath(log_dir)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)

        jobname = pbpt_utils.check_str(jobname, rm_non_ascii=True, rm_dashs=True, rm_spaces=True, rm_punc=True)
        sbatch_cmds = list()
        for n in range(n_jobs):
            sbatch_file = os.path.join(job_dir, "job_file_{}.sbatch".format(n))
            c_jobname = "{}_{}".format(jobname, n)
            out_log = os.path.join(log_dir, "{}_log.out".format(c_jobname))
            err_log = os.path.join(log_dir, "{}_log.err".format(c_jobname))
            with open(sbatch_file, 'w') as sbatch_file_obj:
                sbatch_file_obj.write("#!/bin/bash --login\n")
                sbatch_file_obj.write("\n")
                if account_name is not None:
                    sbatch_file_obj.write("#SBATCH --account={}\n".format(account_name))
                sbatch_file_obj.write("#SBATCH --partition=compute\n")
                sbatch_file_obj.write("#SBATCH --job-name={}\n".format(c_jobname))
                sbatch_file_obj.write("#SBATCH --output={}.%J\n".format(out_log))
                sbatch_file_obj.write("#SBATCH --error={}.%J\n".format(err_log))
                sbatch_file_obj.write("#SBATCH --time={}\n".format(job_time_limit))
                sbatch_file_obj.write("#SBATCH --ntasks={}\n".format(n_cores_per_job))
                sbatch_file_obj.write("#SBATCH --mem-per-cpu={}\n".format(mem_per_core_mb))
                if module_load is not None:
                    sbatch_file_obj.write("\n")
                    sbatch_file_obj.write("{}\n".format(module_load))
                sbatch_file_obj.write("\n")
                sbatch_file_obj.write("parallel -N 1 --delay .2 -j $SLURM_NTASKS < {}\n\n".format(cmds_sh_file))
                sbatch_cmds.append("sbatch {}".format(sbatch_file))

        pbpt_utils.writeList2File(sbatch_cmds, run_script)

    @abstractmethod
    def run_gen_commands(self):
        """
        An abstract function which needs to be implemented with the functions and inputs
        you want run to generate the various commands and scripts to be executed.

        You will presumably want to call:

         * self.gen_command_info
         * self.write_cmd_files

        and then maybe

         * create_slurm_sub_sh

        """
        pass

    @abstractmethod
    def run_check_outputs(self):
        """
        An abstract function which needs to be implemented with the functions and inputs
        you want run to check the outputs of the processing have been successfully completed.

        You will presumably want to call:

         * self.check_job_outputs

        """
        pass

    @abstractmethod
    def run_remove_outputs(self, all_jobs=False, error_jobs=False):
        """
        An abstract function which needs to be implemented with the functions and inputs
        you want run to check the outputs of the processing have been successfully completed.

        You will presumably want to call:

         * self.remove_job_outputs

        """
        pass

    def parse_cmds(self, argv=None):
        """
        A function to parse the command line arguments to retrieve the
        processing parameters.

        :param argv: A list of the of inputs (e.g., ['--gen'] or ['--check']

        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--gen", action='store_true', default=False, help="Execute run_gen_commands() function.")
        parser.add_argument("--check", action='store_true', default=False, help="Execute run_check_outputs() function.")
        parser.add_argument("--report", action='store_true', default=False, help="Execute create_jobs_report() function.")
        parser.add_argument("--rmouts", action='store_true', default=False, help="Execute run_remove_outputs() function.")
        parser.add_argument("--all", action='store_true', default=False, help="Remove outputs for all jobs.")
        parser.add_argument("--error", action='store_true', default=False, help="Remove outputs for jobs with errors.")
        parser.add_argument("-o", "--output", type=str, required=False, help="Specify a report output JSON file. If not provided then report written to console.")
        if argv is None:
            argv = sys.argv[1:]
        args = parser.parse_args(argv)

        if args.gen:
            self.run_gen_commands()
        elif args.check:
            self.run_check_outputs()
        elif args.report:
            self.create_jobs_report(args.output)
        elif args.rmouts:
            self.run_remove_outputs(all_jobs=args.all, error_jobs=args.error)

