Get Started
===========

Install
--------

Installation using pip::
    
    pip install pb-process-tools


Batch Processing Using a Queue
-------------------------------

Using the ``PBPTQProcessTool`` and ``PBPTGenQProcessToolCmds`` abstract classes you can define classes for:

    * generating a set of tasks
    * performing those tasks
    * checking whether those tasks completed
    * reporting on time take and any errors.

The job information is stored within an sqlite or postgresql database, using a JSON field is used to store a dictionary of the input parameters for each task.

The first step is to create an implementation of the class which is going to do the data processing:

This python script has been saved in a file called ``perform_processing.py``::

    from pbprocesstools.pbpt_q_process import PBPTQProcessTool
    import logging
    import random
    import time

    logger = logging.getLogger(__name__)

    class ProcessJob(PBPTQProcessTool):

        def __init__(self):
            super().__init__(cmd_name='perform_processing.py', descript=None)

        def do_processing(self, **kwargs):
            logger.debug("Value: {}".format(self.params["value"]))
            time.sleep(random.randint(1,10))
            if self.params["value"] > 500:
                raise Exception("Error value is greater than 500: {}".format(self.params["value"]))

        def required_fields(self, **kwargs):
            return ["value"]

        def outputs_present(self, **kwargs):
            return True, dict()

        def remove_outputs(self, **kwargs):
            print("No outputs to remove")

    if __name__ == "__main__":
        ProcessJob().std_run()

This class is a very simple class and performs very little work, just logging a value which has been passed to the class, sleeping for a period and if the input value is greater than 500 an exception is throw (to illustrate the systems error reporting). The following functions need implementing:

    * ``__init__`` - provide the name of the script so the help documentation is correct - not critical.
    * ``do_processing`` - the function which performs the analysis
    * ``required_fields`` - returns a list of the required input fields within the params dict.
    * ``outputs_present`` - performs a test (if possible) to check whether the processing of the job has successfully completed. In this case, it just returns True as there are no outputs to check.
    * ``remove_outputs`` - removes job outputs. This is usually used to reset a job which for some reason failed during processing.

The second class which needs implementing generates the jobs (i.e., the inputs to the class above):

This python script has been saved in a file called ``gen_process_cmds.py``::

    from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
    import os.path
    import logging
    import random

    logger = logging.getLogger(__name__)

    class CreateTestCmds(PBPTGenQProcessToolCmds):

        def gen_command_info(self, **kwargs):

            for jobs in range(100):
                c_dict = dict()
                c_dict['value'] = random.randint(1, 1000)
                self.params.append(c_dict)

        def run_gen_commands(self):
            self.gen_command_info()
            self.pop_params_db()
            self.create_shell_exe("exe_analysis.sh", "cmds.sh", 4, db_info_file=None)

    if __name__ == "__main__":
        py_script = os.path.abspath("perform_processing.py")
        script_cmd = "python {}".format(py_script)

        process_tools_mod = 'perform_processing'
        process_tools_cls = 'ProcessJob'

        create_tools = CreateTestCmds(cmd=script_cmd, sqlite_db_file="test_jobs.db",
                                      process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
        create_tools.parse_cmds()


This class generates the individual job parameters, in this case 100 jobs. The dict for each job has to be appended to the ``self.params`` list within the ``gen_command_info`` function. The ``run_gen_commands`` function needs to call the ``gen_command_info`` function but also specifies the output format (i.e., batch processing using GNU parallel on a local system or via slurm on a cluster). In this case it is via a GNU parallel and a shell script listing the commands.

When the class ``CreateTestCmds`` is instantiated, the command to be executed for processing to occur (i.e., ``python perform_processing.py``) needs to be specified and the database file name and path is required.

To run the code to generate the database with the job information, execute the following command::

    python gen_process_cmds.py --gen

This should generate the database and an output script ``exe_analysis.sh`` (which simply contain the GNU Parallel command to execute), which is then executed::

    sh exe_analysis.sh

Your analysis should now be complete...

The next step is to check whether the analysis was successful, execute the following comamand::

    python gen_process_cmds.py --check

Hopefully the reports files will be empty, but in this demo we have explicitly generated errors so there will be errors within these reports.

You can also generate a summary report which will include the python Exception trace and error message for jobs which have failed, execute the following command::

    python gen_process_cmds.py --report

If you want the report outputted to a file run::

    python gen_process_cmds.py --check -o report.json

To remove outputs where an error occurred then you can use the following::

    python gen_process_cmds.py --rmouts --error
    
To remove outputs for all jobs then you can use the following::

    python gen_process_cmds.py --rmouts --all

Where you have had an error occur it can be useful to run a single task in isolation without the database recording any information and any exception being returned to the console rather than captured. This can be performed by calling the processing python file. For example, to process job 20, run the following command::

    python perform_processing.py --dbinfo process_db_info.json -j 20

Where the ``--dbinfo`` input will have been generated and provides the database location and connection information. You're file name will be similar but with a different random set of characters at the end.

You can remove the outputs for just one job using the following command::

    python perform_processing.py --dbinfo process_db_info.json -j 20 -r

You can also print the parameters for a job as well::

    python perform_processing.py --dbinfo process_db_info.json -j 20 -p



