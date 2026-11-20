Get Started
===========

Install
-------

Install ``pb_process_tools`` (v4.0.0) with PostgreSQL driver support::

    pip install ".[postgres]"

or from source::

    pip install .

Batch Processing Using a PostgreSQL Queue
-----------------------------------------

Using the ``PBPTQProcessTool`` and ``PBPTGenQProcessToolCmds`` abstract classes, you define:

* generating and managing a task's queue table in PostgreSQL (with per-task table isolation via ``db_table_name`` and optional ``db_schema``),
* executing individual jobs concurrently across GNU Parallel workers or Slurm jobs / job arrays,
* monitoring live queue status and inspecting errors,
* verifying outputs in parallel and resetting or cleaning up jobs and queue tables.

Because each workflow specifies its own ``db_table_name``, multiple workflows can run concurrently from the same PostgreSQL database without interfering with one another.

1. Define the Worker (``perform_processing.py``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Implement a subclass of ``PBPTQProcessTool``:

.. code-block:: python

    import logging
    import random
    import time

    from pbprocesstools.pbpt_q_process import PBPTQProcessTool

    logger = logging.getLogger(__name__)


    class ProcessJob(PBPTQProcessTool):
        def __init__(self):
            super().__init__(cmd_name="perform_processing.py", descript=None)

        def do_processing(self, **kwargs):
            logger.debug("Value: {}".format(self.params["value"]))
            time.sleep(random.randint(1, 3))
            if self.params["value"] > 500:
                raise RuntimeError(
                    "Error value is greater than 500: {}".format(self.params["value"])
                )

        def required_fields(self, **kwargs):
            return ["value"]

        def outputs_present(self, **kwargs):
            return True, dict()

        def remove_outputs(self, **kwargs):
            print("No outputs to remove")


    if __name__ == "__main__":
        ProcessJob().std_run()

The worker methods are:

* ``__init__``: passes ``cmd_name`` (and optional ``descript``) to ``super().__init__``.
* ``do_processing``: executes the task using parameters stored in ``self.params``.
* ``required_fields``: lists the mandatory keys expected inside ``self.params``.
* ``outputs_present``: verifies that expected output files exist/are valid and returns ``(bool, errors_dict)``. See ``self.check_files(...)`` for ``gdal_image``, ``gdal_vector``, ``hdf5``, ``filesize``, and ``file`` checks.
* ``remove_outputs``: removes outputs produced by a job when running ``--rmouts`` or ``-j <PID> -r``.

2. Define the Queue Generator (``gen_process_cmds.py``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Implement a subclass of ``PBPTGenQProcessToolCmds``. Provide the PostgreSQL connection via ``db_conn_file``, ``db_conn_str``, or the ``PBPT_DB_CONN_STR`` environment variable, and specify a unique ``db_table_name``:

.. code-block:: python

    import os
    import random

    from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds


    class CreateTestCmds(PBPTGenQProcessToolCmds):
        def gen_command_info(self, **kwargs):
            for _ in range(100):
                self.params.append({"value": random.randint(1, 1000)})

        def run_gen_commands(self):
            self.gen_command_info()
            self.pop_params_db()
            self.create_shell_exe(
                run_script="exe_analysis.sh",
                cmds_sh_file="cmds.sh",
                n_cores=4,
                db_info_file="pbpt_lcl_db_info.json",
            )


    if __name__ == "__main__":
        script_dir = os.path.dirname(os.path.abspath(__file__))
        py_script = os.path.join(script_dir, "perform_processing.py")

        create_tools = CreateTestCmds(
            cmd=f"python {py_script}",
            db_conn_file="db_conn.txt",
            process_tools_mod="perform_processing",
            process_tools_cls="ProcessJob",
            db_table_name="demo_test_jobs",
            db_schema="public",
        )
        create_tools.parse_cmds()

You can also scaffold both scripts and wrapper shell scripts automatically using ``pbpt_gen_template.py``::

    pbpt_gen_template.py -o ./my_workflow -d db_conn.txt -t demo_test_jobs

3. Run, Monitor, Check, and Clean Up
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Preview job generation without modifying the database or writing scripts::

    python gen_process_cmds.py --gen --dry-run

Populate the queue table and generate execution scripts::

    python gen_process_cmds.py --gen

Run the generated commands (either GNU Parallel locally or ``sbatch`` scripts on Slurm)::

    sh exe_analysis.sh

Monitor live progress, throughput, and ETA::

    python gen_process_cmds.py --status
    python gen_process_cmds.py --watch --interval 5

Append newly discovered jobs incrementally (skipping duplicates already in the queue)::

    python gen_process_cmds.py --gen --append

Verify outputs in parallel across 8 threads::

    python gen_process_cmds.py --check -w 8

Inspect errored or incomplete jobs directly from the CLI::

    python gen_process_cmds.py --errors --limit 10
    python gen_process_cmds.py --inspect 20

Reset job state without deleting outputs (``--reset``), or delete outputs and reset state (``--rmouts``)::

    python gen_process_cmds.py --reset --error
    python gen_process_cmds.py --reset --pids 20 21
    python gen_process_cmds.py --rmouts --error

Debug a single job directly with the worker script (runs without modifying the queue DB)::

    python perform_processing.py --dbinfo pbpt_lcl_db_info.json -j 20 -p   # Print params
    python perform_processing.py --dbinfo pbpt_lcl_db_info.json -j 20      # Run job 20
    python perform_processing.py --dbinfo pbpt_lcl_db_info.json -j 20 -r   # Remove outputs

Drop the task's queue table when finished, or clean leftover tables across the database::

    python gen_process_cmds.py --drop -y
    pbpt_clean_db.py -d db_conn.txt --all-schemas --drop --completed -y
