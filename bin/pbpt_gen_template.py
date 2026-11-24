#! /usr/bin/env python

############################################################################
#  pbpt_gen_template.py
#
#  Copyright 2023 ARCSI.
#
# Purpose:  A script to generate a template for running pb_process_tools
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 6/1/2023
# Version: 4.0.2
#
############################################################################

import argparse
import os
import shlex

import jinja2

from pbprocesstools.pbpt_process import PBPTProcessToolsBase
from pbprocesstools.pbpt_utils import PBPTUtils

BLACK_AVAIL = True
try:
    import black
except ImportError:
    BLACK_AVAIL = False

pbpt_process_cmd_code_tmplt = """
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTQProcessTool

logger = logging.getLogger(__name__)


class ProcessCmd(PBPTQProcessTool):
    def __init__(self):
        super().__init__(cmd_name="{{ ana_py_file }}", descript=None)

    def do_processing(self, **kwargs):
        print("** Implemented your analysis here **")
        # Access job input parameters via self.params, e.g. self.params["input1"]

    def required_fields(self, **kwargs):
        return [
            "input1",
            "input2",
            "input3",
            "output1",
        ]

    def outputs_present(self, **kwargs):
        # Supported check_files types: gdal_image, gdal_vector, hdf5,
        # file (checks present), and filesize (checks present and size > 0).
        files_dict = dict()
        files_dict[self.params["output1"]] = "gdal_image"
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        if os.path.exists(self.params["output1"]):
            os.remove(self.params["output1"])


if __name__ == "__main__":
    ProcessCmd().std_run()
"""


pbpt_gen_cmds_cls_tmplt = """
import glob
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

logger = logging.getLogger(__name__)


class GenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        print("** This function needs implementing! **")

        # Create output directory if it doesn't exist.
        os.makedirs(kwargs["out_dir"], exist_ok=True)

        # Get the list of input images
        ref_imgs = glob.glob(kwargs["ref_imgs"])

        # Loop through the reference images to create the jobs
        for ref_img in ref_imgs:
            basename = self.get_file_basename(ref_img)
            output_file = os.path.join(kwargs["out_dir"], f"{basename}.tif")
            if not os.path.exists(output_file):
                c_dict = dict()
                c_dict["input1"] = ""
                c_dict["input2"] = ""
                c_dict["input3"] = ""
                c_dict["output1"] = output_file
                self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(
            ref_imgs="path/to/inputs/*.kea",
            out_dir="/path/to/outputs",
        )

        # Pass --append to insert only new jobs or --dry-run to preview.
        self.pop_params_db()

{% if use_slurm %}
        self.create_slurm_sub_sh(
            "job name",  # Provide a name for the job
            16448,  # Memory per core in MB (--mem-per-cpu); set mem_total_mb for --mem
            "/path/to/logs",  # Directory where Slurm console logs will be written
            run_script="{{ file_prefix }}run_exe_analysis.sh",
            job_dir="{{ file_prefix }}job_scripts",
            db_info_file="{{ file_prefix }}pbpt_lcl_db_info.json",
            # account_name=None,
            n_cores_per_job=5,
            n_xtr_cmds=5,
            n_jobs=5,
            job_time_limit="2-23:59",
            module_load="{{ slurm_module_load }}",
            partition={{ partition_repr }},
            use_array={{ use_array }},
            max_concurrent={{ max_concurrent_repr }},
            # mem_total_mb=None,
            # qos=None,
            # constraint=None,
            # gres=None,
            # extra_sbatch_args=None,
        )
{% else %}
        self.create_shell_exe(
            run_script="{{ file_prefix }}run_exe_analysis.sh",
            cmds_sh_file="{{ file_prefix }}pbpt_cmds_lst.sh",
            n_cores=10,
            db_info_file="{{ file_prefix }}pbpt_lcl_db_info.json",
        )
{% endif %}


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    py_script = os.path.join(script_dir, "{{ ana_py_file }}")
{% if use_singularity %}
    script_cmd = f"{{ sing_cmd }} python {py_script}"
{% else %}
    script_cmd = f"python {py_script}"
{% endif %}

    process_tools_mod = "{{ ana_py_mod }}"
    process_tools_cls = "ProcessCmd"

    # Uses db_conn_file if given; otherwise reads PBPT_DB_CONN_STR from environment.
    create_tools = GenCmds(
        cmd=script_cmd,
        {% if dbfile is not none %}db_conn_file="{{ dbfile }}",{% endif %}
        process_tools_mod=process_tools_mod,
        process_tools_cls=process_tools_cls,
        db_table_name={{ db_table_name_repr }},
        db_schema={{ db_schema_repr }},
    )
    create_tools.parse_cmds()
"""


def gen_pb_process_tools_template(
    output_dir,
    dbfile=None,
    out_file_prefix="",
    use_slurm=False,
    singularity=False,
    singularity_paths=None,
    overwrite=False,
    db_table_name=None,
    db_schema=None,
    partition="compute",
    use_array=False,
    max_concurrent=None,
):
    if not os.path.exists(output_dir):
        raise FileNotFoundError(
            "Specified output directory does not exist: {}".format(output_dir)
        )
    if not isinstance(db_table_name, str) or not db_table_name.strip():
        raise ValueError(
            "db_table_name must be specified so each task uses its own database table."
        )
    db_table_name = db_table_name.strip()

    pbpt_base = PBPTProcessToolsBase()
    pbpt_utils = PBPTUtils()

    sing_cmd = ""
    use_singularity = False
    if (singularity is not None) and (singularity is not False) and (singularity != ""):
        sing_paths_bind = ""
        if singularity_paths is not None:
            for sing_path in singularity_paths:
                sing_paths_bind += f" --bind {sing_path}"
        sing_cmd = f"singularity exec{sing_paths_bind} {singularity}"
        use_singularity = True

    gen_py_file_base = "gen_cmds.py"
    ana_py_file_base = "perform_analysis.py"
    run_gen_sh_base = "run_gen_cmds.sh"
    run_status_sh_base = "run_status.sh"
    run_errors_sh_base = "run_errors.sh"
    run_report_sh_base = "run_report.sh"
    run_reset_errs_sh_base = "run_reset_errs.sh"
    run_check_sh_base = "run_checks.sh"
    file_prefix = ""

    if (out_file_prefix is not None) and (out_file_prefix != ""):
        file_prefix = f"{out_file_prefix}_"
        gen_py_file_base = f"{file_prefix}{gen_py_file_base}"
        ana_py_file_base = f"{file_prefix}{ana_py_file_base}"
        run_gen_sh_base = f"{file_prefix}{run_gen_sh_base}"
        run_status_sh_base = f"{file_prefix}{run_status_sh_base}"
        run_errors_sh_base = f"{file_prefix}{run_errors_sh_base}"
        run_report_sh_base = f"{file_prefix}{run_report_sh_base}"
        run_reset_errs_sh_base = f"{file_prefix}{run_reset_errs_sh_base}"
        run_check_sh_base = f"{file_prefix}{run_check_sh_base}"

    gen_py_file = os.path.join(output_dir, gen_py_file_base)
    ana_py_file = os.path.join(output_dir, ana_py_file_base)
    ana_py_mod = pbpt_base.get_file_basename(ana_py_file)
    run_gen_sh_file = os.path.join(output_dir, run_gen_sh_base)
    run_status_sh_file = os.path.join(output_dir, run_status_sh_base)
    run_errors_sh_file = os.path.join(output_dir, run_errors_sh_base)
    run_report_sh_file = os.path.join(output_dir, run_report_sh_base)
    run_reset_errs_sh_file = os.path.join(output_dir, run_reset_errs_sh_base)
    run_check_sh_file = os.path.join(output_dir, run_check_sh_base)

    out_files = [
        gen_py_file,
        ana_py_file,
        run_gen_sh_file,
        run_status_sh_file,
        run_errors_sh_file,
        run_report_sh_file,
        run_reset_errs_sh_file,
        run_check_sh_file,
    ]

    for out_file in out_files:
        if os.path.exists(out_file) and (not overwrite):
            raise FileExistsError(
                f"File already exists, select --overwrite if you "
                f"wish to overwrite: {out_file}"
            )
        elif os.path.exists(out_file) and overwrite:
            os.remove(out_file)

    py_cmd = f"{sing_cmd} python" if use_singularity else "python"
    slurm_module_load = (
        "module load parallel singularity" if use_singularity else "module load parallel"
    )

    render_ctx = {
        "ana_py_file": ana_py_file_base,
        "dbfile": dbfile,
        "ana_py_mod": ana_py_mod,
        "sing_cmd": sing_cmd,
        "use_slurm": bool(use_slurm),
        "use_singularity": use_singularity,
        "slurm_module_load": slurm_module_load,
        "file_prefix": file_prefix,
        "db_table_name_repr": repr(db_table_name),
        "db_schema_repr": repr(db_schema),
        "partition_repr": repr(partition),
        "use_array": repr(bool(use_array)),
        "max_concurrent_repr": repr(max_concurrent),
    }

    pbpt_gen_cmds_cls_tmplt_jj = jinja2.Template(pbpt_gen_cmds_cls_tmplt)
    pbpt_gen_cmds_cls_code = pbpt_gen_cmds_cls_tmplt_jj.render(**render_ctx)

    pbpt_process_cmd_code_tmplt_jj = jinja2.Template(pbpt_process_cmd_code_tmplt)
    pbpt_process_cmd_code = pbpt_process_cmd_code_tmplt_jj.render(
        ana_py_file=ana_py_file_base
    )

    if BLACK_AVAIL:
        pbpt_gen_cmds_cls_code = black.format_str(
            pbpt_gen_cmds_cls_code, mode=black.FileMode()
        )
        pbpt_process_cmd_code = black.format_str(
            pbpt_process_cmd_code, mode=black.FileMode()
        )

    pbpt_utils.writeData2File(pbpt_gen_cmds_cls_code, gen_py_file)
    pbpt_utils.writeData2File(pbpt_process_cmd_code, ana_py_file)

    quoted_gen_py = shlex.quote(gen_py_file)
    sh_scripts = [
        (f'#!/bin/bash\n{py_cmd} {quoted_gen_py} --gen "$@"', run_gen_sh_file),
        (f'#!/bin/bash\nwatch -n 5 {py_cmd} {quoted_gen_py} --status "$@"', run_status_sh_file),
        (f'#!/bin/bash\n{py_cmd} {quoted_gen_py} --errors "$@"', run_errors_sh_file),
        (
            f'#!/bin/bash\n{py_cmd} {quoted_gen_py} --report "$@" | more',
            run_report_sh_file,
        ),
        (
            f'#!/bin/bash\n{py_cmd} {quoted_gen_py} --rmouts --error "$@"',
            run_reset_errs_sh_file,
        ),
        (f'#!/bin/bash\n{py_cmd} {quoted_gen_py} --check "$@"', run_check_sh_file),
    ]
    for script_content, script_path in sh_scripts:
        pbpt_utils.writeData2File(script_content, script_path)
        os.chmod(script_path, 0o755)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="pbpt_gen_template.py",
        description="Create template files using pb_process_tools",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output directory within which the files will be created",
    )
    parser.add_argument(
        "-d",
        "--dbfile",
        type=str,
        required=False,
        default=None,
        help="Path to a file with the PostgreSQL connection string (optional if PBPT_DB_CONN_STR is set)",
    )
    parser.add_argument(
        "-p", "--prefix", type=str, default="", help="Prefix for the output files",
    )
    parser.add_argument(
        "-t",
        "--table",
        type=str,
        required=True,
        help="PostgreSQL table name for this task's job queue (must be unique per concurrent task)",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="Optional PostgreSQL schema name for the job queue table",
    )
    parser.add_argument(
        "--slurm",
        action="store_true",
        default=False,
        help="Setup template to use slurm",
    )
    parser.add_argument(
        "--partition",
        type=str,
        default="compute",
        help="Slurm partition name when using --slurm (default: compute)",
    )
    parser.add_argument(
        "--array",
        action="store_true",
        default=False,
        help="Configure Slurm template to submit a Slurm job array",
    )
    parser.add_argument(
        "--max_concurrent",
        type=int,
        default=None,
        help="Optional Slurm job array concurrency limit (used with --array)",
    )
    parser.add_argument(
        "--singularity", type=str, help="Setup template to use singularity",
    )
    parser.add_argument(
        "--sing_paths",
        nargs="*",
        type=str,
        help="If using singularity then provide the paths "
        "to be mounted (e.g., /home/user:/data)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite files if they already exists",
    )

    args = parser.parse_args()

    gen_pb_process_tools_template(
        args.output,
        dbfile=args.dbfile,
        out_file_prefix=args.prefix,
        use_slurm=args.slurm,
        singularity=args.singularity,
        singularity_paths=args.sing_paths,
        overwrite=args.overwrite,
        db_table_name=args.table,
        db_schema=args.schema,
        partition=args.partition,
        use_array=args.array,
        max_concurrent=args.max_concurrent,
    )
