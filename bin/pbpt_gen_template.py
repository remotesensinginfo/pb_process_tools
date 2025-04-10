#! /usr/bin/env python

############################################################################
#  pbpt_gen_template.py
#
#  Copyright 2023 ARCSI.
#
# Purpose:  A script generate a template for running pb_process_tools
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 6/1/2023
# Version: 1.0
#
# History:
# Version 1.0 - Created.
#
############################################################################

import argparse
import os

import jinja2
import rsgislib.tools.filetools
import rsgislib.tools.utils

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
        # You can access the input parameters from within the self.params dict
        # For example: self.params["input1"]

    def required_fields(self, **kwargs):
        # Return a list of the required fields which will be checked
        return [
            "input1",
            "input2",
            "input3",
            "output1",
        ]

    def outputs_present(self, **kwargs):
        # Check the output files are as expected - called with --check option
        # the function expects a tuple with the first item a list of booleans
        # specifying whether the file is OK and secondly a dict with outputs 
        # as keys and any error message as the value
        
        # A function (self.check_files) has been provided to do the work for 
        # you which takes a dict of inputs which will do the work for you in 
        # most cases. The supported file types are: gdal_image, gdal_vector,
        # hdf5, file (checks present) and filesize (checks present and size > 0)
        
        files_dict = dict()
        files_dict[self.params["output1"]] = "gdal_image"
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output files and reset anything 
        # else which might need to be reset if re-running the job.
        if os.path.exists(self.params["output1"]):
            os.remove(self.params["output1"])


if __name__ == "__main__":
    ProcessCmd().std_run()

"""


pbpt_gen_cmds_cls_slurm_sing_tmplt = """
import logging
import os
import glob
import rsgislib.tools.filetools

from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

logger = logging.getLogger(__name__)

class GenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        print("** This function needs implementing! **")
        
        # Create output directory if it doesn't exist.
        if not os.path.exists(kwargs["out_dir"]):
            os.mkdir(kwargs["out_dir"])
        
        # Get the list of input images
        ref_imgs = glob.glob(kwargs["ref_imgs"])
        
        # Loop through the reference images to create the jobs
        for ref_img in ref_imgs:
            # Get the basename of the input file to make the output file name
            basename = rsgislib.tools.filetools.get_file_basename(ref_img)
            
            # Create the output file name
            output_file = os.path.join(kwargs["out_dir"], f"{basename}.tif")
            # Check if the output file exists.
            if not os.path.exists(output_file):
                # You will probably have a loop here:
                # Within the loop create a dict with the parameters for each
                # job which will be added to the self.params list.
                c_dict = dict()
                c_dict["input1"] = ""
                c_dict["input2"] = ""
                c_dict["input3"] = ""
                c_dict["output1"] = ""
                self.params.append(c_dict)       
    
    def run_gen_commands(self):
        # Could Pass info to gen_command_info function 
        # (e.g., input / output directories)
        self.gen_command_info(ref_imgs="path/to/inputs/*.kea",
                              out_dir="/path/to/outputs")

        self.pop_params_db()
        
        self.create_slurm_sub_sh(
            "job name", # Provide a name for the job
            16448, # Specify the memory which should be requested from slurm
            "/path/to/logs", # Specify a path where slurm console logs will be written
            run_script="run_exe_analysis.sh", # The file to call to run analysis
            job_dir="job_scripts", # Directory will all the slurm files
            db_info_file="pbpt_lcl_db_info.txt", 
            #account_name="", # Optionally provide an account name
            n_cores_per_job=5, # The number of cores per job
            n_xtr_cmds=5, # Number of extra commands - allows for jobs crashes.
            n_jobs=5, # the number of jobs to run in slurm
            job_time_limit="2-23:59", # Time limit for the jobs.
            module_load="module load parallel singularity", # modules to load
        )

if __name__ == "__main__":
    py_script = os.path.abspath("{{ ana_py_file }}")
    script_cmd = f"{{ sing_cmd }} python {py_script}"

    process_tools_mod = "{{ ana_py_mod }}"
    process_tools_cls = "ProcessCmd"

    create_tools = GenCmds(
        cmd=script_cmd,
        db_conn_file="{{ dbfile }}",
        lock_file_path="./pbpt_lock_file.txt",
        process_tools_mod=process_tools_mod,
        process_tools_cls=process_tools_cls,
    )
    create_tools.parse_cmds()

"""

pbpt_gen_cmds_cls_slurm_tmplt = """
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

logger = logging.getLogger(__name__)

class GenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        print("** This function needs implementing! **")

        # Create output directory if it doesn't exist.
        if not os.path.exists(kwargs["out_dir"]):
            os.mkdir(kwargs["out_dir"])

        # You will probably have a loop here:
        # Within the loop create a dict with the parameters for each
        # job which will be added to the self.params list.
        c_dict = dict()
        c_dict["input1"] = ""
        c_dict["input2"] = ""
        c_dict["input3"] = ""
        c_dict["output1"] = ""
        self.params.append(c_dict)       

    def run_gen_commands(self):
        # Could Pass info to gen_command_info function 
        # (e.g., input / output directories)
        self.gen_command_info(out_dir="/path/to/outputs")

        self.pop_params_db()
        
        self.create_slurm_sub_sh(
            "job name", # Provide a name for the job
            16448, # Specify the memory which should be requested from slurm
            "/path/to/logs", # Specify a path where slurm console logs will be written
            run_script="run_exe_analysis.sh", # The file to call to run analysis
            job_dir="job_scripts", # Directory will all the slurm files
            db_info_file="pbpt_lcl_db_info.txt",
            #account_name="", # Optionally provide an account name
            n_cores_per_job=5, # The number of cores per job
            n_xtr_cmds=5, # Number of extra commands - allows for jobs crashes.
            n_jobs=5, # the number of jobs to run in slurm
            job_time_limit="2-23:59", # Time limit for the jobs.
            module_load="module load parallel", # modules to load
        )

if __name__ == "__main__":
    py_script = os.path.abspath("{{ ana_py_file }}")
    script_cmd = f"python {py_script}"

    process_tools_mod = "{{ ana_py_mod }}"
    process_tools_cls = "ProcessCmd"

    create_tools = GenCmds(
        cmd=script_cmd,
        db_conn_file="{{ dbfile }}",
        lock_file_path="./pbpt_lock_file.txt",
        process_tools_mod=process_tools_mod,
        process_tools_cls=process_tools_cls,
    )
    create_tools.parse_cmds()

"""

pbpt_gen_cmds_cls_parallel_sing_tmplt = """
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

logger = logging.getLogger(__name__)

class GenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        print("** This function needs implementing! **")

        # Create output directory if it doesn't exist.
        if not os.path.exists(kwargs["out_dir"]):
            os.mkdir(kwargs["out_dir"])

        # You will probably have a loop here:
        # Within the loop create a dict with the parameters for each
        # job which will be added to the self.params list.
        c_dict = dict()
        c_dict["input1"] = ""
        c_dict["input2"] = ""
        c_dict["input3"] = ""
        c_dict["output1"] = ""
        self.params.append(c_dict)       

    def run_gen_commands(self):
        # Could Pass info to gen_command_info function 
        # (e.g., input / output directories)
        self.gen_command_info(out_dir="/path/to/outputs")

        self.pop_params_db()

        self.create_shell_exe(
            run_script="run_exe_analysis.sh", # The file to call to run analysis
            cmds_sh_file="pbpt_cmds_lst.sh", # The list of commands to be run.
            n_cores=10, # The number of cores to use for analysis.
            db_info_file="pbpt_lcl_db_info.json", 
        )

if __name__ == "__main__":
    py_script = os.path.abspath("{{ ana_py_file }}")
    script_cmd = f"{{ sing_cmd }} python {py_script}"

    process_tools_mod = "{{ ana_py_mod }}"
    process_tools_cls = "ProcessCmd"

    create_tools = GenCmds(
        cmd=script_cmd,
        db_conn_file="{{ dbfile }}",
        lock_file_path="./pbpt_lock_file.txt",
        process_tools_mod=process_tools_mod,
        process_tools_cls=process_tools_cls,
    )
    create_tools.parse_cmds()

"""

pbpt_gen_cmds_cls_parallel_tmplt = """
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

logger = logging.getLogger(__name__)

class GenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        print("** This function needs implementing! **")

        # Create output directory if it doesn't exist.
        if not os.path.exists(kwargs["out_dir"]):
            os.mkdir(kwargs["out_dir"])

        # You will probably have a loop here:
        # Within the loop create a dict with the parameters for each
        # job which will be added to the self.params list.
        c_dict = dict()
        c_dict["input1"] = ""
        c_dict["input2"] = ""
        c_dict["input3"] = ""
        c_dict["output1"] = ""
        self.params.append(c_dict)       

    def run_gen_commands(self):
        # Could Pass info to gen_command_info function 
        # (e.g., input / output directories)
        self.gen_command_info(out_dir="/path/to/outputs")

        self.pop_params_db()
        
        self.create_shell_exe(
            run_script="run_exe_analysis.sh", # The file to call to run analysis
            cmds_sh_file="pbpt_cmds_lst.sh", # The list of commands to be run.
            n_cores=10, # The number of cores to use for analysis.
            db_info_file="pbpt_lcl_db_info.json", 
        )

if __name__ == "__main__":
    py_script = os.path.abspath("{{ ana_py_file }}")
    script_cmd = f"python {py_script}"

    process_tools_mod = "{{ ana_py_mod }}"
    process_tools_cls = "ProcessCmd"

    create_tools = GenCmds(
        cmd=script_cmd,
        db_conn_file="{{ dbfile }}",
        lock_file_path="./pbpt_lock_file.txt",
        process_tools_mod=process_tools_mod,
        process_tools_cls=process_tools_cls,
    )
    create_tools.parse_cmds()

"""


def gen_pb_process_tools_template(
    output_dir,
    dbfile,
    out_file_prefix="",
    use_slurm=False,
    singularity=False,
    singularity_paths=None,
    overwrite=False,
):
    if not os.path.exists(output_dir):
        raise Exception("Specified output directory does not exist.")

    sing_cmd = ""
    use_singularity = False
    if (singularity is not None) and (singularity != ""):
        sing_paths_bind = ""
        if singularity_paths is not None:
            for sing_path in singularity_paths:
                sing_paths_bind += f" --bind {sing_path}"
        sing_cmd = f"singularity exec {sing_paths_bind} {singularity}"
        use_singularity = True

    gen_py_file_base = "gen_cmds.py"
    ana_py_file_base = "perform_analysis.py"
    run_gen_sh_base = "run_gen_cmds.sh"
    run_report_sh_base = "run_report.sh"
    run_reset_errs_sh_base = "run_reset_errs.sh"
    run_check_sh_base = "run_checks.sh"

    if (out_file_prefix is not None) and (out_file_prefix != ""):
        gen_py_file_base = f"{out_file_prefix}_{gen_py_file_base}"
        ana_py_file_base = f"{out_file_prefix}_{ana_py_file_base}"
        run_gen_sh_base = f"{out_file_prefix}_{run_gen_sh_base}"
        run_report_sh_base = f"{out_file_prefix}_{run_report_sh_base}"
        run_reset_errs_sh_base = f"{out_file_prefix}_{run_reset_errs_sh_base}"
        run_check_sh_base = f"{out_file_prefix}_{run_check_sh_base}"

    gen_py_file = os.path.join(output_dir, gen_py_file_base)
    ana_py_file = os.path.join(output_dir, ana_py_file_base)
    ana_py_mod = rsgislib.tools.filetools.get_file_basename(ana_py_file)
    run_gen_sh_file = os.path.join(output_dir, run_gen_sh_base)
    run_report_sh_file = os.path.join(output_dir, run_report_sh_base)
    run_reset_errs_sh_file = os.path.join(output_dir, run_reset_errs_sh_base)
    run_check_sh_file = os.path.join(output_dir, run_check_sh_base)

    out_files = [
        gen_py_file,
        ana_py_file,
        run_gen_sh_file,
        run_report_sh_file,
        run_reset_errs_sh_file,
        run_check_sh_file,
    ]
    
    for out_file in out_files:
        if os.path.exists(out_file) and (not overwrite):
            raise Exception(
                f"File already exists, select --overwrite if you "
                f"wish to over write: {out_file}"
            )
        elif os.path.exists(out_file) and overwrite:
            os.remove(out_file)

    py_cmd = f"{sing_cmd}python"

    if use_singularity and use_slurm:
        pbpt_gen_cmds_cls_tmplt_jj = jinja2.Template(pbpt_gen_cmds_cls_slurm_sing_tmplt)
        pbpt_gen_cmds_cls_code = pbpt_gen_cmds_cls_tmplt_jj.render(
            ana_py_file=ana_py_file_base,
            dbfile=dbfile,
            ana_py_mod=ana_py_mod,
            sing_cmd=sing_cmd,
        )
    elif use_slurm:
        pbpt_gen_cmds_cls_tmplt_jj = jinja2.Template(pbpt_gen_cmds_cls_slurm_tmplt)
        pbpt_gen_cmds_cls_code = pbpt_gen_cmds_cls_tmplt_jj.render(
            ana_py_file=ana_py_file_base, dbfile=dbfile, ana_py_mod=ana_py_mod
        )
    elif use_singularity:
        pbpt_gen_cmds_cls_tmplt_jj = jinja2.Template(
            pbpt_gen_cmds_cls_parallel_sing_tmplt
        )
        pbpt_gen_cmds_cls_code = pbpt_gen_cmds_cls_tmplt_jj.render(
            ana_py_file=ana_py_file_base,
            dbfile=dbfile,
            ana_py_mod=ana_py_mod,
            sing_cmd=sing_cmd,
        )
    else:
        pbpt_gen_cmds_cls_tmplt_jj = jinja2.Template(pbpt_gen_cmds_cls_parallel_tmplt)
        pbpt_gen_cmds_cls_code = pbpt_gen_cmds_cls_tmplt_jj.render(
            ana_py_file=ana_py_file_base, dbfile=dbfile, ana_py_mod=ana_py_mod
        )

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

    rsgislib.tools.utils.write_data_to_file(pbpt_gen_cmds_cls_code, gen_py_file)
    rsgislib.tools.utils.write_data_to_file(pbpt_process_cmd_code, ana_py_file)

    run_gen_cmd = f"{py_cmd} {gen_py_file} --gen"
    rsgislib.tools.utils.write_data_to_file(run_gen_cmd, run_gen_sh_file)

    run_report_cmd = f"{py_cmd} {gen_py_file} --report | more"
    rsgislib.tools.utils.write_data_to_file(run_report_cmd, run_report_sh_file)

    run_reset_errs_cmd = f"{py_cmd} {gen_py_file} --error --rmouts"
    rsgislib.tools.utils.write_data_to_file(run_reset_errs_cmd, run_reset_errs_sh_file)

    run_check_cmd = f"{py_cmd} {gen_py_file} --check"
    rsgislib.tools.utils.write_data_to_file(run_check_cmd, run_check_sh_file)


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
        required=True,
        help="A path to a file with the database connection string",
    )
    parser.add_argument(
        "-p", "--prefix", type=str, default="", help="Prefix for the output files",
    )
    parser.add_argument(
        "--slurm",
        action="store_true",
        default=False,
        help="Setup template to use slurm",
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

    # Call the parser to parse the arguments.
    args = parser.parse_args()

    gen_pb_process_tools_template(
        args.output,
        args.dbfile,
        out_file_prefix=args.prefix,
        use_slurm=args.slurm,
        singularity=args.singularity,
        singularity_paths=args.sing_paths,
        overwrite=args.overwrite,
    )
