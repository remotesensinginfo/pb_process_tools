#!/usr/bin/env python
"""
pb_process_tools - this file is needed to ensure it can be imported

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
# Purpose:  Setup variables and imports across the whole module
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 25/03/2020
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import json
import sys
import os.path
from abc import ABC, abstractmethod


class PBPTProcessToolsBase(ABC):

    def __init__(self, uid_len=6):
        self.uid = self.uid_generator(size=uid_len)
        super().__init__()

    def uid_generator(self, size=6):
        """
        A function which will generate a 'random' string of the specified length based on the UUID

        :param size: the length of the returned string.
        :return: string of length size.

        """
        import uuid
        randomStr = str(uuid.uuid4())
        randomStr = randomStr.replace("-","")
        return randomStr[0:size]

    def find_file(self, dir_path, file_search):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s). Returns None is not found.

        :param dir_path: the file path(s) to be searched. Can be either a single string or a list of file paths
                         to be searched.
        :param file_search: the file search string.
        :return: a string with the file path found or None is either no file paths are
                 found or multiple paths are found.

        """
        import glob
        import os.path

        # Test whether a string has been inputted. If a string has been
        # provided then make it a list with one element.
        if isinstance(dir_path, str):
            dir_path = [dir_path]

        for c_dir_path in dir_path:
            files = glob.glob(os.path.join(c_dir_path, file_search))
            if len(files) > 0:
                break

        if len(files) != 1:
            return None
        return files[0]

    def get_file_basename(self, filepath, checkvalid=False, n_comps=0):
        """
        Uses os.path module to return file basename (i.e., path and extension removed)

        :param filepath: string for the input file name and path
        :param checkvalid: if True then resulting basename will be checked for punctuation
                           characters (other than underscores) and spaces, punctuation
                           will be either removed and spaces changed to an underscore.
                           (Default = False)
        :param n_comps: if > 0 then the resulting basename will be split using underscores
                        and the return based name will be defined using the n_comps
                        components split by under scores.
        :return: basename for file

        """
        import string
        basename = os.path.splitext(os.path.basename(filepath))[0]
        if checkvalid:
            basename = basename.replace(' ', '_')
            for punct in string.punctuation:
                if (punct != '_') and (punct != '-'):
                    basename = basename.replace(punct, '')
        if n_comps > 0:
            basename_split = basename.split('_')
            if len(basename_split) < n_comps:
                raise Exception("The number of components specified is more than "
                                "the number of components in the basename.")
            out_basename = ""
            for i in range(n_comps):
                if i == 0:
                    out_basename = basename_split[i]
                else:
                    out_basename = out_basename + '_' + basename_split[i]
            basename = out_basename
        return basename

    def create_dir(self, tmp_dir, use_abs_path=True, add_uid=False):
        """
        A function which creates a temporary directory and optionally adds
        a unique ID to the last directory within the path so each time the
        process executes a unique temporary is created.

        :param tmp_dir: an input string with the temporary directory path.
        :param use_abs_path: a boolean specifying whether the input path should be
                             converted to an absolute path if a relative path has been provided.
                             The default is True.
        :param add_uid: A boolean specifying whether a UID should be added to the tmp_dir_path
        :return: (str, bool) returns the path of the temporary directory created and boolean as to whether the
                directory was created or already existed (True = created).

        """
        import pathlib
        tmp_dir_path = pathlib.Path(tmp_dir)
        if use_abs_path:
            tmp_dir_path = tmp_dir_path.resolve()

        if add_uid:
            last_dir_name = tmp_dir_path.parent.name
            last_dir_name_uid = "{}_{}".format(last_dir_name, self.uid)
            tmp_dir_path = tmp_dir_path.parent.joinpath(pathlib.Path(last_dir_name_uid))

        created = False
        if not tmp_dir_path.exists():
            tmp_dir_path.mkdir()
            created = True
        return str(tmp_dir_path), created

    def remove_dir(self, in_dir):
        """
        A function which deletes the in_dir and its contents.

        :param in_dir: an input string with the directory path to be removed.

        """
        import shutil
        import pathlib

        in_dir_path = pathlib.Path(in_dir)
        if in_dir_path.exists():
            shutil.rmtree(in_dir, ignore_errors=True)


class PBPTProcessTool(PBPTProcessToolsBase):
    """
    An abstract class for aiding the quick creation
    of simple processing scripts which take their
    parameters from the command line using a JSON
    file as the input.
    """

    def __init__(self, cmd_name=None, descript=None, params=None, uid_len=6):
        """
        A class to implement a processing tool for batch processing data analysis.

        :param cmd_name: optionally provide the name of the command (i.e., the python script name).
        :param descript: optionally provide a description of the command file.
        :param params: optionally provide a list of dicts, which will relate to each command options.

        """
        self.cmd_name = cmd_name
        self.descript = descript
        self.params = params
        super().__init__(uid_len)

    @abstractmethod
    def do_processing(self, **kwargs):
        """
        An abstract function to undertake processing.

        :param kwargs:

        """
        pass

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
            parser.add_argument("-p", "--params", type=str,
                                required=True, help="Specify a file path for a JSON file "
                                                    "containing the input processing"
                                                    "parameters.")
            if argv is None:
                argv = sys.argv[1:]
            args = parser.parse_args(argv)
            with open(args.params) as f:
                self.params = json.load(f)

            if not self.check_required_fields(**kwargs):
                raise Exception("The required fields where not present.")
        except Exception:
            import traceback
            traceback.print_exception(*sys.exc_info())
            return False
        return True


class PBPTGenProcessToolCmds(PBPTProcessToolsBase):

    def __init__(self, cmd, cmds_sh_file, out_cmds_base=None, uid_len=6):
        """
        A class to implement a the generation of commands for batch processing data analysis.

        :param cmd: the command to be executed (e.g., python run_analysis.py).
        :param cmds_sh_file: the output file with the list of the commands to be executed
                             (e.g., /file/path/cmds_list.sh).
        :param out_cmds_base: the base output file name and path for the individual commands
                              (e.g., /file/path/sgl_cmd_).

        """
        self.params = []
        self.cmd = cmd
        self.cmds_sh_file = cmds_sh_file
        self.out_cmds_base = out_cmds_base
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

    def write_cmd_files(self, cmd=None, cmds_sh_file=None, out_cmds_base=None):
        """
        A function to write the output files for the commands.

        :param cmd: optional input to override the __init__ variable. The command to be executed
                    (e.g., python run_analysis.py).
        :param cmds_sh_file: optional input to override the __init__ variable. The output file with the list of
                             the commands to be executed (e.g., /file/path/cmds_list.sh).
        :param out_cmds_base: optional input to override the __init__ variable. the base output file name and path
                              for the individual commands (e.g., /file/path/sgl_cmd_).

        """
        if cmd is None:
            cmd = self.cmd
        if cmds_sh_file is None:
            cmds_sh_file = self.cmds_sh_file
        if out_cmds_base is None:
            out_cmds_base = self.out_cmds_base

        cmds_sh_file = os.path.abspath(cmds_sh_file)

        if out_cmds_base is None:
            cmds_sh_file_path = os.path.split(cmds_sh_file)[0]
            cmds_sh_file_basename = os.path.splitext(os.path.basename(cmds_sh_file))[0]
            out_cmds_base = os.path.join(cmds_sh_file_path, '{}_cmd_'.format(cmds_sh_file_basename))
        else:
            out_cmds_base = os.path.abspath(out_cmds_base)

        out_cmds_base = os.path.abspath(out_cmds_base)
        out_cmds_dir_path = os.path.dirname(out_cmds_base)
        if not os.path.exists(out_cmds_dir_path):
            os.mkdir(out_cmds_dir_path)

        with open(cmds_sh_file, 'w') as cmds_sh_file_obj:
            for i, param in enumerate(self.params):
                out_json_file = '{}{}.json'.format(out_cmds_base, i + 1)
                with open(out_json_file, 'w') as ojf:
                    json.dump(param, ojf, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                cmds_sh_file_obj.write("{} -p {}\n".format(cmd, out_json_file))
                cmds_sh_file_obj.flush()

    def create_slurm_sub_sh(self, jobname, mem_per_core_mb, log_dirs, run_script='gen_exe_hpccmds.sh',
                            sbatch_config='config-sbatch.json', sbatch_template='sbatch_template.jinja2',
                            account_name=None, n_cores_per_job=10, n_jobs=10, job_time_limit='2-23:59',
                            sub_scripts_dir='./subscriptsplit', prepend=None, cmds_sh_file=None,
                            module_load='module load parallel singularity'):
        """
        A function which generates the scripts needed to run an analysis using slurm.

        :param jobname: The name of the job
        :param mem_per_core_mb: the amount of memory (in megabytes) for each job
        :param log_dirs: a directory where the log files will be outputted.
        :param run_script: the file name and path for the script to be executed.
        :param sbatch_config: the file name and path for the config json file for the sbatch submission script.
        :param sbatch_template: the file name and path for the jinja2 template for the sbatch submission script.
        :param account_name: The slurm account name for the jobs to be submitted under.
        :param n_cores_per_job: the number of cores per job
        :param n_jobs: the number of jobs to split the input list of commands into.
        :param job_time_limit: The time limit for the job: Days-HH:MM e.g., 2-23:59; 2 days, 23 hours and 59 minutes.
        :param sub_scripts_dir: a directory where the intermediate scripts to be generated to.
        :param prepend: A command to be pre-appended to the commands in the output scripts
                       (e.g., for singularity or docker)
        :param cmds_sh_file: A custom commands list file. Normally not used.
        :param module_load: Module loads within the sbatch submission scripts. If None them ignored.

        """
        log_dirs = os.path.abspath(log_dirs)
        sub_scripts_dir = os.path.abspath(sub_scripts_dir)
        sbatch_config = os.path.abspath(sbatch_config)
        sbatch_template = os.path.abspath(sbatch_template)
        if cmds_sh_file is None:
            cmds_sh_file = self.cmds_sh_file
        cmds_sh_file = os.path.abspath(cmds_sh_file)

        # Create the pbprocesstools sbatch config file.
        sbatch_config_dict = dict()
        sbatch_config_dict["pbprocesstools"] = dict()
        sbatch_config_dict["pbprocesstools"]["sbatch"] = dict()
        if account_name is not None:
            sbatch_config_dict["pbprocesstools"]["sbatch"]["account"] = account_name
        sbatch_config_dict["pbprocesstools"]["sbatch"]["jobname"] = jobname
        sbatch_config_dict["pbprocesstools"]["sbatch"]["logfileout"] = os.path.join(log_dirs,
                                                                                    "{}_log.out".format(jobname))
        sbatch_config_dict["pbprocesstools"]["sbatch"]["logfileerr"] = os.path.join(log_dirs,
                                                                                    "{}_log.err".format(jobname))
        sbatch_config_dict["pbprocesstools"]["sbatch"]["time"] = job_time_limit
        sbatch_config_dict["pbprocesstools"]["sbatch"]["mem_per_core_mb"] = "{}".format(mem_per_core_mb)
        sbatch_config_dict["pbprocesstools"]["sbatch"]["ncores"] = "{}".format(n_cores_per_job)

        with open(sbatch_config, 'w') as sbatch_config_file:
            json.dump(sbatch_config_dict, sbatch_config_file, sort_keys=True, indent=4,
                      separators=(',', ': '), ensure_ascii=False)

        # Create the sbatch template
        with open(sbatch_template, 'w') as sbatch_template_file:
            sbatch_template_file.write("#!/bin/bash --login\n")
            sbatch_template_file.write("\n")
            if account_name is not None:
                sbatch_template_file.write("#SBATCH --account={{ account }}\n")
            sbatch_template_file.write("#SBATCH --partition=compute\n")
            sbatch_template_file.write("#SBATCH --job-name={{ jobname }}\n")
            sbatch_template_file.write("#SBATCH --output={{ logfileout }}.%J\n")
            sbatch_template_file.write("#SBATCH --error={{ logfileerr }}.%J\n")
            sbatch_template_file.write("#SBATCH --time={{ time }}\n")
            sbatch_template_file.write("#SBATCH --ntasks={{ ncores }}\n")
            sbatch_template_file.write("#SBATCH --mem-per-cpu={{ mem_per_core_mb }}\n")
            if module_load is not None:
                sbatch_template_file.write("\n")
                sbatch_template_file.write("{}\n".format(module_load))
            sbatch_template_file.write("\n")
            sbatch_template_file.write("parallel -N 1 --delay .2 -j $SLURM_NTASKS < {{cmds_file}}\n")

        # Create the shell script which runs the whole processing chain.
        with open(run_script, 'w') as run_script_file:
            run_script_file.write("#!/bin/bash\n")
            run_script_file.write("# Check the scripts directory exists, if not then create\n")
            run_script_file.write("if [ ! -d \"{}\" ]; then\n".format(sub_scripts_dir))
            run_script_file.write("  mkdir {}\n".format(sub_scripts_dir))
            run_script_file.write("fi\n")
            run_script_file.write("\n")

            run_script_file.write("# Split the input commands into a file for each job.\n")
            sub_cmds_sh_file = os.path.join(sub_scripts_dir, "cmds_sh_file.sh")
            if prepend is None:
                run_script_file.write("splitcmdslist.py -i {} -o {} -f {}\n".format(cmds_sh_file,
                                                                                    sub_cmds_sh_file,
                                                                                    n_jobs))
            else:
                run_script_file.write("{} splitcmdslist.py -i {} -o {} -f {}\n".format(prepend,
                                                                                       cmds_sh_file,
                                                                                       sub_cmds_sh_file,
                                                                                       n_jobs))
            run_script_file.write("\n")

            run_script_file.write("# Create the SLURM job submission scripts.\n")
            cmds_filelst = os.path.join(sub_scripts_dir, "cmds_sh_file_filelst.sh")
            cmds_srun = os.path.join(sub_scripts_dir, "cmds_sh_file_srun.sh")
            cmds_sbatch = os.path.join(sub_scripts_dir, "cmds_sh_file_sbatch.sh")
            if prepend is None:
                run_script_file.write("genslurmsub.py -c {} -t {} -i {} -f {} -o {} --multi\n".format(sbatch_config,
                                                                                                      sbatch_template,
                                                                                                      cmds_filelst,
                                                                                                      cmds_srun,
                                                                                                      cmds_sbatch))
            else:
                run_script_file.write("{} genslurmsub.py -c {} -t {} -i {} -f {} -o {} --multi\n".format(prepend,
                                                                                                         sbatch_config,
                                                                                                         sbatch_template,
                                                                                                         cmds_filelst,
                                                                                                         cmds_srun,
                                                                                                         cmds_sbatch))

            run_script_file.write("\n")

            run_script_file.write("# Submit the jobs to the slurm batch queue.\n")
            run_sbatch = os.path.join(sub_scripts_dir, "cmds_sh_file_sbatch_runall.sh")
            run_script_file.write("sh {}\n".format(run_sbatch))




