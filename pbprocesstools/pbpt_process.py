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


class PBPTProcessTool(ABC):
    """
    An abstract class for aiding the quick creation
    of simple processing scripts which take their
    parameters from the command line using a JSON
    file as the input.
    """

    def __init__(self, cmd_name=None, descript=None, params=None):
        """
        A class to implement a processing tool for batch processing data analysis.

        :param cmd_name: optionally provide the name of the command (i.e., the python script name).
        :param descript: optionally provide a description of the command file.
        :param params: optionally provide a list of dicts, which will relate to each command options.

        """
        self.cmd_name = cmd_name
        self.descript = descript
        self.params = params
        super().__init__()

    @abstractmethod
    def do_processing(self):
        """
        An abstract function to undertake processing.

        """
        pass

    def parse_cmds(self, argv=None):
        """
        A function to parse the command line arguments to retrieve the
        processing parameters.

        :param argv: A list of the of inputs (e.g., ['-j', 'input_file.json']
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
        except Exception:
            import traceback
            traceback.print_exception(*sys.exc_info())
            return False
        return True


class PBPTGenProcessToolCmds(ABC):

    def __init__(self, cmd, cmds_sh_file, out_cmds_base=None):
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
        super().__init__()

    @abstractmethod
    def gen_command_info(self, **kwargs):
        """
        An abstract function to create the list of dict's for the individual commands.

        The function should populate the self.params variable. Parameters can be passed
        to the function using the kwargs variable.

        e.g.,
        obj.gen_command_info(input='')
        kwargs['input']

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

        with open(cmds_sh_file, 'w') as cmds_sh_file_obj:
            for i, param in enumerate(self.params):
                out_json_file = '{}{}.json'.format(out_cmds_base, i + 1)
                with open(out_json_file, 'w') as ojf:
                    json.dump(param, ojf, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                cmds_sh_file_obj.write("{} -j {}\n".format(cmd, out_json_file))
                cmds_sh_file_obj.flush()

