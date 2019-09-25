#!/usr/bin/env python
"""
pb_process_tools - Create slurm submission script(s).
"""
# This file is part of 'pb_process_tools'
# A set of utilities for batch processing data.
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
# Purpose:  Command line tool for running the system.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 12/11/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import logging

import pbprocesstools.pbpt_utils
import pbprocesstools.pbpt_sbatch

logger = logging.getLogger('genslurmsub.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, required=True, help="Path to the JSON config file.")
    parser.add_argument("-i", "--input", type=str, required=True, help="Specify an input file.")
    parser.add_argument("-f", "--cmdsfile", type=str, required=True, help="Specify an output file with srun "
                                                                          "commands to be run.")
    parser.add_argument("-o", "--output", type=str, required=True, help="Specify an output file.")
    parser.add_argument("-t", "--template", type=str, help="Optionally provide a custom template file.")
    parser.add_argument("-p", "--precmd", type=str, help="Optionally provide custom command (e.g., singularity) to be "
                                                         "prepended to all the commands.")
    parser.add_argument("--multi", action='store_true', default=False,
                        help="""Specify that multiple input files are being provided as a file list (i.e., 
                                the input file is a file which lists the input files.""")

    args = parser.parse_args()

    if args.multi:
        pbpt_txt_utils = pbprocesstools.pbpt_utils.PBPTTextFileUtils()
        in_file_lst = pbpt_txt_utils.readTextFile2List(args.input)
        pbprocesstools.pbpt_sbatch.get_gnuparallel_multi_sbatch(args.config, in_file_lst, args.cmdsfile,
                                                                args.output, args.template, args.precmd)
    else:
        pbprocesstools.pbpt_sbatch.get_gnuparallel_single_sbatch(args.config, args.input, args.cmdsfile,
                                                                 args.output, args.template, args.precmd)
