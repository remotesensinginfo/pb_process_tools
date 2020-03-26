#!/usr/bin/env python
"""
pb_process_tools - prefix to .
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
# Purpose:  Command line tool for splitting a list of commands into
#           separate files.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 20/11/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import logging

import pbprocesstools.pbpt_utils

logger = logging.getLogger('prefixcmdslst.py')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, required=True, help="Specify an input file.")
    parser.add_argument("-o", "--output", type=str, required=True, help="Specify an output file which is used ")
    parser.add_argument("-p", "--prefix", type=str, required=True, help="Provide a custom command (e.g., Singularity "
                                                                        "or Docker) to be prepended to all the "
                                                                        "commands in the input file.")

    args = parser.parse_args()

    pbpt_txt_utils = pbprocesstools.pbpt_utils.PBPTTextFileUtils()
    cmds_lst = pbpt_txt_utils.readTextFile2List(args.input)

    out_cmds_lst = list()
    for cmd in cmds_lst:
        if (cmd != "") and ('#' not in cmd):
            n_cmd = '{0} {1}'.format(args.prefix, cmd)
            out_cmds_lst.append(n_cmd)

    pbpt_txt_utils.writeList2File(out_cmds_lst, args.output)

