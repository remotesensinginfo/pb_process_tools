#!/usr/bin/env python
"""
pb_process_tools - Setup/Update the system.
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
import math
import os.path
import tqdm

import pbprocesstools.pbpt_utils

logger = logging.getLogger('splitcmdslist.py')

def splitByNumCommands(cmds_lst, n_out_cmds, outputfile, deal_split=False):
    """

    :param cmds_lst:
    :param n_out_cmds:
    :param outputfile:
    :return:
    """
    pbpt_txt_utils = pbprocesstools.pbpt_utils.PBPTTextFileUtils()
    n_cmds = len(cmds_lst)
    n_out_files = math.floor(n_cmds / n_out_cmds)
    n_remain = n_cmds - (n_out_files * n_out_cmds)
    outfile_base, outfile_ext = os.path.splitext(outputfile)

    # Loop through and create individual files.
    out_file_lst = []
    if deal_split:
        if n_remain > 0:
            n_out_files = n_out_files + 1

        outfile_id = 1
        split_lst = []
        for i in range(n_out_files):
            outfile_name = '{0}_{1}{2}'.format(outfile_base, outfile_id, outfile_ext)
            logger.info('Creating file: {}.'.format(outfile_name))
            out_file_lst.append(outfile_name)
            i_info = dict()
            i_info["file"] = outfile_name
            i_info["cmds"] = list()
            outfile_id = outfile_id + 1
            split_lst.append(i_info)

        c_lst = 0
        for cmd in tqdm.tqdm(cmds_lst):
            print("{} - {}".format(c_lst, cmd))
            split_lst[c_lst]["cmds"].append(cmd)
            c_lst = c_lst + 1
            if c_lst >= n_out_files:
                c_lst = 0

        for out_cmds_file in split_lst:
            pbpt_txt_utils.writeList2File(out_cmds_file["cmds"], out_cmds_file["file"])
    else:
        outfile_id = 1
        for i in range(n_out_files):
            l_bound = i * n_out_cmds
            u_bound = (i + 1) * n_out_cmds
            outfile_name = '{0}_{1}{2}'.format(outfile_base, outfile_id, outfile_ext)
            logger.info('Creating file: {}.'.format(outfile_name))
            pbpt_txt_utils.writeList2File(cmds_lst[l_bound:u_bound], outfile_name)
            out_file_lst.append(outfile_name)
            outfile_id = outfile_id + 1

        if n_remain > 0:
            # Output remaining cmds file
            l_bound = n_out_files * n_out_cmds
            u_bound = n_cmds
            outfile_name = '{0}_{1}{2}'.format(outfile_base, outfile_id, outfile_ext)
            logger.info('Creating file: {}.'.format(outfile_name))
            pbpt_txt_utils.writeList2File(cmds_lst[l_bound:u_bound], outfile_name)
            out_file_lst.append(outfile_name)

    out_filelist_name = '{0}_filelst{2}'.format(outfile_base, outfile_id, outfile_ext)
    pbpt_txt_utils.writeList2File(out_file_lst, out_filelist_name)

def splitIntoNFiles(cmds_lst, n_out_files, outputfile, deal_split=False):
    n_cmds = len(cmds_lst)
    n_cmds_perfile = math.ceil(n_cmds/n_out_files)
    logger.info("There will be {} commands per output file.".format(n_cmds_perfile))
    splitByNumCommands(cmds_lst, n_cmds_perfile, outputfile, deal_split)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, required=True, help="Specify an input file.")
    parser.add_argument("-o", "--output", type=str, required=True, help="Specify an output file which is used "
                                                                        "as the base name for the output files.")
    parser.add_argument("-s", "--split", type=int, default=0, help="The number of commands per output file.")
    parser.add_argument("-f", "--nfiles", type=int, default=0, help="The number of output files to generate.")
    parser.add_argument("-p", "--precmd", type=str, help="Optionally provide custom command (e.g., singularity) to be "
                                                         "prepended to all the commands.")
    parser.add_argument("--dealsplit", action='store_true', default=False,
                        help="Splits the commands as cards would be dealt rather than linear sequential split.")

    args = parser.parse_args()

    if (args.split == 0) and (args.nfiles == 0):
        raise Exception("You must specify either --split or --files.")
    elif (args.split > 0) and (args.nfiles > 0):
        raise Exception("You have specified both --split and --files just specify one "
                        "(i.e., with a value greater than zero).")

    pbpt_txt_utils = pbprocesstools.pbpt_utils.PBPTTextFileUtils()
    cmds_lst = pbpt_txt_utils.readTextFile2List(args.input)

    if args.precmd is not None:
        tmp_cmds_lst = list()
        for cmd in cmds_lst:
            n_cmd = '{0} {1}'.format(args.precmd, cmd)
            tmp_cmds_lst.append(n_cmd)
        cmds_lst = tmp_cmds_lst

    if args.split > 0:
        splitByNumCommands(cmds_lst, args.split, args.output, args.dealsplit)
    elif args.nfiles > 0:
        splitIntoNFiles(cmds_lst, args.nfiles, args.output, args.dealsplit)