#!/usr/bin/env python
"""
pb_slurm_user_tools - Setup/Update the system.
"""
# This file is part of 'pb_slurm_user_tools'
# A set of utilities for working with slurm.
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

import pbslurmusertools.pbsut_utils

logger = logging.getLogger('splitcmdslist.py')

def splitByNumCommands(cmds_lst, n_out_cmds, outputfile):
    """

    :param cmds_lst:
    :param n_out_cmds:
    :param outputfile:
    :return:
    """
    pbs_txt_utils = pbslurmusertools.pbsut_utils.PBSUTTextFileUtils()
    n_cmds = len(cmds_lst)
    n_out_files = math.floor(n_cmds / n_out_cmds)
    n_remain = n_cmds - (n_out_files * n_out_cmds)
    outfile_base, outfile_ext = os.path.splitext(outputfile)

    # Loop through and create individual files.
    out_file_lst = []
    outfile_id = 1
    for i in range(n_out_files):
        l_bound = i * n_out_cmds
        u_bound = (i + 1) * n_out_cmds
        outfile_name = '{0}_{1}{2}'.format(outfile_base, outfile_id, outfile_ext)
        logger.info('Creating file: {}.'.format(outfile_name))
        pbs_txt_utils.writeList2File(cmds_lst[l_bound:u_bound], outfile_name)
        out_file_lst.append(outfile_name)
        outfile_id = outfile_id + 1

    if n_remain > 0:
        # Output remaining cmds file
        l_bound = n_out_files * n_out_cmds
        u_bound = n_cmds
        outfile_name = '{0}_{1}{2}'.format(outfile_base, outfile_id, outfile_ext)
        logger.info('Creating file: {}.'.format(outfile_name))
        pbs_txt_utils.writeList2File(cmds_lst[l_bound:u_bound], outfile_name)
        out_file_lst.append(outfile_name)

    out_filelist_name = '{0}_filelst{2}'.format(outfile_base, outfile_id, outfile_ext)
    pbs_txt_utils.writeList2File(out_file_lst, out_filelist_name)

def splitIntoNFiles(cmds_lst, n_out_files, outputfile):
    n_cmds = len(cmds_lst)
    n_cmds_perfile = math.ceil(n_cmds/n_out_files)
    logger.info("There will be {} commands per output file.".format(n_cmds_perfile))
    splitByNumCommands(cmds_lst, n_cmds_perfile, outputfile)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, required=True, help="Specify an input file.")
    parser.add_argument("-o", "--output", type=str, required=True, help="Specify an output file which is used "
                                                                        "as the base name for the output files.")
    parser.add_argument("-s", "--split", type=int, default=0, help="The number of commands per output file.")
    parser.add_argument("-f", "--nfiles", type=int, default=0, help="The number of output files to generate.")

    args = parser.parse_args()

    if (args.split == 0) and (args.nfiles == 0):
        raise Exception("You must specify either --split or --files.")
    elif (args.split > 0) and (args.nfiles > 0):
        raise Exception("You have specified both --split and --files just specify one "
                        "(i.e., with a value greater than zero).")

    pbs_txt_utils = pbslurmusertools.pbsut_utils.PBSUTTextFileUtils()
    cmds_lst = pbs_txt_utils.readTextFile2List(args.input)

    if args.split > 0:
        splitByNumCommands(cmds_lst, args.split, args.output)
    elif args.nfiles > 0:
        splitIntoNFiles(cmds_lst, args.nfiles, args.output)