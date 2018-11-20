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
#
# Purpose:  Create input scripts for the sbatch command.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 14/11/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json
import os
import os.path
import jinja2

import pbslurmusertools.pbsut_utils

logger = logging.getLogger(__name__)


def create_srun_gnuparallel_cmds_file(input_file, out_cmds_file):
    """

    :param input_file:
    :param out_cmds_file:
    :return:
    """
    pbs_txt_utils = pbslurmusertools.pbsut_utils.PBSUTTextFileUtils()
    in_cmds_lst = pbs_txt_utils.readTextFile2List(input_file)
    out_cmds_lst = []
    for cmd in in_cmds_lst:
        out_cmd = "srun -n1 -N1 --exclusive {}".format(cmd)
        out_cmds_lst.append(out_cmd)
    pbs_txt_utils.writeList2File(out_cmds_lst, out_cmds_file)


def get_gnuparallel_single_sbatch(config_file, input_file, out_cmds_file, output_sbatch_file, custom_template):
    """
    A function which generates an sbatch submission script where a list of commands
    will be executed using GNU Parallel.

    :param config_file:
    :param input_file:
    :param out_cmds_file:
    :param output_sbatch_file:
    :return:
    """
    # Parse JSON config file.
    send_email = False
    json_utils = pbslurmusertools.pbsut_utils.PBSUTJSONParseHelper()

    with open(config_file) as f:
        config_data = json.load(f)
        vals_dict = json_utils.getValueDict(config_data, ["pbslurmusertools", "sbatch"])
        if "email_address" in vals_dict:
            send_email = True

    create_srun_gnuparallel_cmds_file(input_file, out_cmds_file)
    # Read jinja2 template

    if custom_template is not None:
        search_path, template_name = os.path.split(custom_template)
        template_loader = jinja2.FileSystemLoader(searchpath=search_path)
    else:
        template_loader = jinja2.PackageLoader('pbslurmusertools')
        if send_email:
            template_name = 'sbatchsub_gnuparallel_email.jinja2'
        else:
            template_name = 'sbatchsub_gnuparallel.jinja2'

    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template(template_name)

    vals_dict['cmds_file'] = input_file

    output_text = template.render(vals_dict)

    with open(output_sbatch_file, 'w') as out_file:
        out_file.write(output_text + '\n')
        out_file.flush()
        out_file.close()


def get_gnuparallel_multi_sbatch(config_file, input_file_lst, out_cmds_base_file, output_base_file, custom_template):
    """
    A function which iterates through a list of files creating output files to
    be executed using sbatch.

    :param config_file:
    :param input_file_lst:
    :param out_cmds_base_file:
    :param output_base_file:
    :return:
    """
    out_cmds_file_base, out_cmds_file_ext = os.path.splitext(out_cmds_base_file)
    output_file_base, output_file_ext = os.path.splitext(output_base_file)

    sbatch_cmds = []
    i = 1
    for input_file in input_file_lst:
        out_cmds_file = '{0}_{1}{2}'.format(out_cmds_file_base, i, out_cmds_file_ext)
        output_sbatch_file = '{0}_{1}{2}'.format(output_file_base, i, output_file_ext)
        get_gnuparallel_single_sbatch(config_file, input_file, out_cmds_file, output_sbatch_file, custom_template)
        sbatch_cmds.append('sbatch {}'.format(output_sbatch_file))
        i = i + 1

    output_sbatch_file = '{0}_{1}{2}'.format(output_file_base, 'runall', output_file_ext)
    pbs_txt_utils = pbslurmusertools.pbsut_utils.PBSUTTextFileUtils()
    pbs_txt_utils.writeList2File(sbatch_cmds, output_sbatch_file)
