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

import pbprocesstools.pbpt_utils

logger = logging.getLogger(__name__)


def get_single_sbatch(config_file, out_cmd, output_sbatch_file, custom_template):
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
    specify_cores_per_node = False
    json_utils = pbprocesstools.pbpt_utils.PBPTJSONParseHelper()

    with open(config_file) as f:
        config_data = json.load(f)
        vals_dict = json_utils.getValueDict(config_data, ["pbprocesstools", "sbatch"])
        if "email_address" in vals_dict:
            send_email = True
        if "ncores_node" in vals_dict:
            specify_cores_per_node = True

    # Read jinja2 template
    if custom_template is not None:
        search_path, template_name = os.path.split(custom_template)
        template_loader = jinja2.FileSystemLoader(searchpath=search_path)
    else:
        template_loader = jinja2.PackageLoader('pbprocesstools')
        if send_email and specify_cores_per_node:
            template_name = 'sbatchsub_basic_nnodes_email.jinja2'
        elif send_email:
            template_name = 'sbatchsub_basic_email.jinja2'
        elif specify_cores_per_node:
            template_name = 'sbatchsub_basic_nnodes.jinja2'
        else:
            template_name = 'sbatchsub_basic.jinja2'

    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template(template_name)

    vals_dict['cmd'] = out_cmd

    output_text = template.render(vals_dict)

    with open(output_sbatch_file, 'w') as out_file:
        out_file.write(output_text + '\n')
        out_file.flush()
        out_file.close()

def create_srun_gnuparallel_cmds_file(input_file, out_cmds_file, prepend_cmd):
    """

    :param input_file:
    :param out_cmds_file:
    :param prepend_cmd:
    :return:
    """
    pbpt_txt_utils = pbprocesstools.pbpt_utils.PBPTTextFileUtils()
    in_cmds_lst = pbpt_txt_utils.readTextFile2List(input_file)
    out_cmds_lst = []
    for cmd in in_cmds_lst:
        if prepend_cmd is not None:
            out_cmd = "srun -n1 -N1 {0} {1}".format(prepend_cmd, cmd)
        else:
            out_cmd = "srun -n1 -N1 {0}".format(cmd)
        out_cmds_lst.append(out_cmd)
    pbpt_txt_utils.writeList2File(out_cmds_lst, out_cmds_file)


def get_gnuparallel_single_sbatch(config_file, input_file, out_cmds_file, output_sbatch_file,
                                  custom_template, prepend_cmd):
    """
    A function which generates an sbatch submission script where a list of commands
    will be executed using GNU Parallel.

    :param config_file:
    :param input_file:
    :param out_cmds_file:
    :param output_sbatch_file:
    :param prepend_cmd:
    :return:
    """
    # Parse JSON config file.
    send_email = False
    specify_cores_per_node = False
    json_utils = pbprocesstools.pbpt_utils.PBPTJSONParseHelper()

    with open(config_file) as f:
        config_data = json.load(f)
        vals_dict = json_utils.getValueDict(config_data, ["pbprocesstools", "sbatch"])
        if "email_address" in vals_dict:
            send_email = True
        if "ncores_node" in vals_dict:
            specify_cores_per_node = True

    create_srun_gnuparallel_cmds_file(input_file, out_cmds_file, prepend_cmd)

    # Read jinja2 template
    if custom_template is not None:
        search_path, template_name = os.path.split(custom_template)
        template_loader = jinja2.FileSystemLoader(searchpath=search_path)
    else:
        template_loader = jinja2.PackageLoader('pbprocesstools')
        if send_email and specify_cores_per_node:
            template_name = 'sbatchsub_gnuparallel_nnodes_email.jinja2'
        elif send_email:
            template_name = 'sbatchsub_gnuparallel_email.jinja2'
        elif specify_cores_per_node:
            template_name = 'sbatchsub_gnuparallel_nnodes.jinja2'
        else:
            template_name = 'sbatchsub_gnuparallel.jinja2'

    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template(template_name)

    vals_dict['cmds_file'] = out_cmds_file

    output_text = template.render(vals_dict)

    with open(output_sbatch_file, 'w') as out_file:
        out_file.write(output_text + '\n')
        out_file.flush()
        out_file.close()


def get_gnuparallel_multi_sbatch(config_file, input_file_lst, out_cmds_base_file, output_base_file,
                                 custom_template, prepend_cmd):
    """
    A function which iterates through a list of files creating output files to
    be executed using sbatch.

    :param config_file:
    :param input_file_lst:
    :param out_cmds_base_file:
    :param output_base_file:
    :param prepend_cmd:
    :return:
    """
    out_cmds_file_base, out_cmds_file_ext = os.path.splitext(out_cmds_base_file)
    output_file_base, output_file_ext = os.path.splitext(output_base_file)

    sbatch_cmds = []
    i = 1
    for input_file in input_file_lst:
        out_cmds_file = '{0}_{1}{2}'.format(out_cmds_file_base, i, out_cmds_file_ext)
        output_sbatch_file = '{0}_{1}{2}'.format(output_file_base, i, output_file_ext)
        get_gnuparallel_single_sbatch(config_file, input_file, out_cmds_file, output_sbatch_file,
                                      custom_template, prepend_cmd)
        sbatch_cmds.append('sbatch {}'.format(output_sbatch_file))
        i = i + 1

    output_sbatch_file = '{0}_{1}{2}'.format(output_file_base, 'runall', output_file_ext)
    pbpt_txt_utils = pbprocesstools.pbpt_utils.PBPTTextFileUtils()
    pbpt_txt_utils.writeList2File(sbatch_cmds, output_sbatch_file)
