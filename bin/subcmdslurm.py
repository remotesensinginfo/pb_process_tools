#!/usr/bin/env python
"""
pb_process_tools - Create slurm submission script for a single command.
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
# Date: 12/12/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import logging

import pbprocesstools.pbpt_sbatch

logger = logging.getLogger('subcmdslurm.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, required=True, help="Path to the JSON config file.")
    parser.add_argument("--cmd", type=str, required=True, help="Specify an input file.")
    parser.add_argument("-o", "--output", type=str, required=True, help="Specify an output file.")
    parser.add_argument("-t", "--template", type=str, help="Optionally provide a custom template file.")

    args = parser.parse_args()

    pbprocesstools.pbpt_sbatch.get_single_sbatch(args.config, args.cmd, args.output, args.template)
