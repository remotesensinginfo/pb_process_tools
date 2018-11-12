#!/usr/bin/env python
"""
pb_slurm_user_tools - this file is needed to ensure it can be imported

See other source files for details
"""
# This file is part of 'pb_slurm_user_tools'
# A set of utilties for working with slurm.
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
# Date: 12/11/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

from distutils.version import LooseVersion
import os
import logging
import logging.config
import json

PB_SLURM_USER_TOOLS_VERSION_MAJOR = 0
PB_SLURM_USER_TOOLS_VERSION_MINOR = 6
PB_SLURM_USER_TOOLS_VERSION_PATCH = 0

PB_SLURM_USER_TOOLS_VERSION = str(PB_SLURM_USER_TOOLS_VERSION_MAJOR) + "."  + str(PB_SLURM_USER_TOOLS_VERSION_MINOR) + "." + str(PB_SLURM_USER_TOOLS_VERSION_PATCH)
PB_SLURM_USER_TOOLS_VERSION_OBJ = LooseVersion(PB_SLURM_USER_TOOLS_VERSION)

PB_SLURM_USER_TOOLS_COPYRIGHT_YEAR = "2018"
PB_SLURM_USER_TOOLS_COPYRIGHT_NAMES = "Pete Bunting"

PB_SLURM_USER_TOOLS_SUPPORT_EMAIL = "rsgislib-support@googlegroups.com"


log_default_level=logging.INFO

log_config_value = os.getenv('PBSUT_LOG_CFG', None)
if log_config_value:
    log_config_path = log_config_value
if os.path.exists(log_config_path):
    with open(log_config_path, 'rt') as f:
        config = json.load(f)
    logging.config.dictConfig(config)
else:
    print('Warning: did not find the logging configuration file.')
    logging.basicConfig(level=log_default_level)
