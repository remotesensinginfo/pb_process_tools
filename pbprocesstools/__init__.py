#!/usr/bin/env python
"""
pb_process_tools - this file is needed to ensure it can be imported

See other source files for details
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

PB_PROCESS_TOOLS_VERSION_MAJOR = 0
PB_PROCESS_TOOLS_VERSION_MINOR = 0
PB_PROCESS_TOOLS_VERSION_PATCH = 9

PB_PROCESS_TOOLS_VERSION = str(PB_PROCESS_TOOLS_VERSION_MAJOR) + "."  + str(PB_PROCESS_TOOLS_VERSION_MINOR) + "." + str(PB_PROCESS_TOOLS_VERSION_PATCH)
PB_PROCESS_TOOLS_VERSION_OBJ = LooseVersion(PB_PROCESS_TOOLS_VERSION)

PB_PROCESS_TOOLS_COPYRIGHT_YEAR = "2018"
PB_PROCESS_TOOLS_COPYRIGHT_NAMES = "Pete Bunting"

PB_PROCESS_TOOLS_SUPPORT_EMAIL = "rsgislib-support@googlegroups.com"


log_default_level=logging.INFO

# Get install prefix
install_prefix = __file__[:__file__.find('lib')]

log_config_path = os.path.join(install_prefix, "share","pbprocesstools", "loggingconfig.json")

log_config_value = os.getenv('PBPT_LOG_CFG', None)
if log_config_value:
    log_config_path = log_config_value
if os.path.exists(log_config_path):
    with open(log_config_path, 'rt') as f:
        config = json.load(f)
    logging.config.dictConfig(config)
else:
    print('Warning: did not find a logging configuration file.')
    logging.basicConfig(level=log_default_level)
