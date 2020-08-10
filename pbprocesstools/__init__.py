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
import sys
import logging
import logging.config
import json

PB_PROCESS_TOOLS_VERSION_MAJOR = 1
PB_PROCESS_TOOLS_VERSION_MINOR = 4
PB_PROCESS_TOOLS_VERSION_PATCH = 0

PB_PROCESS_TOOLS_VERSION = "{}.{}.{}".format(PB_PROCESS_TOOLS_VERSION_MAJOR,
                                             PB_PROCESS_TOOLS_VERSION_MINOR,
                                             PB_PROCESS_TOOLS_VERSION_PATCH)
PB_PROCESS_TOOLS_VERSION_OBJ = LooseVersion(PB_PROCESS_TOOLS_VERSION)

py_sys_version = sys.version_info
py_sys_version_str = "{}.{}".format(py_sys_version.major, py_sys_version.minor)
py_sys_version_flt = float(py_sys_version_str)

PB_PROCESS_TOOLS_COPYRIGHT_YEAR = "2018"
PB_PROCESS_TOOLS_COPYRIGHT_NAMES = "Pete Bunting"

PB_PROCESS_TOOLS_SUPPORT_EMAIL = "rsgislib-support@googlegroups.com"

pbpt_log_level = os.getenv('PBTP_LOG_LVL', 'INFO')

log_default_level=logging.INFO
if pbpt_log_level.upper() == 'INFO':
    log_default_level = logging.INFO
elif pbpt_log_level.upper() == 'DEBUG':
    log_default_level = logging.DEBUG
elif pbpt_log_level.upper() == 'WARNING':
    log_default_level = logging.WARNING
elif pbpt_log_level.upper() == 'ERROR':
    log_default_level = logging.ERROR
elif pbpt_log_level.upper() == 'CRITICAL':
    log_default_level = logging.CRITICAL
else:
    raise Exception("Logging level specified ('{}') is not recognised.".format(pbpt_log_level))

log_config_path = os.getenv('PBPT_LOG_CFG', None)
if (log_config_path is not None) and os.path.exists(log_config_path):
    with open(log_config_path, 'rt') as f:
        config = json.load(f)
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=log_default_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
