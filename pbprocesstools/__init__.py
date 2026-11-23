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
# Version: 4.0.2

import json
import logging
import logging.config
import os
import sys

PB_PROCESS_TOOLS_VERSION_MAJOR = 4
PB_PROCESS_TOOLS_VERSION_MINOR = 0
PB_PROCESS_TOOLS_VERSION_PATCH = 2

PB_PROCESS_TOOLS_VERSION = (
    f"{PB_PROCESS_TOOLS_VERSION_MAJOR}."
    f"{PB_PROCESS_TOOLS_VERSION_MINOR}."
    f"{PB_PROCESS_TOOLS_VERSION_PATCH}"
)
PB_PROCESS_TOOLS_VERSION_INFO = (
    PB_PROCESS_TOOLS_VERSION_MAJOR,
    PB_PROCESS_TOOLS_VERSION_MINOR,
    PB_PROCESS_TOOLS_VERSION_PATCH,
)
PB_PROCESS_TOOLS_VERSION_OBJ = PB_PROCESS_TOOLS_VERSION_INFO
__version__ = PB_PROCESS_TOOLS_VERSION

py_sys_version = sys.version_info
py_sys_version_str = f"{py_sys_version.major}.{py_sys_version.minor}"
py_sys_version_flt = float(py_sys_version_str)

PB_PROCESS_TOOLS_COPYRIGHT_YEAR = "2018"
PB_PROCESS_TOOLS_COPYRIGHT_NAMES = "Pete Bunting"

PB_PROCESS_TOOLS_SUPPORT_EMAIL = "petebunting@mac.com"

_LOG_LEVEL_MAP = {
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

pbpt_log_level = os.getenv("PBPT_LOG_LVL") or os.getenv("PBTP_LOG_LVL") or "INFO"
if pbpt_log_level.upper() not in _LOG_LEVEL_MAP:
    raise ValueError(
        f"Logging level specified ('{pbpt_log_level}') is not recognised."
    )
log_default_level = _LOG_LEVEL_MAP[pbpt_log_level.upper()]

log_config_path = os.getenv("PBPT_LOG_CFG", None)
if (log_config_path is not None) and os.path.exists(log_config_path):
    with open(log_config_path, "rt", encoding="utf-8") as f:
        config = json.load(f)
    logging.config.dictConfig(config)
else:
    logging.getLogger(__name__).addHandler(logging.NullHandler())


def configure_logging(level=None):
    """Configure root logging for CLI invocations if no handlers are registered."""
    if level is None:
        level = log_default_level
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
