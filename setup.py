#!/usr/bin/env python
"""
Setup script for pb_process_tools. Use like this for Unix:

$ python setup.py install

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
# Purpose:  install software.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 14/11/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import setuptools
import os

setuptools.setup(name='pb_process_tools',
    version='3.1.3',
    description='Tools for batch processing data, including on HPC cluster with slurm.',
    author='Pete Bunting',
    author_email='petebunting@mac.com',
    include_package_data=True,
    packages=['pbprocesstools'],
    package_dir={'pbprocesstools': 'pbprocesstools'},
    data_files=[(os.path.join('share','pbprocesstools'),
                [os.path.join('share','loggingconfig.json')])],
    license='LICENSE.txt',
    install_requires=['sqlalchemy', 'tqdm'],
    url='https://github.com/remotesensinginfo/pb_process_tools',
    classifiers=['Intended Audience :: Developers',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: 3.5',
                 'Programming Language :: Python :: 3.6',
                 'Programming Language :: Python :: 3.7',
                 'Programming Language :: Python :: 3.8',
                 'Programming Language :: Python :: 3.9',
                 'Programming Language :: Python :: 3.10'])
