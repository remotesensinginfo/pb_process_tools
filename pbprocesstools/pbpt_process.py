#!/usr/bin/env python
"""
pb_process_tools - Base utilities for batch processing data.
"""
# This file is part of 'pb_process_tools'
# A set of utilities for batch processing data
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
# Purpose:  Tool for batch processing data.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 25/03/2020
# Version: 4.0.2

import importlib
import os
import pathlib
import shutil
from abc import ABC
from datetime import datetime

import rsgispy.tools.files
import rsgispy.tools.strings


class PBPTProcessToolsBase(ABC):
    """Base helper class providing UID, directory, file-search, and output checks."""

    def __init__(self, uid_len=6):
        self.uid = self.uid_generator(size=uid_len)
        super().__init__()

    def uid_generator(self, size=6):
        """
        Generate a random hexadecimal string of the specified length based on a UUID.

        :param size: The length of the returned string.
        :return: String of length ``size``.
        """
        return rsgispy.tools.strings.uid_generator(size=size)

    def generate_readable_timestamp_str(self):
        """
        Generate a timestamp string suitable for inclusion within a file name.

        :return: Timestamp string formatted as ``YYYYMMDD_HHMMSS``.
        """
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def find_file(self, dir_path, file_search):
        """
        Search for a single file matching ``file_search`` in ``dir_path``.

        :param dir_path: Directory path or iterable of directory paths to search.
        :param file_search: Glob search pattern (with ``*`` wildcards).
        :return: Single matching file path, or ``None`` if zero or multiple files match.
        """
        if isinstance(dir_path, str):
            return rsgispy.tools.files.find_file_none(dir_path, file_search)

        for c_dir_path in dir_path:
            found = rsgispy.tools.files.find_file_none(c_dir_path, file_search)
            if found is not None:
                return found
        return None

    def find_first_file(self, dir_path, file_search, rtn_except=True):
        """
        Walk ``dir_path`` recursively and return the first single glob match.

        :param dir_path: Base directory path to search.
        :param file_search: Filename glob pattern (e.g., ``'*.tif'``).
        :param rtn_except: If True (default), raise an exception when zero or multiple
                           files match; otherwise return ``None``.
        :return: Matched file path, or ``None`` if ``rtn_except=False``.
        """
        return rsgispy.tools.files.find_first_file(
            dir_path, file_search, rtn_except=rtn_except
        )

    def get_file_basename(
        self, file_path, check_valid=False, n_comps=0, rm_n_exts=1
    ):
        """
        Return the basename of ``file_path`` with directory path and extension(s) removed.

        :param file_path: Input file path.
        :param check_valid: If True, replace spaces with underscores and strip
                            punctuation other than ``_`` and ``-`` (Default = False).
        :param n_comps: If > 0, split the basename on ``_`` and keep the first
                        ``n_comps`` components (Default = 0).
        :param rm_n_exts: Number of dot-separated extensions to strip (Default = 1;
                          pass 0 to strip all extensions).
        :return: Extracted basename string.
        """
        return rsgispy.tools.files.get_file_basename(
            file_path,
            check_valid=check_valid,
            n_comps=n_comps,
            rm_n_exts=rm_n_exts,
        )

    def create_dir(self, dir_path, use_abs_path=True, add_uid=False):
        """
        Create a directory and optionally append this instance's UID to its name.

        :param dir_path: Directory path to create.
        :param use_abs_path: If True (default), resolve relative paths to absolute paths.
        :param add_uid: If True, append ``_<uid>`` to the final directory name.
        :return: Tuple of ``(dir_path_str, created_bool)``.
        """
        dir_path_obj = pathlib.Path(dir_path)
        if use_abs_path:
            dir_path_obj = dir_path_obj.resolve()

        if add_uid:
            last_dir_name_uid = f"{dir_path_obj.name}_{self.uid}"
            dir_path_obj = dir_path_obj.parent.joinpath(last_dir_name_uid)

        created = False
        if not dir_path_obj.exists():
            dir_path_obj.mkdir(parents=True, exist_ok=True)
            created = True
        return str(dir_path_obj), created

    def remove_dir(self, dir_path):
        """
        Delete ``dir_path`` and all of its contents if it exists.

        :param dir_path: Directory path to remove.
        """
        dir_path_obj = pathlib.Path(dir_path)
        if dir_path_obj.exists():
            shutil.rmtree(dir_path, ignore_errors=True)

    @staticmethod
    def has_files(dir_path: str, recursive: bool = False) -> bool:
        """
        Check if the directory contains at least one regular file.

        :param dir_path: Path to the directory to inspect.
        :param recursive: If True, searches subdirectories as well.
        :return: True if at least one file is found, False otherwise.
        """
        path = pathlib.Path(dir_path)

        if not path.is_dir():
            raise NotADirectoryError(
                f"The path '{dir_path}' is not a valid directory."
            )

        iterator = path.rglob("*") if recursive else path.iterdir()
        return any(item.is_file() for item in iterator)

    @staticmethod
    def _import_checkdatasets(file_type: str):
        try:
            return importlib.import_module("rsgispy.tools.checkdatasets")
        except ImportError as err:
            raise ImportError(
                f"To check a '{file_type}' file is valid, rsgispy.tools.checkdatasets "
                f"must be available."
            ) from err

    @classmethod
    def _check_gdal_image(cls, filepath: str, file_spec, dict_params: bool):
        chk_mod = cls._import_checkdatasets("gdal_image")
        if dict_params:
            chk_proj = file_spec.get("chk_proj", False)
            epsg_code = file_spec.get("epsg_code", 0) if chk_proj else 0
            read_img = file_spec.get("read_img", False)
            smpl_n_pxls = file_spec.get("smpl_n_pxls", 10) if read_img else 10
            file_ok = chk_mod.run_check_gdal_image_file(
                filepath,
                check_bands=True,
                n_bands=file_spec.get("n_bands", 0),
                chk_proj=chk_proj,
                epsg_code=epsg_code,
                read_img=read_img,
                smpl_n_pxls=smpl_n_pxls,
                calc_chk_sum=file_spec.get("calc_chk_sum", False),
                max_file_size=file_spec.get("max_file_size", None),
                rm_err=False,
                print_err=False,
                timeout=file_spec.get("timeout", 4),
            )
        else:
            file_ok = chk_mod.run_check_gdal_image_file(
                filepath,
                check_bands=True,
                rm_err=False,
                print_err=False,
            )
        return None if file_ok else f"Error with GDAL Image File: {filepath}"

    @classmethod
    def _check_gdal_vector(cls, filepath: str, file_spec, dict_params: bool):
        chk_mod = cls._import_checkdatasets("gdal_vector")
        if dict_params:
            chk_proj = file_spec.get("chk_proj", False)
            epsg_code = file_spec.get("epsg_code", 0) if chk_proj else 0
            file_ok = chk_mod.run_check_gdal_vector_file(
                filepath,
                chk_proj=chk_proj,
                epsg_code=epsg_code,
                max_file_size=file_spec.get("max_file_size", None),
                rm_err=False,
                print_err=False,
                multi_file=False,
                timeout=file_spec.get("timeout", 4),
            )
        else:
            file_ok = chk_mod.run_check_gdal_vector_file(
                filepath, rm_err=False, print_err=False
            )
        return None if file_ok else f"Error with GDAL Vector File: {filepath}"

    @classmethod
    def _check_hdf5(cls, filepath: str, file_spec, dict_params: bool):
        chk_mod = cls._import_checkdatasets("hdf5")
        timeout = file_spec.get("timeout", 4) if dict_params else 4
        file_ok = chk_mod.run_check_hdf5_file(
            filepath, rm_err=False, print_err=False, timeout=timeout
        )
        return None if file_ok else f"Error with HDF5 File: {filepath}"

    @staticmethod
    def _check_filesize(filepath: str):
        if not os.path.isfile(filepath):
            return "Path is not a regular file."
        if os.stat(filepath).st_size <= 0:
            return "File does not have a file size > 0."
        return None

    def check_files(self, files_dict):
        """
        Test whether the listed files are present and, for supported formats,
        verify that each file can be opened and read.

        Supported format options:

         * ``gdal_image`` - validated via ``rsgispy.tools.checkdatasets``
         * ``gdal_vector`` - validated via ``rsgispy.tools.checkdatasets``
         * ``hdf5`` - validated via ``rsgispy.tools.checkdatasets``
         * ``filesize`` - checks that the path is a regular file with size > 0
         * ``file`` - checks only that the path exists

        :param files_dict: Mapping of ``filepath -> format_str`` or
                           ``filepath -> {"type": format_str, ...}``.
        :return: Tuple of ``(all_ok_bool, errors_dict)``.
        """
        files_present = True
        errs_dict = {}
        for filepath, file_spec in files_dict.items():
            if not os.path.exists(filepath):
                files_present = False
                errs_dict[filepath] = "Does not exist in file system."
                continue

            dict_params = False
            file_type = "file"
            if isinstance(file_spec, dict):
                if "type" not in file_spec:
                    raise ValueError(
                        "If check_files is provided a dict of file info "
                        "to check, the 'type' key must be provided."
                    )
                file_type = file_spec["type"].lower()
                dict_params = True
            elif isinstance(file_spec, str):
                file_type = file_spec.lower()

            err_msg = None
            if file_type == "gdal_image":
                err_msg = self._check_gdal_image(filepath, file_spec, dict_params)
            elif file_type == "gdal_vector":
                err_msg = self._check_gdal_vector(filepath, file_spec, dict_params)
            elif file_type == "hdf5":
                err_msg = self._check_hdf5(filepath, file_spec, dict_params)
            elif file_type == "filesize":
                err_msg = self._check_filesize(filepath)

            if err_msg is not None:
                files_present = False
                errs_dict[filepath] = err_msg

        return files_present, errs_dict
