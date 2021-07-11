#!/usr/bin/env python
"""
pb_process_tools - this file has classes for batch processing data

See other source files for details
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
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import json
import sys
import pathlib
import os
import shutil
import tqdm
from abc import ABC, abstractmethod


class PBPTProcessToolsBase(ABC):

    def __init__(self, uid_len=6):
        self.uid = self.uid_generator(size=uid_len)
        super().__init__()

    def uid_generator(self, size=6):
        """
        A function which will generate a 'random' string of the specified length based on the UUID

        :param size: the length of the returned string.
        :return: string of length size.

        """
        import uuid
        randomStr = str(uuid.uuid4())
        randomStr = randomStr.replace("-","")
        return randomStr[0:size]

    def generate_readable_timestamp_str(self):
        """
        A function which generates a timestamp string which is suitable
        to include within a file name.

        :return: string.

        """
        from datetime import datetime
        now_time = datetime.now()
        return now_time.strftime("%Y%m%d_%H%M%S")

    def find_file(self, dir_path, file_search):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s). Returns None is not found.

        :param dir_path: the file path(s) to be searched. Can be either a single string or a list of file paths
                         to be searched.
        :param file_search: the file search string.
        :return: a string with the file path found or None is either no file paths are
                 found or multiple paths are found.

        """
        import glob
        import os.path

        # Test whether a string has been inputted. If a string has been
        # provided then make it a list with one element.
        if isinstance(dir_path, str):
            dir_path = [dir_path]

        for c_dir_path in dir_path:
            files = glob.glob(os.path.join(c_dir_path, file_search))
            if len(files) > 0:
                break

        if len(files) != 1:
            return None
        return files[0]

    def find_first_file(self, dirPath, fileSearch, rtn_except=True):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).

        :param dirPath: The directory within which to search, note that the search will be within
                        sub-directories within the base directory until a file meeting the search
                        criteria are met.
        :param fileSearch: The file search string in the file name and must contain a wild character (i.e., *).
        :param rtn_except: If True then an exception will be raised if no file or multiple files are found (default).
                           If False then None will be returned rather than an exception raised.
        :return: The file found (or None if rtn_except=False)

        """
        import glob
        files = None
        for root, dirs, files in os.walk(dirPath):
            files = glob.glob(os.path.join(root, fileSearch))
            if len(files) > 0:
                break
        out_file = None
        if (files is not None) and (len(files) == 1):
            out_file = files[0]
        elif rtn_except:
            raise Exception("Could not find a single file ({0}) in {1}; "
                            "found {2} files.".format(fileSearch, dirPath, len(files)))
        return out_file

    def get_file_basename(self, filepath, checkvalid=False, n_comps=0):
        """
        Uses os.path module to return file basename (i.e., path and extension removed)

        :param filepath: string for the input file name and path
        :param checkvalid: if True then resulting basename will be checked for punctuation
                           characters (other than underscores) and spaces, punctuation
                           will be either removed and spaces changed to an underscore.
                           (Default = False)
        :param n_comps: if > 0 then the resulting basename will be split using underscores
                        and the return based name will be defined using the n_comps
                        components split by under scores.
        :return: basename for file

        """
        import string
        basename = os.path.splitext(os.path.basename(filepath))[0]
        if checkvalid:
            basename = basename.replace(' ', '_')
            for punct in string.punctuation:
                if (punct != '_') and (punct != '-'):
                    basename = basename.replace(punct, '')
        if n_comps > 0:
            basename_split = basename.split('_')
            if len(basename_split) < n_comps:
                raise Exception("The number of components specified is more than "
                                "the number of components in the basename.")
            out_basename = ""
            for i in range(n_comps):
                if i == 0:
                    out_basename = basename_split[i]
                else:
                    out_basename = out_basename + '_' + basename_split[i]
            basename = out_basename
        return basename

    def create_dir(self, dir_path, use_abs_path=True, add_uid=False):
        """
        A function which creates a temporary directory and optionally adds
        a unique ID to the last directory within the path so each time the
        process executes a unique temporary is created.

        :param dir_path: an input string with the directory path to be created.
        :param use_abs_path: a boolean specifying whether the input path should be
                             converted to an absolute path if a relative path has been provided.
                             The default is True.
        :param add_uid: A boolean specifying whether a UID should be added to the tmp_dir_path
        :return: (str, bool) returns the path of the temporary directory created and boolean as to whether the
                directory was created or already existed (True = created).

        """
        dir_path_obj = pathlib.Path(dir_path)
        if use_abs_path:
            dir_path_obj = dir_path_obj.resolve()

        if add_uid:
            last_dir_name = dir_path_obj.parent.name
            last_dir_name_uid = "{}_{}".format(last_dir_name, self.uid)
            dir_path_obj = dir_path_obj.parent.joinpath(pathlib.Path(last_dir_name_uid))

        created = False
        if not dir_path_obj.exists():
            dir_path_obj.mkdir()
            created = True
        return str(dir_path_obj), created

    def remove_dir(self, dir_path):
        """
        A function which deletes the in_dir and its contents.

        :param dir_path: an input string with the directory path to be removed.

        """
        dir_path_obj = pathlib.Path(dir_path)
        if dir_path_obj.exists():
            shutil.rmtree(dir_path, ignore_errors=True)

    def check_hdf5_file(self, h5_file):
        """
        A function which checks a HDF5 file and returns an error message if appropriate.

        :param h5_file: the file path to the HDF5 file.
        :return: boolean (True: file OK; False: Error found), string (error message if required otherwise empty string)

        """
        file_ok = True
        err_str = ''
        if os.path.exists(h5_file):
            try:
                import h5py
                try:
                    fH5 = h5py.File(h5_file, 'r')
                    if fH5 is None:
                        file_ok = False
                        err_str = 'h5py could not open the dataset as returned a Null dataset.'
                except:
                    file_ok = False
                    err_str = 'h5py could not open the dataset.'
            except Exception as e:
                file_ok = False
                err_str = str(e)
        else:
            file_ok = False
            err_str = 'File does not exist.'
        return file_ok, err_str

    def check_gdal_image_file(self, gdal_img, check_bands=True):
        """
        A function which checks a GDAL compatible image file and returns an error message if appropriate.

        :param gdal_img: the file path to the gdal image file.
        :param check_bands: boolean specifying whether individual image bands should be
                            opened and checked (Default: True)
        :return: boolean (True: file OK; False: Error found), string (error message if required otherwise empty string)

        """
        file_ok = True
        err_str = ''
        if os.path.exists(gdal_img):
            from osgeo import gdal
            from pbprocesstools.pbpt_utils import PBPTGDALErrorHandler

            err = PBPTGDALErrorHandler()
            err_handler = err.handler
            gdal.PushErrorHandler(err_handler)
            gdal.UseExceptions()

            try:
                raster_ds = gdal.Open(gdal_img, gdal.GA_ReadOnly)
                if raster_ds is None:
                    file_ok = False
                    err_str = 'GDAL could not open the dataset, returned None.'
                elif check_bands:
                    n_bands = raster_ds.RasterCount
                    for n in range(n_bands):
                        band = n + 1
                        img_band = raster_ds.GetRasterBand(band)
                        if img_band is None:
                            file_ok = False
                            err_str = 'GDAL could not open band {} in the dataset, returned None.'.format(band)
                raster_ds = None
            except Exception as e:
                file_ok = False
                err_str = str(e)
            else:
                if err.err_level >= gdal.CE_Warning:
                    file_ok = False
                    err_str = str(err.err_msg)
            finally:
                gdal.PopErrorHandler()
        else:
            file_ok = False
            err_str = 'File does not exist.'
        return file_ok, err_str

    def check_gdal_vector_file(self, gdal_vec):
        """
        A function which checks a GDAL compatible vector file and returns an error message if appropriate.

        :param gdal_vec: the file path to the gdal vector file.
        :return: boolean (True: file OK; False: Error found), string (error message if required otherwise empty string)

        """
        file_ok = True
        err_str = ''
        if os.path.exists(gdal_vec):
            from osgeo import gdal
            from pbprocesstools.pbpt_utils import PBPTGDALErrorHandler

            err = PBPTGDALErrorHandler()
            err_handler = err.handler
            gdal.PushErrorHandler(err_handler)
            gdal.UseExceptions()

            try:
                vec_ds = gdal.OpenEx(gdal_vec, gdal.OF_VECTOR )
                if vec_ds is None:
                    file_ok = False
                    err_str = 'GDAL could not open the data source, returned None.'
                else:
                    for lyr_idx in range(vec_ds.GetLayerCount()):
                        vec_lyr = vec_ds.GetLayerByIndex(lyr_idx)
                        if vec_lyr is None:
                            file_ok = False
                            err_str = 'GDAL could not open all the vector layers.'
                            break
                vec_ds = None
            except Exception as e:
                file_ok = False
                err_str = str(e)
            else:
                if err.err_level >= gdal.CE_Warning:
                    file_ok = False
                    err_str = str(err.err_msg)
            finally:
                gdal.PopErrorHandler()
        else:
            file_ok = False
            err_str = 'File does not exist.'
        return file_ok, err_str



    def check_files(self, files_dict):
        """
        A function which test whether the files listed are present and for some formats
        will perform a check the file can be read.

        At present the format options are:

         * gdal_image
         * gdal_vector
         * hdf5
         * file - just does a check whether the file is present.

        :param files_dict: dict with the structure key: filepath, value:format

        :return: tuple (boolean, dict with outputs as keys and error message as the value)

        """
        files_present = True
        errs_dict = dict()
        for filepath in files_dict:
            if not os.path.exists(filepath):
                files_present = False
                errs_dict[filepath] = "Does not exist in file system."
            else:
                if files_dict[filepath].lower() == 'gdal_image':
                    # Test GDAL image
                    file_ok, err_str = self.check_gdal_image_file(filepath)
                    if not file_ok:
                        files_present = False
                        errs_dict[filepath] = err_str
                elif files_dict[filepath].lower() == 'gdal_vector':
                    # Test GDAL vector
                    file_ok, err_str = self.check_gdal_vector_file(filepath)
                    if not file_ok:
                        files_present = False
                        errs_dict[filepath] = err_str
                elif files_dict[filepath].lower() == 'hdf5':
                    # Test HDF5 file
                    file_ok, err_str = self.check_hdf5_file(filepath)
                    if not file_ok:
                        files_present = False
                        errs_dict[filepath] = err_str
                elif files_dict[filepath].lower() == 'filesize':
                    file_ok = False
                    err_str = ""
                    file_size = os.stat(filepath).st_size
                    if not (file_size > 0):
                        files_present = False
                        errs_dict[filepath] = "File does not have a file size > 0."
                # Else: just ignore, no test being undertaken - i.e., being present is enough...
        return files_present, errs_dict
