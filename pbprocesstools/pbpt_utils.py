#!/usr/bin/env python
"""
pb_process_tools - Utilities used within the PB Process Tools system.

Most file, text, JSON, string, and dictionary helpers delegate directly to
the ``rsgispy.tools`` package to avoid code duplication while preserving the
existing ``pbprocesstools.pbpt_utils`` class interfaces.
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
# Purpose:  Utilities used within the PB Process Tools System.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 12/11/2018
# Version: 4.0.2

import logging
import os

import rsgispy
import rsgispy.tools.dicts
import rsgispy.tools.files
import rsgispy.tools.jsonio
import rsgispy.tools.strings
import rsgispy.tools.textio

logger = logging.getLogger(__name__)


class PBPTUtils:
    """General file locking, text/JSON I/O, and string utilities backed by rsgispy."""

    # pylint: disable=invalid-name

    def get_file_lock(
        self, input_file, sleep_period=1, wait_iters=120, use_except=False, timeout=3600
    ):
        """
        Acquire a lock file alongside ``input_file`` using ``rsgispy.tools.files``.

        If the lock cannot be acquired within ``wait_iters``, stale locks older than
        ``timeout`` seconds are cleaned up and acquisition is retried once.

        :param input_file: The input file for which the lock will be created.
        :param sleep_period: Time in seconds to sleep between retries (Default=1).
        :param wait_iters: Number of retry iterations before giving up (Default=120).
        :param use_except: If True, raise an exception if the lock cannot be gained.
        :param timeout: Lock expiry threshold in seconds (Default=3600).
        :return: True if the lock was acquired, False otherwise.
        """
        got_lock = rsgispy.tools.files.get_file_lock(
            input_file,
            sleep_period=sleep_period,
            wait_iters=wait_iters,
            use_except=False,
        )
        if not got_lock:
            file_path, _ = os.path.split(input_file)
            self.clean_file_locks(file_path or ".", timeout)
            got_lock = rsgispy.tools.files.get_file_lock(
                input_file,
                sleep_period=0,
                wait_iters=0,
                use_except=use_except,
            )
        return got_lock

    def release_file_lock(self, input_file, timeout=3600):
        """
        Release the lock file for ``input_file``.

        If removing the lock file fails, ``clean_file_locks`` is called as a fallback.

        :param input_file: The input file whose lock file will be released.
        :param timeout: Lock expiry threshold in seconds (Default=3600).
        """
        file_path, _ = os.path.split(input_file)
        dir_path = file_path or "."
        try:
            rsgispy.tools.files.release_file_lock(input_file)
        except (OSError, rsgispy.RSGISPyException):
            logger.debug("Failed to remove lock file so calling clean_file_locks.")
            self.clean_file_locks(dir_path, timeout)

    def clean_file_locks(self, dir_path, timeout=3600):
        """
        Clean up stale ``.*.lok`` lock files older than ``timeout`` seconds.

        :param dir_path: Directory path to search for lock files.
        :param timeout: Lock expiry threshold in seconds (Default=3600).
        """
        rsgispy.tools.files.clean_file_locks(dir_path or ".", timeout=timeout)

    def readTextFileNoNewLines(self, file):
        """
        Read a text file into a single string with newlines and line whitespace stripped.

        :param file: File path to the input file.
        :return: Concatenated file content string.
        """
        return rsgispy.tools.textio.read_text_file_no_new_lines(file)

    def readTextFile2List(self, file):
        """
        Read a text file into a list of non-empty stripped lines.

        :param file: File path to the input file.
        :return: List of line strings.
        """
        return rsgispy.tools.textio.read_text_file_to_list(file)

    def writeList2File(self, dataList, outFile):
        """
        Write a list to a text file, one item per line.

        :param dataList: List of values to write to the output file.
        :param outFile: File path to the output file.
        """
        rsgispy.tools.textio.write_list_to_file(dataList, outFile)

    def writeData2File(self, data_val, out_file):
        """
        Write a value (converted via ``str``) followed by a newline to a text file.

        :param data_val: Data to write to the output file.
        :param out_file: File path to the output file.
        """
        rsgispy.tools.textio.write_data_to_file(data_val, out_file)

    def writeDict2JSON(self, data_dict, out_file):
        """
        Write a dictionary or list to a pretty-printed JSON file.

        :param data_dict: The dict (or list) to write to the output JSON file.
        :param out_file: File path to the output file.
        """
        rsgispy.tools.jsonio.write_dict_to_json(data_dict, out_file)

    def readJSON2Dict(self, input_file):
        """
        Read a JSON file and return the parsed dict or list.

        :param input_file: Input JSON file path.
        :return: Parsed JSON data structure.
        """
        return rsgispy.tools.jsonio.read_json_to_dict(input_file)

    def check_str(
        self,
        str_val,
        rm_non_ascii=False,
        rm_dashs=False,
        rm_spaces=False,
        rm_punc=False,
    ):
        """
        Sanitize a string by optionally stripping non-ASCII characters, converting
        dashes/spaces to underscores, and removing punctuation.

        :param str_val: Input string to process.
        :param rm_non_ascii: If True, remove non-ASCII characters.
        :param rm_dashs: If True, replace dashes with underscores.
        :param rm_spaces: If True, replace spaces with underscores.
        :param rm_punc: If True, remove punctuation other than ``_`` and ``-``.
        :return: Cleaned string.
        """
        return rsgispy.tools.strings.check_str(
            str_val,
            rm_non_ascii=rm_non_ascii,
            rm_dashs=rm_dashs,
            rm_spaces=rm_spaces,
            rm_punc=rm_punc,
        )

    def remove_repeated_chars(self, str_val, repeat_char):
        """
        Collapse consecutive occurrences of ``repeat_char`` in ``str_val``.

        :param str_val: Input string.
        :param repeat_char: Single character to deduplicate.
        :return: String without consecutive repeats of ``repeat_char``.
        """
        return rsgispy.tools.strings.remove_repeated_chars(str_val, repeat_char)

    def uidGenerator(self, size=6):
        """
        Generate a random hexadecimal UID string of the specified length.

        :param size: Length of the returned string.
        :return: Random UID string.
        """
        return rsgispy.tools.strings.uid_generator(size=size)


class PBPTTextFileUtils:
    """Text file list I/O helpers backed by ``rsgispy.tools.textio``."""

    # pylint: disable=invalid-name

    def readTextFile2List(self, file):
        """Read a text file into a list of non-empty stripped lines."""
        return rsgispy.tools.textio.read_text_file_to_list(file)

    def writeList2File(self, data_list, out_file):
        """Write a list to a text file, one item per line."""
        rsgispy.tools.textio.write_list_to_file(data_list, out_file)


class PBPTJSONParseHelper:
    """Gzipped JSON I/O and nested dict lookup helpers backed by ``rsgispy.tools``."""

    # pylint: disable=invalid-name

    def readGZIPJSON(self, file_path):
        """Read a gzipped JSON file and return the parsed data structure."""
        return rsgispy.tools.jsonio.read_gz_json_to_dict(file_path)

    def writeGZIPJSON(self, data, file_path):
        """Write a data structure to a gzipped JSON file."""
        rsgispy.tools.jsonio.write_dict_to_json_gz(data, file_path)

    def doesPathExist(self, json_obj, tree_sequence):
        """Test whether a key path exists within a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_does_path_exist(
            json_obj, tree_sequence
        )

    def getValueDict(self, json_obj, tree_sequence):
        """Retrieve a nested dictionary value at ``tree_sequence``."""
        curr_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = f"{steps_str}:{tree_step}"
            if isinstance(curr_obj, dict) and (tree_step in curr_obj):
                curr_obj = curr_obj[tree_step]
            else:
                raise KeyError(f"Could not find '{steps_str}'")
        return dict(curr_obj)

    def getStrValue(self, json_obj, tree_sequence, valid_values=None):
        """Retrieve a string value from a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_get_str_value(
            json_obj, tree_sequence, valid_values=valid_values
        )

    def getBooleanValue(self, json_obj, tree_sequence):
        """Retrieve a boolean value from a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_get_boolean_value(
            json_obj, tree_sequence
        )

    def getDateValue(self, json_obj, tree_sequence, date_format="%Y-%m-%d"):
        """Retrieve a ``datetime.date`` value from a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_get_date_value(
            json_obj, tree_sequence, date_format=date_format
        )

    def getDateTimeValue(self, json_obj, tree_sequence, date_time_format="%Y-%m-%d"):
        """Retrieve a ``datetime.datetime`` value from a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_get_datetime_value(
            json_obj, tree_sequence, date_time_format=date_time_format
        )

    def getStrListValue(self, json_obj, tree_sequence, valid_values=None):
        """Retrieve a list of string values from a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_get_str_list_value(
            json_obj, tree_sequence, valid_values=valid_values
        )

    def getNumericValue(
        self, json_obj, tree_sequence, valid_lower=None, valid_upper=None
    ):
        """Retrieve a numeric value from a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_get_numeric_value(
            json_obj,
            tree_sequence,
            valid_lower=valid_lower,
            valid_upper=valid_upper,
        )

    def getListValue(self, json_obj, tree_sequence):
        """Retrieve a list value from a nested dictionary structure."""
        return rsgispy.tools.dicts.dict_struct_get_list_value(
            json_obj, tree_sequence
        )
