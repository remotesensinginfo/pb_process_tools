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
# Purpose:  Utilities used within the PB Process Tools System.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 12/11/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import datetime
import glob
import gzip
import json
import logging
import os
import string
import time
import uuid

logger = logging.getLogger(__name__)


class PBPTUtils(object):
    def get_file_lock(
        self, input_file, sleep_period=1, wait_iters=120, use_except=False, timeout=3600
    ):
        """
        A function which gets a lock on a file using atomic file creation.

        The lock file will be a unix hidden file (i.e., starts with a .) and it will have .lok added to the end.
        E.g., for input file hello_world.txt the lock file will be .hello_world.txt.lok. The contents of the lock
        file will be the time and date of creation.

        Using the default parameters (sleep 1 second and wait 120 iterations) if the lock isn't available
        it will be retried every second for 120 seconds (i.e., 2 mins).

        :param input_file: The input file for which the lock will be created.
        :param sleep_period: time in seconds to sleep for, if the lock isn't available. (Default=1 second)
        :param wait_iters: the number of iterations to wait for before giving up. (Default=120)
        :param use_except: Boolean. If True then an exception will be thrown if the lock is not
                           available. If False (default) False will be returned if the lock is
                           not successful.
        :return: boolean. True: lock was successfully gained. False: lock was not gained.
        :param timeout: the time (in seconds) for the lock file timeout. Default: 3600 (1 hours).

        """
        file_path, file_name = os.path.split(input_file)
        dir_path = file_path if file_path else "."
        lock_file_name = ".{}.lok".format(file_name)
        lock_file_path = os.path.join(dir_path, lock_file_name)

        got_lock = False
        for _ in range(wait_iters + 1):
            try:
                fd = os.open(
                    lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644
                )
                try:
                    c_datetime = datetime.datetime.now()
                    os.write(fd, "{}\n".format(c_datetime.isoformat()).encode("utf-8"))
                finally:
                    os.close(fd)
                got_lock = True
                break
            except FileExistsError:
                time.sleep(sleep_period)

        if not got_lock:
            self.clean_file_locks(dir_path, timeout)
            if use_except:
                raise Exception("Lock could not be gained for file: {}".format(input_file))

        return got_lock

    def release_file_lock(self, input_file, timeout=3600):
        """
        A function which releases a lock file for the input file. If for some reason
        removing the lock file fails then the clean_file_locks is called and the timeout
        variable will be used.

        :param input_file: The input file for which the lock will be created.
        :param timeout: the time (in seconds) for the timeout. Default: 3600 (1 hours).

        """
        file_path, file_name = os.path.split(input_file)
        dir_path = file_path if file_path else "."
        lock_file_name = ".{}.lok".format(file_name)
        lock_file_path = os.path.join(dir_path, lock_file_name)
        if os.path.exists(lock_file_path):
            try:
                os.remove(lock_file_path)
            except FileNotFoundError:
                pass
            except OSError:
                logger.debug("Failed to remove lock file so calling clean_file_locks.")
                self.clean_file_locks(dir_path, timeout)

    def clean_file_locks(self, dir_path, timeout=3600):
        """
        A function which cleans up any remaining lock file (i.e., if an application has crashed).
        The timeout time will be compared with the time written within the file.

        :param dir_path: the file path to search for lock files (i.e., ".*.lok")
        :param timeout: the time (in seconds) for the timeout. Default: 3600 (1 hours)

        """
        if not dir_path:
            dir_path = "."
        c_datetime = datetime.datetime.now()
        lock_files = glob.glob(os.path.join(glob.escape(dir_path), ".*.lok"))
        for lock_file_path in lock_files:
            try:
                with open(lock_file_path, "r") as f:
                    create_date_str = f.read().strip()
                if create_date_str != "":
                    create_date = datetime.datetime.fromisoformat(create_date_str)
                    time_since_create = (c_datetime - create_date).total_seconds()
                    if time_since_create > timeout:
                        os.remove(lock_file_path)
                else:
                    os.remove(lock_file_path)
            except (FileNotFoundError, ValueError, OSError):
                continue

    def readTextFileNoNewLines(self, file):
        """
        Read a text file into a single string
        removing new lines.

        :param file: File path to the input file.
        :return: string

        """
        if not os.path.exists(file):
            raise FileNotFoundError("File does not exist: '{}'".format(file))
        txtStr = ""
        with open(file, "r") as dataFile:
            for line in dataFile:
                txtStr += line.strip()
        return txtStr

    def readTextFile2List(self, file):
        """
        Read a text file into a list where each line
        is an element in the list.

        :param file: File path to the input file.
        :return: list

        """
        outList = []
        with open(file, "r") as dataFile:
            for line in dataFile:
                line = line.strip()
                if line != "":
                    outList.append(line)
        return outList

    def writeList2File(self, dataList, outFile):
        """
        Write a list a text file, one line per item.

        :param dataList: List of values to be written to the output file.
        :param outFile: File path to the output file.

        """
        with open(outFile, "w") as f:
            for item in dataList:
                f.write(str(item) + "\n")

    def writeData2File(self, data_val, out_file):
        """
        Write some data (a string or can be converted to a string using str(data_val) to
        an output text file.

        :param data_val: Data to be written to the output file.
        :param out_file: File path to the output file.

        """
        with open(out_file, "w") as f:
            f.write(str(data_val) + "\n")

    def writeDict2JSON(self, data_dict, out_file):
        """
        Write some data to a JSON file. The data would commonly be structured as a dict but could also be a list.

        :param data_dict: The dict (or list) to be written to the output JSON file.
        :param out_file: The file path to the output file.

        """
        with open(out_file, "w") as fp:
            json.dump(
                data_dict,
                fp,
                sort_keys=True,
                indent=4,
                separators=(",", ": "),
                ensure_ascii=False,
            )

    def readJSON2Dict(self, input_file):
        """
        Read a JSON file. Will return a list or dict.

        :param input_file: input JSON file path.

        """
        with open(input_file) as f:
            data = json.load(f)
        return data

    def check_str(
        self,
        str_val,
        rm_non_ascii=False,
        rm_dashs=False,
        rm_spaces=False,
        rm_punc=False,
    ):
        """
        A function which can check a string removing spaces (replaced with underscores),
        remove punctuation and any non ascii characters.

        :param str_val: the input string to be processed.
        :param rm_non_ascii: If True (default False) remove any non-ascii characters from the string
        :param rm_dashs: If True (default False) remove any dashs from the string and replace with underscores.
        :param rm_spaces: If True (default False) remove any spaces from the string.
        :param rm_punc: If True (default False) remove any punctuation (other than '_' or '-') from the string.
        :return: returns a string outputted from the processing.

        """
        str_val_tmp = str_val.strip()

        if rm_non_ascii:
            allowed_ascii = set(string.ascii_letters + string.digits + string.punctuation + " ")
            str_val_tmp = "".join(c for c in str_val_tmp if c in allowed_ascii)

        if rm_dashs:
            str_val_tmp = str_val_tmp.replace("-", "_")
            str_val_tmp = self.remove_repeated_chars(str_val_tmp, "_")

        if rm_spaces:
            str_val_tmp = str_val_tmp.replace(" ", "_")
            str_val_tmp = self.remove_repeated_chars(str_val_tmp, "_")

        if rm_punc:
            for punct in string.punctuation:
                if (punct != "_") and (punct != "-"):
                    str_val_tmp = str_val_tmp.replace(punct, "")

        return str_val_tmp

    def remove_repeated_chars(self, str_val, repeat_char):
        """
        A function which removes repeated characters within a string for the specified character

        :param str_val: The input string.
        :param repeat_char: The character
        :return: string without repeat_char

        """
        if len(repeat_char) != 1:
            raise Exception("The repeat character has multiple characters.")
        out_str = ""
        p = ""
        for c in str_val:
            if c == repeat_char:
                if c != p:
                    out_str += c
            else:
                out_str += c
            p = c
        return out_str

    def uidGenerator(self, size=6):
        """
        A function which will generate a 'random' string of the specified length based on the UUID

        :param size: the length of the returned string.
        :return: string of length size.

        """
        randomStr = str(uuid.uuid4()).replace("-", "")
        return randomStr[0:size]


class PBPTTextFileUtils(object):
    def readTextFile2List(self, file):
        """
        Read a text file into a list where each line
        is an element in the list.
        :param file:
        :return:
        """
        out_list = []
        with open(file, "r") as dataFile:
            for line in dataFile:
                line = line.strip()
                if line != "":
                    out_list.append(line)
        return out_list

    def writeList2File(self, data_list, out_file):
        """
        Write a list a text file, one line per item.
        :param data_list:
        :param out_file:
        :return:
        """
        with open(out_file, "w") as f:
            for item in data_list:
                f.write(str(item) + "\n")


class PBPTJSONParseHelper(object):
    def readGZIPJSON(self, file_path):
        """
        Function to read a gzipped JSON file returning the data structure produced
        :param file_path:
        :return:
        """
        with gzip.GzipFile(file_path, "r") as fin:
            json_bytes = fin.read()

        json_str = json_bytes.decode("utf-8")
        data = json.loads(json_str)
        return data

    def writeGZIPJSON(self, data, file_path):
        """
        Function to write a gzipped json file.
        :param data:
        :param file_path:
        :return:
        """
        json_str = json.dumps(data) + "\n"
        json_bytes = json_str.encode("utf-8")

        with gzip.GzipFile(file_path, "w") as fout:
            fout.write(json_bytes)

    def doesPathExist(self, json_obj, tree_sequence):
        """
        A function which tests whether a path exists within JSON file.
        :param json_obj:
        :param tree_sequence: list of strings
        :return: boolean
        """
        curr_json_obj = json_obj
        path_exists = True
        for tree_step in tree_sequence:
            if isinstance(curr_json_obj, dict) and (tree_step in curr_json_obj):
                curr_json_obj = curr_json_obj[tree_step]
            else:
                path_exists = False
                break
        return path_exists

    def _traverse(self, json_obj, tree_sequence):
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str + ":" + tree_step
            if isinstance(curr_json_obj, dict) and (tree_step in curr_json_obj):
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))
        return curr_json_obj, steps_str

    def getValueDict(self, json_obj, tree_sequence):
        """

        :param json_obj:
        :param tree_sequence:
        :return:
        """
        curr_json_obj, _ = self._traverse(json_obj, tree_sequence)
        return dict(curr_json_obj)

    def getStrValue(self, json_obj, tree_sequence, valid_values=None):
        """
        A function which retrieves a single string value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj, _ = self._traverse(json_obj, tree_sequence)
        if valid_values is not None:
            if curr_json_obj not in valid_values:
                raise Exception(
                    "'{}' is not within the list of valid values.".format(curr_json_obj)
                )
        return curr_json_obj

    def getBooleanValue(self, json_obj, tree_sequence):
        """
        A function which retrieves a single boolean value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj, _ = self._traverse(json_obj, tree_sequence)
        if isinstance(curr_json_obj, bool):
            return curr_json_obj
        raise Exception("'{}' is not 'True' or 'False'.".format(curr_json_obj))

    def getDateValue(self, json_obj, tree_sequence, date_format="%Y-%m-%d"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param date_format:
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj, _ = self._traverse(json_obj, tree_sequence)
        return datetime.datetime.strptime(curr_json_obj, date_format).date()

    def getDateTimeValue(self, json_obj, tree_sequence, date_time_format="%Y-%m-%d"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param date_time_format:
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj, _ = self._traverse(json_obj, tree_sequence)
        return datetime.datetime.strptime(curr_json_obj, date_time_format)

    def getStrListValue(self, json_obj, tree_sequence, valid_values=None):
        """
        A function which retrieves a list of string values from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj, _ = self._traverse(json_obj, tree_sequence)
        if not isinstance(curr_json_obj, list):
            raise Exception("Retrieved value is not a list.")
        if valid_values is not None:
            for val in curr_json_obj:
                if not isinstance(val, str):
                    raise Exception("'{}' is not of type string.".format(val))
                if val not in valid_values:
                    raise Exception(
                        "'{}' is not within the list of valid values.".format(val)
                    )
        return curr_json_obj

    def getNumericValue(
        self, json_obj, tree_sequence, valid_lower=None, valid_upper=None
    ):
        """
        A function which retrieves a single numeric value from a JSON structure.
        :param valid_lower:
        :param valid_upper:
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj, steps_str = self._traverse(json_obj, tree_sequence)

        if (not isinstance(curr_json_obj, bool)) and isinstance(
            curr_json_obj, (int, float)
        ):
            out_value = curr_json_obj
        elif isinstance(curr_json_obj, str):
            try:
                out_value = float(curr_json_obj)
            except ValueError:
                raise Exception(
                    "The identified value is not numeric '{}'".format(steps_str)
                )
        else:
            raise Exception(
                "The identified value is not numeric '{}'".format(steps_str)
            )

        if valid_lower is not None:
            if out_value < valid_lower:
                raise Exception(
                    "'{}' is less than the defined valid range.".format(out_value)
                )
        if valid_upper is not None:
            if out_value > valid_upper:
                raise Exception(
                    "'{}' is higher than the defined valid range.".format(out_value)
                )
        return out_value

    def getListValue(self, json_obj, tree_sequence):
        """
        A function which retrieves a list of values from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj, _ = self._traverse(json_obj, tree_sequence)
        if not isinstance(curr_json_obj, list):
            raise Exception("Retrieved value is not a list.")
        return curr_json_obj
