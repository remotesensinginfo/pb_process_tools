#!/usr/bin/env python
"""
pb_slurm_user_tools - Setup/Update the system.
"""
# This file is part of 'pb_slurm_user_tools'
# A set of utilities for working with slurm.
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
# Purpose:  Utilities used within the PB Slurm User Tools System.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 12/11/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import gzip
import json
import datetime

logger = logging.getLogger(__name__)


class PBSUTTextFileUtils(object):

    def readTextFile2List(self, file):
        """
        Read a text file into a list where each line
        is an element in the list.
        :param file:
        :return:
        """
        out_list = []
        try:
            dataFile = open(file, 'r')
            for line in dataFile:
                line = line.strip()
                if line != "":
                    out_list.append(line)
            dataFile.close()
        except Exception as e:
            raise e
        return out_list

    def writeList2File(self, data_list, out_file):
        """
        Write a list a text file, one line per item.
        :param data_list:
        :param out_file:
        :return:
        """
        try:
            f = open(out_file, 'w')
            for item in data_list:
                f.write(str(item) + '\n')
            f.flush()
            f.close()
        except Exception as e:
            raise e


class PBSUTJSONParseHelper(object):

    def readGZIPJSON(self, file_path):
        """
        Function to read a gzipped JSON file returning the data structure produced
        :param file_path:
        :return:
        """
        with gzip.GzipFile(file_path, "r") as fin:  # 4. gzip
            json_bytes = fin.read()                 # 3. bytes (i.e. UTF-8)

        json_str = json_bytes.decode("utf-8")       # 2. string (i.e. JSON)
        data = json.loads(json_str)                 # 1. data
        return data

    def writeGZIPJSON(self, data, file_path):
        """
        Function to write a gzipped json file.
        :param data:
        :param file_path:
        :return:
        """
        json_str = json.dumps(data) + "\n"           # 1. string (i.e. JSON)
        json_bytes = json_str.encode("utf-8")        # 2. bytes (i.e. UTF-8)

        with gzip.GzipFile(file_path, "w") as fout:  # 3. gzip
            fout.write(json_bytes)

    def doesPathExist(self, json_obj, tree_sequence):
        """
        A function which tests whether a path exists within JSON file.
        :param json_obj:
        :param tree_sequence: list of strings
        :return: boolean
        """
        curr_json_obj = json_obj
        steps_str = ""
        path_exists = True
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                path_exists = False
                break
        return path_exists


    def getValueDict(self, json_obj, tree_sequence):
        """

        :param json_obj:
        :param tree_sequence:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str + ":" + tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))

        out_dict = dict()
        for name in curr_json_obj:
            out_dict[name] = curr_json_obj[name]

        return out_dict

    def getStrValue(self, json_obj, tree_sequence, valid_values=None):
        """
        A function which retrieves a single string value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))
        if valid_values is not None:
            if curr_json_obj not in valid_values:
                raise Exception("'{}' is not within the list of valid values.".format(curr_json_obj))
        return curr_json_obj

    def getBooleanValue(self, json_obj, tree_sequence):
        """
        A function which retrieves a single boolean value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))
        if type(curr_json_obj).__name__ == "bool":
            rtn_bool = curr_json_obj
        else:
            raise Exception("'{}' is not 'True' or 'False'.".format(curr_json_obj))
        return rtn_bool

    def getDateValue(self, json_obj, tree_sequence, date_format="%Y-%m-%d"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param date_format:
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))
        try:
            out_date_obj = datetime.datetime.strptime(curr_json_obj, date_format).date()
        except Exception as e:
            raise Exception(e)
        return out_date_obj

    def getDateTimeValue(self, json_obj, tree_sequence, date_time_format="%Y-%m-%d"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param date_time_format:
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))
        try:
            out_datetime_obj = datetime.datetime.strptime(curr_json_obj, date_time_format)
        except Exception as e:
            raise Exception(e)
        return out_datetime_obj

    def getStrListValue(self, json_obj, tree_sequence, valid_values=None):
        """
        A function which retrieves a list of string values from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))

        if type(curr_json_obj).__name__ != "list":
            raise Exception("Retrieved value is not a list.")
        if valid_values is not None:
            for val in curr_json_obj:
                if type(val).__name__ != "str":
                    raise Exception("'{}' is not of type string.".format(val))
                if val not in valid_values:
                    raise Exception("'{}' is not within the list of valid values.".format(val))
        return curr_json_obj

    def getNumericValue(self, json_obj, tree_sequence, valid_lower=None, valid_upper=None):
        """
        A function which retrieves a single numeric value from a JSON structure.
        :param valid_lower:
        :param valid_upper:
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))

        if (type(curr_json_obj).__name__ == "int") or (type(curr_json_obj).__name__ == "float"):
            out_value = curr_json_obj
        elif type(curr_json_obj).__name__ == "str":
            if curr_json_obj.isnumeric():
                out_value = float(curr_json_obj)
            else:
                raise Exception("The identified value is not numeric '{}'".format(steps_str))
        else:
            raise Exception("The identified value is not numeric '{}'".format(steps_str))

        if valid_lower is not None:
            if out_value < valid_lower:
                raise Exception("'{}' is less than the defined valid range.".format(out_value))
        if valid_upper is not None:
            if out_value > valid_upper:
                raise Exception("'{}' is higher than the defined valid range.".format(out_value))
        return out_value

    def getListValue(self, json_obj, tree_sequence):
        """
        A function which retrieves a list of values from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise Exception("Could not find '{}'".format(steps_str))

        if type(curr_json_obj).__name__ != "list":
            raise Exception("Retrieved value is not a list.")
        return curr_json_obj

    def findStringValueESALst(self, lst_json_obj, name):
        """

        :param lst_json_obj:
        :param name:
        :return: [found, value]
        """
        value = ""
        found = False
        for json_obj in lst_json_obj:
            if json_obj["name"] == name:
                value = json_obj["content"]
                found = True
                break
        return [found, value]

    def findIntegerValueESALst(self, lst_json_obj, name):
        """

        :param lst_json_obj:
        :param name:
        :return: [found, value]
        """
        value = 0
        found = False
        for json_obj in lst_json_obj:
            if json_obj["name"] == name:
                value = int(json_obj["content"])
                found = True
                break
        return [found, value]
