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

import logging
import gzip
import json
import datetime
import os
import time
import glob

logger = logging.getLogger(__name__)

class PBPTUtils(object):

    def get_file_lock(self, input_file, sleep_period=1, wait_iters=120, use_except=False):
        """
        A function which gets a lock on a file.

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

        """
        file_path, file_name = os.path.split(input_file)
        lock_file_name = ".{}.lok".format(file_name)
        lock_file_path = os.path.join(file_path, lock_file_name)

        got_lock = False
        for i in range(wait_iters+1):
            if not os.path.exists(lock_file_path):
                got_lock = True
                break
            time.sleep(sleep_period)

        if got_lock:
            c_datetime = datetime.datetime.now()
            f = open(lock_file_path, 'w')
            f.write('{}\n'.format(c_datetime.isoformat()))
            f.flush()
            f.close()
        elif use_except:
            raise Exception("Lock could not be gained for file: {}".format(input_file))

        return got_lock

    def release_file_lock(self, input_file, timeout=3600):
        """
        A function which releases a lock file for the input file. If for some reason
        removing the lock file fails then the clean_file_locks is called and the timeout
        variable will be called.

        :param input_file: The input file for which the lock will be created.
        :param timeout: the time (in seconds) for the timeout. Default: 3600 (1 hours).

        """
        file_path, file_name = os.path.split(input_file)
        lock_file_name = ".{}.lok".format(file_name)
        lock_file_path = os.path.join(file_path, lock_file_name)
        if os.path.exists(lock_file_path):
            try:
                os.remove(lock_file_path)
            except:
                logger.debug("Failed to remove lock file so calling clean_file_locks.")
                self.clean_file_locks(file_path, timeout)

    def clean_file_locks(self, dir_path, timeout=3600):
        """
        A function which cleans up any remaining lock file (i.e., if an application has crashed).
        The timeout time will be compared with the time written within the file.

        :param dir_path: the file path to search for lock files (i.e., ".*.lok")
        :param timeout: the time (in seconds) for the timeout. Default: 3600 (1 hours)

        """
        c_dateime = datetime.datetime.now()
        lock_files = glob.glob(os.path.join(dir_path, ".*.lok"))
        for lock_file_path in lock_files:
            create_date_str = self.readTextFileNoNewLines(lock_file_path)
            create_date_str = create_date_str.strip()
            if create_date_str != "":
                create_date = datetime.datetime.fromisoformat(create_date_str)
                time_since_create = (c_dateime - create_date).total_seconds()
                if time_since_create > timeout:
                    os.remove(lock_file_path)
            else:
                os.remove(lock_file_path)

    def readTextFileNoNewLines(self, file):
        """
        Read a text file into a single string
        removing new lines.

        :param file: File path to the input file.
        :return: string

        """
        txtStr = ""
        try:
            if os.path.exists(file):
                dataFile = open(file, 'r')
                for line in dataFile:
                    txtStr += line.strip()
                dataFile.close()
        except Exception as e:
            raise e
        return txtStr

    def readTextFile2List(self, file):
        """
        Read a text file into a list where each line
        is an element in the list.

        :param file: File path to the input file.
        :return: list

        """
        outList = []
        try:
            dataFile = open(file, 'r')
            for line in dataFile:
                line = line.strip()
                if line != "":
                    outList.append(line)
            dataFile.close()
        except Exception as e:
            raise e
        return outList

    def writeList2File(self, dataList, outFile):
        """
        Write a list a text file, one line per item.

        :param dataList: List of values to be written to the output file.
        :param out_file: File path to the output file.

        """
        try:
            f = open(outFile, 'w')
            for item in dataList:
               f.write(str(item)+'\n')
            f.flush()
            f.close()
        except Exception as e:
            raise e

    def writeData2File(self, data_val, out_file):
        """
        Write some data (a string or can be converted to a string using str(data_val) to
        an output text file.

        :param data_val: Data to be written to the output file.
        :param out_file: File path to the output file.

        """
        try:
            f = open(out_file, 'w')
            f.write(str(data_val)+'\n')
            f.flush()
            f.close()
        except Exception as e:
            raise e

    def writeDict2JSON(self, data_dict, out_file):
        """
        Write some data to a JSON file. The data would commonly be structured as a dict but could also be a list.

        :param data_dict: The dict (or list) to be written to the output JSON file.
        :param out_file: The file path to the output file.

        """
        import json
        with open(out_file, 'w') as fp:
            json.dump(data_dict, fp, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)

    def readJSON2Dict(self, input_file):
        """
        Read a JSON file. Will return a list or dict.

        :param input_file: input JSON file path.

        """
        import json
        with open(input_file) as f:
            data = json.load(f)
        return data

    def check_str(self, str_val, rm_non_ascii=False, rm_dashs=False, rm_spaces=False, rm_punc=False):
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
        import string
        str_val_tmp = str_val.strip()

        if rm_non_ascii:
            str_val_tmp_ascii = ""
            for c in str_val_tmp:
                if (c in string.ascii_letters) or (c in string.punctuation) or (c == ' '):
                    str_val_tmp_ascii += c
            str_val_tmp = str_val_tmp_ascii

        if rm_dashs:
            str_val_tmp = str_val_tmp.replace('-', '_')
            str_val_tmp = self.remove_repeated_chars(str_val_tmp, '_')

        if rm_spaces:
            str_val_tmp = str_val_tmp.replace(' ', '_')
            str_val_tmp = self.remove_repeated_chars(str_val_tmp, '_')

        if rm_punc:
            for punct in string.punctuation:
                if (punct != '_') and (punct != '-'):
                    str_val_tmp = str_val_tmp.replace(punct, '')

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
        out_str = ''
        p = ''
        for c in str_val:
            if c == repeat_char:
                if c != p:
                    out_str += c
            else:
                out_str += c
            p = c
        return out_str



class PBPTTextFileUtils(object):

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


class PBPTJSONParseHelper(object):

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
