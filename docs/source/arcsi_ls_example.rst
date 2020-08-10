ARCSI Landsat Processing
===========================

This example uses pb_process_tools and the arcsi (https://arcsi.remotesensing.info) tool to download
and process Landsat data. The scripts are within the follow repository:

https://github.com/petebunting/pbpt_examples/tree/master/ls_analysis

Setting up local database
--------------------------

The first step is to create a local database of the landsat acquisitions::

    arcsisetuplandsatdb.py -f ./01_find_ls_scns/ls_db_20200810.db


Finding and Downloading Scenes
--------------------------------

The next step is to identify and download the relevant scenes. This is setup using the ARCSI class RecordScn2Process.
The ``RecordScn2Process`` is a class which provides an interface to a database (SQLite) for recording whether scenes
have been downloaded and a the ARD product generated.

This is setup using implementations of the ``PBPTGenQProcessToolCmds`` and ``PBPTQProcessTool`` classes.

The rows and paths to be downloaded are provided as a separate text file (rowpaths.txt)::

    23,205
    24,205
    23,204
    24,204
    23,203
    24,203


The ``PBPTGenQProcessToolCmds`` implementation is then shown below, where the ``run_gen_commands`` function is the
function which controls the processing taking place being the function called when the ``--gen`` option is executed.
Calling the ``self.gen_command_info`` function generates the command, in this case searching the database and
populating the ``c_dict`` dict, which gets written to the database::

    from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
    import rsgislib
    import os.path
    import logging
    import sqlite3
    import statistics
    import os
    from arcsilib.arcsiscnprocessdb import RecordScn2Process


    logger = logging.getLogger(__name__)


    def gen_arcsi_ls_sensor(spacecraft, sensor):
        arcsi_sensor = ''
        if spacecraft == 'LANDSAT_1':
            arcsi_sensor = 'ls1'
        elif spacecraft == 'LANDSAT_2':
            arcsi_sensor = 'ls2'
        elif spacecraft == 'LANDSAT_3':
            arcsi_sensor = 'ls4'
        elif (spacecraft == 'LANDSAT_4') and (sensor == 'MSS'):
            arcsi_sensor = 'ls4mss'
        elif (spacecraft == 'LANDSAT_5') and (sensor == 'MSS'):
            arcsi_sensor = 'ls5mss'
        elif (spacecraft == 'LANDSAT_4') and (sensor == 'TM'):
            arcsi_sensor = 'ls4tm'
        elif (spacecraft == 'LANDSAT_5') and (sensor == 'TM'):
            arcsi_sensor = 'ls5tm'
        elif spacecraft == 'LANDSAT_7':
            arcsi_sensor = 'ls7'
        elif spacecraft == 'LANDSAT_8':
            arcsi_sensor = 'ls8'
        else:
            raise Exception("Do not recognise the spacecraft/sensor.")
        return arcsi_sensor

    class FindLandsatScnsGenDwnlds(PBPTGenQProcessToolCmds):

        def gen_command_info(self, **kwargs):
            rsgis_utils = rsgislib.RSGISPyUtils()
            rowpath_lst = rsgis_utils.readTextFile2List(kwargs['rowpath_lst'])
            gg_ls_db_conn = sqlite3.connect(kwargs['db_file'])
            query = """SELECT PRODUCT_ID, SPACECRAFT_ID, SENSOR_ID, BASE_URL FROM LANDSAT WHERE WRS_ROW = ? AND WRS_PATH = ?
                       AND COLLECTION_NUMBER = ? AND CLOUD_COVER < ?
                       AND date(SENSING_TIME) > date(?) AND date(SENSING_TIME) < date(?)
                       ORDER BY CLOUD_COVER ASC, date(SENSING_TIME) DESC LIMIT {}""".format(kwargs['n_scns'] + kwargs['n_scns_xt'])


            scn_rcd_obj = RecordScn2Process(kwargs['scn_db_file'])
            if not os.path.exists(kwargs['scn_db_file']):
                scn_rcd_obj.init_db()

            for rowpathstr in rowpath_lst:
                rowpath_lst = rowpathstr.split(',')
                row = int(rowpath_lst[0])
                path = int(rowpath_lst[1])
                logger.info("Processing Row/Path: [{}, {}]".format(row, path))
                rowpath_geoid = "r{}_p{}".format(row, path)
                n_scns = scn_rcd_obj.n_geoid_scns(rowpath_geoid)
                if n_scns < kwargs['n_scns']:
                    gg_ls_db_cursor = gg_ls_db_conn.cursor()
                    query_vars = [row, path, kwargs['collection'], kwargs['cloud_thres'], kwargs['start_date'], kwargs['end_date']]

                    scn_ids = list()
                    scn_lst = list()
                    for row in gg_ls_db_cursor.execute(query, query_vars):
                        spacecraft_id = row[1]
                        sensor_id = row[2]
                        arcsi_sensor = gen_arcsi_ls_sensor(spacecraft_id, sensor_id)

                        if (not scn_rcd_obj.is_scn_in_db(row[0], arcsi_sensor)) and (row[0] not in scn_ids):
                            logger.info("Adding to processing: {}".format(row[0]))
                            scn = dict()
                            scn['product_id'] = row[0]
                            scn['sensor'] = arcsi_sensor
                            scn['scn_url'] = row[3]
                            scn['geo_str_id'] = rowpath_geoid
                            scn_lst.append(scn)

                            c_dict = dict()
                            c_dict['product_id'] = row[0]
                            c_dict['sensor'] = arcsi_sensor
                            c_dict['scn_url'] = row[3]
                            c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], row[0])
                            c_dict['scn_db_file'] = kwargs['scn_db_file']
                            c_dict['goog_key_json'] = kwargs['goog_key_json']
                            if not os.path.exists(c_dict['downpath']):
                                os.mkdir(c_dict['downpath'])
                            self.params.append(c_dict)
                            scn_ids.append(row[0])
                            n_scns += 1
                        elif not scn_rcd_obj.is_scn_downloaded(row[0], arcsi_sensor):
                            c_dict = dict()
                            c_dict['product_id'] = row[0]
                            c_dict['sensor'] = arcsi_sensor
                            c_dict['scn_url'] = row[3]
                            c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], row[0])
                            c_dict['scn_db_file'] = kwargs['scn_db_file']
                            c_dict['goog_key_json'] = kwargs['goog_key_json']
                            if not os.path.exists(c_dict['downpath']):
                                os.mkdir(c_dict['downpath'])
                            self.params.append(c_dict)
                        if n_scns >= kwargs['n_scns']:
                            break
                    if len(scn_lst) > 0:
                        scn_rcd_obj.add_scns(scn_lst)
                else:
                    #GET SCENES WHICH HAVE NOT DOWNLOADED AND ADD to JOB LIST.
                    scns = scn_rcd_obj.get_scns_download(rowpath_geoid)
                    for scn in scns:
                        c_dict = dict()
                        c_dict['product_id'] = scn.product_id
                        c_dict['sensor'] = scn.sensor_id
                        c_dict['scn_url'] = scn.scn_url
                        c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], scn.product_id)
                        c_dict['scn_db_file'] = kwargs['scn_db_file']
                        c_dict['goog_key_json'] = kwargs['goog_key_json']
                        if not os.path.exists(c_dict['downpath']):
                            os.mkdir(c_dict['downpath'])
                        self.params.append(c_dict)

        def run_gen_commands(self):
            self.gen_command_info(
                db_file='./ls_db_20200810.db',
                rowpath_lst='./rowpaths.txt',
                cloud_thres=20,
                collection = '01',
                start_date='2016-01-01',
                end_date='2020-07-01',
                n_scns=1,
                n_scns_xt=10,
                scn_db_file='./ls_scn.db',
                dwnld_path='/Users/pete/Temp/arcsi_test_db_class/ls_dwnlds',
                goog_key_json='/Users/pete/Temp/arcsi_test_db_class/GlobalMangroveWatch-74b58b05fd73.json')

            self.pop_params_db()
            self.create_shell_exe("run_dwnld_cmds.sh", "dwnld_cmds.sh", 4, db_info_file=None)
            #self.create_slurm_sub_sh("dwnld_ls_scns", 8224, '/scratch/a.pfb/gmw_v2_gapfill/logs',
            #                         run_script='run_exe_analysis.sh', job_dir="job_scripts",
            #                         db_info_file=None, account_name='scw1376', n_cores_per_job=5, n_jobs=2,
            #                         job_time_limit='2-23:59',
            #                         module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

        def run_check_outputs(self):
            process_tools_mod = 'perform_dwnld_jobs'
            process_tools_cls = 'PerformScnDownload'
            time_sample_str = self.generate_readable_timestamp_str()
            out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
            out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
            self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

        def run_remove_outputs(self, all_jobs=False, error_jobs=False):
            process_tools_mod = 'perform_dwnld_jobs'
            process_tools_cls = 'PerformScnDownload'
            self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)

    if __name__ == "__main__":
        py_script = os.path.abspath("perform_dwnld_jobs.py")
        #script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)
        script_cmd = "python {}".format(py_script)

        create_tools = FindLandsatScnsGenDwnlds(cmd=script_cmd, sqlite_db_file="dwnld_ls_scns.db")
        create_tools.parse_cmds()


You have the choice between calling ``create_shell_exe`` or ``create_slurm_sub_sh``. ``create_shell_exe`` is used
when analysis is being undertaken locally on a machine while ``create_slurm_sub_sh`` is used to generate the scripts
to perform the processing on a SLURM based computational cluster. Note if you are using singularity or docker for
your analysis then you'll need to add the prefix to the command as shown (but commented out).


The implementation of ``PBPTQProcessTool`` to download the scenes uses the google gsutil command::

    from pbprocesstools.pbpt_q_process import PBPTQProcessTool
    import logging
    import subprocess
    import os
    from arcsilib.arcsiscnprocessdb import RecordScn2Process

    logger = logging.getLogger(__name__)

    class PerformScnDownload(PBPTQProcessTool):

        def __init__(self):
            super().__init__(cmd_name='perform_dwnld_jobs.py', descript=None)

        def do_processing(self, **kwargs):
            scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
            downloaded = scn_rcd_obj.is_scn_downloaded(self.params['product_id'], self.params['sensor'])
            if not downloaded:
                cmd = "gsutil -m cp -r {} {}".format(self.params['scn_url'], self.params['downpath'])
                logger.debug("Running command: '{}'".format(cmd))
                subprocess.call(cmd, shell=True)
                scn_rcd_obj.set_scn_downloaded(self.params['product_id'], self.params['sensor'], self.params['downpath'])

        def required_fields(self, **kwargs):
            return ["product_id", "sensor", "scn_url", "downpath", "scn_db_file", "goog_key_json"]

        def outputs_present(self, **kwargs):
            scn_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
            downloaded = scn_rcd_obj.is_scn_downloaded(self.params['product_id'], self.params['sensor'])
            return downloaded, dict()

        def remove_outputs(self, **kwargs):
            # Remove the output files.
            scn_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
            scn_rcd_obj.reset_all_scn(self.params['product_id'], self.params['sensor'], delpath=True)
            if not os.path.exists(self.params['downpath']):
                os.mkdir(self.params['downpath'])

    if __name__ == "__main__":
        PerformScnDownload().std_run()


Generating ARD
----------------

Generating the ARD products will take the same approach, using implementations of the ``PBPTGenQProcessToolCmds``
and ``PBPTQProcessTool`` classes.

To generate the commands to be executed the ``RecordScn2Process`` class will be used to find the scenes
which have been downloaded but not processed to an ARD product::

    from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
    import logging
    import os
    from arcsilib.arcsiscnprocessdb import RecordScn2Process

    logger = logging.getLogger(__name__)

    class FindSen2ScnsGenDwnlds(PBPTGenQProcessToolCmds):

        def gen_command_info(self, **kwargs):
            if not os.path.exists(kwargs['scn_db_file']):
                raise Exception("Landsat scene database does not exist...")
            scn_rcd_obj = RecordScn2Process(kwargs['scn_db_file'])
            scns = scn_rcd_obj.get_scns_ard()
            for scn in scns:
                c_dict = dict()
                c_dict['product_id'] = scn.product_id
                c_dict['sensor'] = scn.sensor_id
                c_dict['dwnld_path'] = scn.download_path
                c_dict['ard_path'] = os.path.join(kwargs['ard_path'], scn.product_id)
                c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], scn.product_id)
                c_dict['scn_db_file'] = kwargs['scn_db_file']
                c_dict['dem'] = kwargs['dem']
                if not os.path.exists(c_dict['ard_path']):
                    os.mkdir(c_dict['ard_path'])
                if not os.path.exists(c_dict['tmp_dir']):
                    os.mkdir(c_dict['tmp_dir'])
                self.params.append(c_dict)

        def run_gen_commands(self):
            self.gen_command_info(scn_db_file='../01_find_ls_scns/ls_scn.db',
                                  ard_path='/Users/pete/Temp/arcsi_test_db_class/ls_ard',
                                  tmp_dir='/Users/pete/Temp/arcsi_test_db_class/tmp',
                                  dem='/Users/pete/Temp/arcsi_test_db_class/dem.kea')
            self.pop_params_db()
            self.create_shell_exe("run_ard_cmds.sh", "ard_cmds.sh", 4, db_info_file=None)
            #self.create_slurm_sub_sh("ard_ls_scns", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
            #                         run_script='run_exe_analysis.sh', job_dir="job_scripts",
            #                         db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
            #                         job_time_limit='2-23:59',
            #                         module_load='module load parallel singularity\n\n')

        def run_check_outputs(self):
            process_tools_mod = 'perform_ard_process'
            process_tools_cls = 'PerformScnARD'
            time_sample_str = self.generate_readable_timestamp_str()
            out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
            out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
            self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

        def run_remove_outputs(self, all_jobs=False, error_jobs=False):
            process_tools_mod = 'perform_ard_process'
            process_tools_cls = 'PerformScnARD'
            self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)


    if __name__ == "__main__":
        py_script = os.path.abspath("perform_ard_process.py")
        #script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)
        script_cmd = "python {}".format(py_script)

        create_tools = FindSen2ScnsGenDwnlds(cmd=script_cmd, sqlite_db_file="ard_ls_scns.db")
        create_tools.parse_cmds()


The ``PBPTQProcessTool`` implementation for generating an ARD product for the scenes, note ARCSI is
executed by calling the ``runARCSI`` function.::

    from pbprocesstools.pbpt_q_process import PBPTQProcessTool
    import logging
    import arcsilib
    import arcsilib.arcsirun
    import os
    import shutil
    from arcsilib.arcsiscnprocessdb import RecordScn2Process

    logger = logging.getLogger(__name__)

    class PerformScnARD(PBPTQProcessTool):

        def __init__(self):
            super().__init__(cmd_name='perform_ard_process.py', descript=None)

        def do_processing(self, **kwargs):
            scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
            downloaded = scn_rcd_obj.is_scn_downloaded(self.params['product_id'], self.params['sensor'])
            ard_processed = scn_rcd_obj.is_scn_ard(self.params['product_id'], self.params['sensor'])
            if downloaded and (not ard_processed):
                input_hdr = self.find_first_file(self.params['dwnld_path'], "*MTL.txt")

                arcsilib.arcsirun.runARCSI(input_hdr, None, None, self.params['sensor'], None, "KEA",
                                           self.params['ard_path'], None, None, None, None, None, None,
                                           ["CLOUDS", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW",
                                            "METADATA"],
                                           True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH,
                                           arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                                           "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                                           None, None, self.params['tmp_dir'], 0.05, 0.5, 0.1, 0.4, self.params['dem'],
                                           None, None, True, 20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                                           True, False, False, None, None, True, None, 'LSMSK')
                scn_rcd_obj.set_scn_ard(self.params['product_id'], self.params['sensor'], self.params['ard_path'])
            elif ard_processed:
                scn_rcd_obj.set_scn_ard(self.params['product_id'], self.params['sensor'], self.params['ard_path'])

            if os.path.exists(self.params['tmp_dir']):
                shutil.rmtree(self.params['tmp_dir'])

        def required_fields(self, **kwargs):
            return ["product_id", "sensor", "dwnld_path", "ard_path", "scn_db_file", "tmp_dir", "dem"]

        def outputs_present(self, **kwargs):
            scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
            ard_processed = scn_rcd_obj.is_scn_ard(self.params['product_id'], self.params['sensor'])
            return ard_processed, dict()

        def remove_outputs(self, **kwargs):
            # Reset the tmp dir
            if os.path.exists(self.params['tmp_dir']):
                shutil.rmtree(self.params['tmp_dir'])
            os.mkdir(self.params['tmp_dir'])

            # Remove the output files.
            scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
            scn_rcd_obj.reset_ard_scn(self.params['product_id'], self.params['sensor'], delpath=True)
            if not os.path.exists(self.params['ard_path']):
                os.mkdir(self.params['ard_path'])


    if __name__ == "__main__":
        PerformScnARD().std_run()




