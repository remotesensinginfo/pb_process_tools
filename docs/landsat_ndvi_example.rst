Example: Calculating NDVI for Landsat Images using rsgispy
==========================================================

This example demonstrates a complete batch workflow using ``pb_process_tools`` v4.0.2 and ``rsgispy`` to calculate Normalised Difference Vegetation Index (NDVI) across a directory of multi-band Landsat surface reflectance images.

Each scene is processed independently by parallel workers (on Slurm using a job array or on a local workstation using GNU Parallel), tracked in a dedicated PostgreSQL queue table, verified in parallel, and cleaned up afterwards.

Overview
--------

* **Input**: A directory of multi-band Landsat surface reflectance GeoTIFFs (e.g., Landsat 8/9 OLI where Band 4 is Red and Band 5 is NIR, or Landsat 4/5/7 TM/ETM+ where Band 3 is Red and Band 4 is NIR).
* **Processing Library**: ``rsgispy.imagecalc.calcindices.calc_indices`` computes ``NDVI = (NIR - Red) / (NIR + Red)`` in a single windowed pass and populates statistics and overview pyramids.
* **Queue Isolation**: Uses table ``landsat_ndvi`` in schema ``eo_workflows`` so it can run concurrently alongside other tasks in the same PostgreSQL database.


1. Generate the Project Scaffold
--------------------------------

Configure your PostgreSQL connection string in ``db_conn.txt`` (e.g., ``postgresql+psycopg2://eo_user:secret@dbhost:5432/processing``) or export ``PBPT_DB_CONN_STR``. Then scaffold the workflow:

.. code-block:: bash

    mkdir -p landsat_ndvi_workflow
    pbpt_gen_template.py \
        --output landsat_ndvi_workflow \
        --dbfile db_conn.txt \
        --table landsat_ndvi \
        --schema eo_workflows \
        --prefix lndvi \
        --slurm \
        --partition compute \
        --array \
        --max_concurrent 20

This generates ``lndvi_perform_analysis.py``, ``lndvi_gen_cmds.py``, and executable helper scripts (``lndvi_run_gen_cmds.sh``, ``lndvi_run_status.sh``, ``lndvi_run_errors.sh``, ``lndvi_run_checks.sh``, ``lndvi_run_report.sh``, ``lndvi_run_reset_errs.sh``).


2. Implement the Worker (``lndvi_perform_analysis.py``)
-------------------------------------------------------

Update ``lndvi_perform_analysis.py`` to compute NDVI with ``rsgispy.imagecalc.calcindices.calc_indices`` and validate outputs with ``self.check_files``:

.. code-block:: python

    import logging
    import os

    import rsgispy
    import rsgispy.imagecalc.calcindices

    from pbprocesstools.pbpt_q_process import PBPTQProcessTool

    logger = logging.getLogger(__name__)


    class ProcessCmd(PBPTQProcessTool):
        def __init__(self):
            super().__init__(cmd_name="lndvi_perform_analysis.py", descript=None)

        def do_processing(self, **kwargs):
            input_img = self.params["input_img"]
            output_ndvi = self.params["output_ndvi"]
            os.makedirs(os.path.dirname(output_ndvi), exist_ok=True)

            band_names = {
                int(self.params["red_band"]): "red",
                int(self.params["nir_band"]): "nir",
            }

            logger.info("Calculating NDVI for %s -> %s", input_img, output_ndvi)
            rsgispy.imagecalc.calcindices.calc_indices(
                input_img=input_img,
                output_img=output_ndvi,
                band_names=band_names,
                indices=["NDVI"],
                in_gain=float(self.params.get("in_gain", 1.0)),
                in_offset=float(self.params.get("in_offset", 0.0)),
                gdalformat="GTIFF",
                datatype=rsgispy.TYPE_32FLOAT,
                calc_stats=True,
                use_pbar=False,
            )

        def required_fields(self, **kwargs):
            return [
                "input_img",
                "output_ndvi",
                "red_band",
                "nir_band",
            ]

        def outputs_present(self, **kwargs):
            files_dict = {
                self.params["output_ndvi"]: "gdal_image",
            }
            return self.check_files(files_dict)

        def remove_outputs(self, **kwargs):
            output_ndvi = self.params["output_ndvi"]
            if os.path.exists(output_ndvi):
                os.remove(output_ndvi)


    if __name__ == "__main__":
        ProcessCmd().std_run()

.. note::

   Setting ``use_pbar=False`` inside ``calc_indices`` keeps worker logs clean when many tasks run concurrently under Slurm or GNU Parallel. If your Landsat Collection 2 surface reflectance products are stored as scaled uint16 values (scale ``0.0000275``, offset ``-0.2``, i.e. ``physical = (dn - in_offset) / in_gain``), pass ``in_gain=1.0 / 0.0000275`` and ``in_offset=0.2 / 0.0000275`` (or convert to reflectance beforehand). For Landsat surface reflectance already scaled by ``10000``, use ``in_gain=10000.0`` and ``in_offset=0.0``.


3. Implement the Job Generator (``lndvi_gen_cmds.py``)
------------------------------------------------------

Update ``lndvi_gen_cmds.py`` to scan input Landsat scenes, determine the Red/NIR band indices per sensor, skip scenes whose output already exists, populate the queue, and write Slurm job array scripts:

.. code-block:: python

    import glob
    import logging
    import os

    from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

    logger = logging.getLogger(__name__)


    def infer_landsat_red_nir_bands(filename: str) -> tuple[int, int]:
        """Return 1-based (red, nir) band indices for common Landsat stacks."""
        upper_name = os.path.basename(filename).upper()
        if ("LC08" in upper_name) or ("LC09" in upper_name):
            # Landsat 8/9 OLI stacked bands: 1=Coastal, 2=Blue, 3=Green, 4=Red, 5=NIR
            return 4, 5
        # Landsat 4/5 TM & Landsat 7 ETM+: 1=Blue, 2=Green, 3=Red, 4=NIR
        return 3, 4


    class GenCmds(PBPTGenQProcessToolCmds):
        def gen_command_info(self, **kwargs):
            out_dir = kwargs["out_dir"]
            os.makedirs(out_dir, exist_ok=True)

            landsat_imgs = sorted(glob.glob(kwargs["in_imgs_glob"]))
            for in_img in landsat_imgs:
                basename = self.get_file_basename(in_img)
                output_ndvi = os.path.join(out_dir, f"{basename}_ndvi.tif")

                if not os.path.exists(output_ndvi):
                    red_band, nir_band = infer_landsat_red_nir_bands(in_img)
                    self.params.append(
                        {
                            "input_img": os.path.abspath(in_img),
                            "output_ndvi": os.path.abspath(output_ndvi),
                            "red_band": red_band,
                            "nir_band": nir_band,
                            "in_gain": kwargs.get("in_gain", 10000.0),
                            "in_offset": kwargs.get("in_offset", 0.0),
                        }
                    )

        def run_gen_commands(self):
            self.gen_command_info(
                in_imgs_glob="/data/landsat/sr/*.tif",
                out_dir="/data/landsat/ndvi",
                in_gain=10000.0,
                in_offset=0.0,
            )

            self.pop_params_db()

            self.create_slurm_sub_sh(
                "landsat_ndvi",
                8192,
                "/data/landsat/logs/ndvi",
                run_script="lndvi_run_exe_analysis.sh",
                job_dir="lndvi_job_scripts",
                db_info_file="lndvi_pbpt_lcl_db_info.json",
                n_cores_per_job=8,
                n_xtr_cmds=0,
                n_jobs=10,
                job_time_limit="04:00:00",
                module_load="module load parallel",
                partition="compute",
                use_array=True,
                max_concurrent=20,
            )


    if __name__ == "__main__":
        script_dir = os.path.dirname(os.path.abspath(__file__))
        py_script = os.path.join(script_dir, "lndvi_perform_analysis.py")
        script_cmd = f"python {py_script}"

        create_tools = GenCmds(
            cmd=script_cmd,
            db_conn_file="db_conn.txt",
            process_tools_mod="lndvi_perform_analysis",
            process_tools_cls="ProcessCmd",
            db_table_name="landsat_ndvi",
            db_schema="eo_workflows",
        )
        create_tools.parse_cmds()

.. tip::

   To run on a single workstation instead of Slurm, replace ``self.create_slurm_sub_sh(...)`` with::

       self.create_shell_exe(
           run_script="lndvi_run_exe_analysis.sh",
           cmds_sh_file="lndvi_pbpt_cmds_lst.sh",
           n_cores=8,
           db_info_file="lndvi_pbpt_lcl_db_info.json",
       )


4. Execute, Monitor, Check, and Clean Up
----------------------------------------

1. **Dry-run and populate the queue**:

   .. code-block:: bash

       ./lndvi_run_gen_cmds.sh --dry-run
       ./lndvi_run_gen_cmds.sh

2. **Submit the Slurm job array** (or run GNU Parallel locally):

   .. code-block:: bash

       sh lndvi_run_exe_analysis.sh

3. **Watch live progress**:

   .. code-block:: bash

       ./lndvi_run_status.sh --watch --interval 10

4. **Append new scenes later without resetting existing results**:

   .. code-block:: bash

       ./lndvi_run_gen_cmds.sh --append

5. **Verify outputs in parallel & inspect any failures**:

   .. code-block:: bash

       ./lndvi_run_checks.sh -w 8
       ./lndvi_run_errors.sh --limit 10
       python lndvi_gen_cmds.py --inspect 42

   If transient errors occurred, reset failed jobs (or delete partial files and reset) before resubmitting:

   .. code-block:: bash

       python lndvi_gen_cmds.py --reset --error
       # or delete bad outputs too:
       ./lndvi_run_reset_errs.sh

6. **Clean up completed queue tables**:

   .. code-block:: bash

       python lndvi_gen_cmds.py --drop -y
       # Or sweep all finished tables across schemas with pbpt_clean_db.py:
       pbpt_clean_db.py -d db_conn.txt --all-schemas --drop --completed --drop-empty-schemas --vacuum -y
