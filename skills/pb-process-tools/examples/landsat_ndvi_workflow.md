# `pb_process_tools` Examples

## 1. Batch Landsat NDVI with `rsgispy` (Slurm Job Array or GNU Parallel)

### Worker (`lndvi_perform_analysis.py`)

```python
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
        os.makedirs(os.path.dirname(os.path.abspath(output_ndvi)), exist_ok=True)

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
        return ["input_img", "output_ndvi", "red_band", "nir_band"]

    def outputs_present(self, **kwargs):
        return self.check_files({self.params["output_ndvi"]: "gdal_image"})

    def remove_outputs(self, **kwargs):
        output_ndvi = self.params["output_ndvi"]
        if os.path.exists(output_ndvi):
            os.remove(output_ndvi)


if __name__ == "__main__":
    ProcessCmd().std_run()
```

### Generator (`lndvi_gen_cmds.py`)

```python
import glob
import logging
import os

from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds

logger = logging.getLogger(__name__)


def infer_landsat_red_nir_bands(filename: str) -> tuple[int, int]:
    upper_name = os.path.basename(filename).upper()
    if ("LC08" in upper_name) or ("LC09" in upper_name):
        return 4, 5  # Landsat 8/9 OLI: Band 4=Red, Band 5=NIR
    return 3, 4      # Landsat 4/5/7 TM/ETM+: Band 3=Red, Band 4=NIR


class GenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        out_dir = os.path.abspath(kwargs["out_dir"])
        os.makedirs(out_dir, exist_ok=True)

        for in_img in sorted(glob.glob(kwargs["in_imgs_glob"])):
            basename = self.get_file_basename(in_img)
            output_ndvi = os.path.join(out_dir, f"{basename}_ndvi.tif")
            if not os.path.exists(output_ndvi):
                red_band, nir_band = infer_landsat_red_nir_bands(in_img)
                self.params.append(
                    {
                        "input_img": os.path.abspath(in_img),
                        "output_ndvi": output_ndvi,
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

    create_tools = GenCmds(
        cmd=f"python {py_script}",
        db_conn_file="db_conn.txt",
        process_tools_mod="lndvi_perform_analysis",
        process_tools_cls="ProcessCmd",
        db_table_name="landsat_ndvi",
        db_schema="eo_workflows",
    )
    create_tools.parse_cmds()
```

---

## 2. Running Multiple Concurrent Workflows in One Database

Give each workflow its own `db_table_name` (and separate `db_info_file`, `run_script`, and `job_dir`):

```python
# Workflow A (e.g., Sentinel-2 cloud masking)
gen_a = S2CloudGenCmds(
    cmd=f"python {s2_worker}",
    db_conn_file="db_conn.txt",
    process_tools_mod="s2_worker",
    process_tools_cls="S2ProcessCmd",
    db_table_name="s2_cloud_mask_2026",
    db_schema="eo_workflows",
)

# Workflow B (e.g., Landsat NDVI running simultaneously in the same DB/schema)
gen_b = NDVIGenCmds(
    cmd=f"python {ndvi_worker}",
    db_conn_file="db_conn.txt",
    process_tools_mod="ndvi_worker",
    process_tools_cls="NDVIProcessCmd",
    db_table_name="landsat_ndvi_2026",
    db_schema="eo_workflows",
)
```

Or override table/schema from the CLI on any `GenCmds` script:

```bash
python lndvi_gen_cmds.py --gen --table landsat_ndvi_roi2 --schema eo_workflows
python lndvi_gen_cmds.py --status --table landsat_ndvi_roi2 --schema eo_workflows
```
