# `pb_process_tools` Examples

## 1. Batch Landsat NDVI with `rsgispy` (Slurm Job Array)

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
        return self.check_files(
            {
                self.params["output_ndvi"]: {
                    "type": "gdal_image",
                    "n_bands": 1,
                }
            }
        )

    def remove_outputs(self, **kwargs):
        output_ndvi = self.params["output_ndvi"]
        for candidate in (output_ndvi, f"{output_ndvi}.aux.xml", f"{output_ndvi}.ovr"):
            if os.path.exists(candidate):
                os.remove(candidate)


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

---

## 3. Local GNU Parallel Workflow with `out_cmp_file` Sentinel & `kwargs`

Use `out_cmp_file` in `self.params` when a job writes multiple outputs into a directory; `PBPTQProcessTool.completed_processing()` automatically touches `out_cmp_file` upon success. Extra `**kwargs` passed to `std_run(...)` are forwarded into `do_processing`, `required_fields`, and debug `remove_outputs`.

### Worker (`tile_process.py`)

```python
import os
from pbprocesstools.pbpt_q_process import PBPTQProcessTool


class TileWorker(PBPTQProcessTool):
    def __init__(self):
        super().__init__(cmd_name="tile_process.py", descript="Process tile folder.")

    def do_processing(self, **kwargs):
        out_dir = self.params["out_dir"]
        os.makedirs(out_dir, exist_ok=True)
        # Use kwargs passed from std_run(scale_factor=...)
        scale = kwargs.get("scale_factor", 1.0)
        with open(os.path.join(out_dir, "result.txt"), "w", encoding="utf-8") as fp:
            fp.write(f"tile={self.params['tile_id']} scale={scale}\n")

    def required_fields(self, **kwargs):
        return ["tile_id", "out_dir", "out_cmp_file"]

    def outputs_present(self, **kwargs):
        return self.check_files(
            {
                self.params["out_cmp_file"]: "file",
                os.path.join(self.params["out_dir"], "result.txt"): "filesize",
            }
        )

    def remove_outputs(self, **kwargs):
        self.remove_dir(self.params["out_dir"])
        if os.path.exists(self.params["out_cmp_file"]):
            os.remove(self.params["out_cmp_file"])


if __name__ == "__main__":
    TileWorker().std_run(scale_factor=0.0001)
```

### Generator (`tile_gen_cmds.py`, using `PBPT_DB_CONN_STR`)

```python
import os
from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds


class TileGenCmds(PBPTGenQProcessToolCmds):
    def gen_command_info(self, **kwargs):
        base_out = os.path.abspath(kwargs["base_out"])
        os.makedirs(base_out, exist_ok=True)
        for tile_id in kwargs["tile_ids"]:
            cmp_file = os.path.join(base_out, f"{tile_id}.done")
            if not os.path.exists(cmp_file):
                self.params.append(
                    {
                        "tile_id": tile_id,
                        "out_dir": os.path.join(base_out, tile_id),
                        "out_cmp_file": cmp_file,
                    }
                )

    def run_gen_commands(self):
        self.gen_command_info(base_out="./tile_outputs", tile_ids=["T30UVB", "T30UVC"])
        self.pop_params_db()
        self.create_shell_exe(
            run_script="run_tiles.sh",
            cmds_sh_file="tile_cmds.sh",
            n_cores=4,
            db_info_file="tiles_db_info.json",
        )


if __name__ == "__main__":
    # Reads PBPT_DB_CONN_STR from environment when db_conn_file / db_conn_str are omitted
    py_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tile_process.py")
    TileGenCmds(
        cmd=f"python {py_script}",
        process_tools_mod="tile_process",
        process_tools_cls="TileWorker",
        db_table_name="tile_jobs_demo",
    ).parse_cmds()
```
