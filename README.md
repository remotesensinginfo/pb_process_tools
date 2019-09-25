# PB Process Tools

Tools for batch processing large datasets, including generating slurm job submission scripts. These are a set of scripts I have put together to streamline my workflows.

## Example Configuration files

Basic options which must be provided. 
```json
{
    "pbprocesstools":
    {
        "sbatch":
        {
            "jobname":"Test",
            "logfileout":"./log.out",
            "logfileerr":"./log.err",
            "time":"1-24:00",
            "mem_per_core_mb":"2000",
            "ncores":"1",
            "ncores_node":"1",
            "env_setup":"source activate au-eoed-env-v1"
        }
    }
}
```

If you wish for the system to send you an emails about the job then you can add the following options, note email type can be either ALL or END.
```json
{
    "pbprocesstools":
    {
        "sbatch":
        {
            "jobname":"Test",
            "logfileout":"./log.out",
            "logfileerr":"./log.err",
            "time":"1-24:00",
            "mem_per_core_mb":"2000",
            "ncores":"1",
            "ncores_node":"1",
            "env_setup":"source activate au-eoed-env-v1",
            "email_address":"pfb@aber.ac.uk",
            "email_type":"END"
        }
    }
}
```

## Example command:

The following command will create a script to run the commands listed in ard_cmds.txt using GNU parallel submitted to a batch squeue using the sbatch command.
```bash
genslurmsub.py -c config-sbatch.json -i ard_cmds.txt -f ard_cmds_srun.sh -o ard_cmds_sbatch.sh
```

The contents of the input file ard_cmds.txt are (these can be any commands which are executed from the terminal):

```bash
eoddrun.py -c ./EODataDownBaseConfig_psql.json -n 1 -s LandsatGOOG --processard --sceneid 9
eoddrun.py -c ./EODataDownBaseConfig_psql.json -n 1 -s LandsatGOOG --processard --sceneid 83
```

The output file ard_cmds_srun.sh contains:

```bash
srun -n1 -N1 --exclusive eoddrun.py -c ./EODataDownBaseConfig_psql.json -n 1 -s LandsatGOOG --processard --sceneid 9
srun -n1 -N1 --exclusive eoddrun.py -c ./EODataDownBaseConfig_psql.json -n 1 -s LandsatGOOG --processard --sceneid 83
```

While the output file ard_cmds_sbatch.sh contains:

```bash
#!/bin/bash --login

#SBATCH --partition=compute
#SBATCH --job-name=Test
#SBATCH --output=./log.out.%J
#SBATCH --error=./log.err.%J
#SBATCH --time=Test
#SBATCH --ntasks=1
#SBATCH --mem-per-cpu=2000
#SBATCH --ntasks-per-node=1
#SBATCH --mail-type=END
#SBATCH --mail-user=pfb@aber.ac.uk

source activate au-eoed-env-v1

parallel -N 1 --delay .2 -j $SLURM_NTASKS < ard_cmds_srun.sh

```

This is executed using: 
```bash
sbatch ard_cmds_sbatch.sh
```
