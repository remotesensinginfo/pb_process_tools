Command Line Tools
===================

splitcmdslist.py
----------------

This command allows a text file, i.e., a list of commands to be executed as a shell script, to split into a number of scripts. You can either specify the number of output files you require (using the ``-f`` option) or the number of commands per file (using the ``-s`` option).

Options::

    usage: splitcmdslist.py [-h] -i INPUT -o OUTPUT [-s SPLIT] [-f NFILES] [-p PRECMD] [--dealsplit]

    optional arguments:
      -h, --help            show this help message and exit
      -i INPUT, --input INPUT
                            Specify an input file.
      -o OUTPUT, --output OUTPUT
                            Specify an output file which is used as the base name for the output files.
      -s SPLIT, --split SPLIT
                            The number of commands per output file.
      -f NFILES, --nfiles NFILES
                            The number of output files to generate.
      -p PRECMD, --precmd PRECMD
                            Optionally provide custom command (e.g., singularity) to be prepended to all the commands.
      --dealsplit           Splits the commands as cards would be dealt rather than linear sequential split.


You can also choose to prepend each line (i.e., command) with some text. For example ``"docker run -itv /data:/data petebunting/au-eoed python "`` to execute with docker.


prefixcmdslst.py
-----------------

This command allows each line (i.e., command) to be pre-appended by some text. For example ``"docker run -itv /data:/data petebunting/au-eoed python "`` to execute with docker.

Options::

    usage: prefixcmdslst.py [-h] -i INPUT -o OUTPUT -p PREFIX

    optional arguments:
      -h, --help            show this help message and exit
      -i INPUT, --input INPUT
                            Specify an input file.
      -o OUTPUT, --output OUTPUT
                            Specify an output file which is used
      -p PREFIX, --prefix PREFIX
                            Provide a custom command (e.g., Singularity or Docker) to be prepended to all the commands in the input file.


subcmdslurm.py
--------------
This command allows a SLURM job submission script to be easily generated from template.


Options::

    usage: subcmdslurm.py [-h] -c CONFIG --cmd CMD -o OUTPUT [-t TEMPLATE]

    optional arguments:
      -h, --help            show this help message and exit
      -c CONFIG, --config CONFIG
                            Path to the JSON config file.
      --cmd CMD             Specify an input file.
      -o OUTPUT, --output OUTPUT
                            Specify an output file.
      -t TEMPLATE, --template TEMPLATE
                            Optionally provide a custom template file.



genslurmsub.py
--------------
This command is used to generate SLURM submission scripts from a shell script listing all the commands to be executed.


Options::

    usage: genslurmsub.py [-h] -c CONFIG -i INPUT -f CMDSFILE -o OUTPUT [-t TEMPLATE] [-p PRECMD] [--multi]

    optional arguments:
      -h, --help            show this help message and exit
      -c CONFIG, --config CONFIG
                            Path to the JSON config file.
      -i INPUT, --input INPUT
                            Specify an input file.
      -f CMDSFILE, --cmdsfile CMDSFILE
                            Specify an output file with srun commands to be run.
      -o OUTPUT, --output OUTPUT
                            Specify an output file.
      -t TEMPLATE, --template TEMPLATE
                            Optionally provide a custom template file.
      -p PRECMD, --precmd PRECMD
                            Optionally provide custom command (e.g., singularity) to be prepended to all the commands.
      --multi               Specify that multiple input files are being provided as a file list (i.e., the input file is a file which lists the input files.
