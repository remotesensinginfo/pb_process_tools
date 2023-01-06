# PB Process Tools

![example workflow](https://github.com/remotesensinginfo/pb_process_tools/actions/workflows/main.yml/badge.svg)

Tools for batch processing workflows as a set of jobs. Can either be used on a HPC with 
slurm or on a single workstation using GNU parallel. 

To install 

To install pb_process_tools to the default location, the following command is to be used from within the pb_process_tools source directory (i.e., where setup.py is located):

``
pip install .
``


if you want to install into another location using the --prefix option.

``
pip install . --prefix=/to/install/path
``

## Get Started ##

Run the command line function to generate a set of template functions:

``
pbpt_gen_template.py --output /path/to/out/dir --dbfile path/to/db/config/file.txt
``

