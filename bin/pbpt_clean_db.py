#! /usr/bin/env python

############################################################################
#  pbpt_clean_db.py
#
#  Copyright 2026 PB Process Tools.
#
# Purpose:  CLI tool to inspect and clean up leftover pb_process_tools
#           job queue tables from a PostgreSQL database.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 20/11/2026
# Version: 4.0.0
#
############################################################################

from pbprocesstools.pbpt_db_clean import main

if __name__ == "__main__":
    raise SystemExit(main())
