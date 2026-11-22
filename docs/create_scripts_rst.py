#!/usr/bin/env python
"""
Script to generate .rst file for scripts documentation. Runs scripts to get output and
inserts into text.

Inspired by answer suggesting modifying Makefile here:

http://stackoverflow.com/questions/7250659/python-code-to-generate-part-of-sphinx-documentation-is-it-possible

"""

import glob
import os
import subprocess
import sys


def get_script_help(script):
    """Get help output from a CLI script."""
    out = subprocess.check_output([sys.executable, script, "-h"])
    stdout_lines = out.decode("utf-8").split("\n")
    return "".join(f"    {line}\n" for line in stdout_lines)


if __name__ == "__main__":
    docs_dir = os.path.dirname(os.path.abspath(__file__))
    outfile = os.path.join(docs_dir, "scripts.rst")
    scripts_list = sorted(glob.glob(os.path.join(docs_dir, "..", "bin", "*.py")))

    scripts_text = "\n\nScripts\n========\n\n"
    for script in scripts_list:
        try:
            script_out = get_script_help(script)
        except Exception as err:
            print(f"Couldn't run {script}\n{err}")
            continue

        scripts_text = (
            f"{scripts_text}\n\n"
            f"{os.path.basename(script)}\n"
            f"------------------------------\n\n"
            f".. code-block:: text\n\n"
            f"{script_out}\n\n"
        )

    with open(outfile, "w", encoding="utf-8") as f:
        f.write(scripts_text)

