#!/usr/bin/python

import sys
import config
import shutil
import dependency
import subprocess
from optparse import OptionParser

def run(files):
    # Check are pre-condition
    if not dependency.precondition(files, '.pre'):
        import pre_filter 
        if config.verbose: print "running dependencies... '.pre' files doesn't exist"
        pre_filter.run(files)

    for input_filename in files:
        if config.verbose: print "sort(ing)", input_filename

        cmd = """/bin/bash -c "sort -t $'\t' -k 2,2 -k 1,1 -k 7,7 -o %s %s" """ % (config.get_current_file(input_filename,'.sort'), config.get_current_file(input_filename,'.pre'))
        subprocess.call(cmd, shell=True)

        if config.output_file_stdout:
            with open(config.get_current_file(input_filename,'.sort'), "r") as f:
                shutil.copyfileobj(f, sys.stdout)



if __name__ == "__main__":
    parser = OptionParser()
    (options, arguments) = parser.parse_args()

    # Filename on the command line have precedence over configuration
    files = config.input_files
    if arguments:
        files = arguments

    run(files)
