#!/usr/bin/python

import sys
import config
import shutil
import dependency
import subprocess
from optparse import OptionParser

def execute(command):    
    popen = subprocess.Popen(command, stdout=subprocess.PIPE)
    lines_iterator = iter(popen.stdout.readline, b"")
    for line in lines_iterator:
        print(line) # yield line

def run(files):
    # Check are pre-condition
    if not dependency.precondition(files, '.pv'):
        import normalize
        if config.verbose: print "running dependencies... '.pv' files doesn't exist"
        normalize.run(files)

    for input_filename in files:
        cmd = """more %s | awk -F $',' '{arr[$1]+=$%d;sum+=$%d} END {for (i in arr) {print i,arr[i]};print sum}'""" % (config.get_current_file(input_filename,'.pv'), options.column, options.column)
        subprocess.call(cmd, shell=True)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-c', '--column',
        help='Column to sum', type=int, default=4)

    options, arguments = parser.parse_args()

    # Filename on the command line have precedence over configuration
    files = config.input_files
    if arguments:
        files = arguments

    run(files)
