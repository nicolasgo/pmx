#!/usr/bin/python

import re
import sys
import traceback
import shutil
import datetime
import os.path
from optparse import OptionParser

import config
import dependency
import nginx_parse

def print_header(output, time, msisdn, retcode, numbytes, ip, useragent, path):
    line = '\t'.join([time.strftime("%Y-%m-%d %H:%M:%S"), msisdn, str(retcode), str(numbytes), ip, '"'+useragent+'"', path])
    output.write(line)
    output.write('\n')

    return False

def run(files):
    # Check are pre-condition
    if not dependency.precondition(files, '.pre'):
        import pre_filter
        if config.verbose: print "running dependencies... '.pre' files doesn't exist"
        pre_filter.run(files)

    for input_filename in files:
        if config.verbose: print "normaliz(ing)", input_filename

        input_file = open(input_filename+'.pre', 'r')
        output_file = open(input_filename+'.norm', 'w')

        nginx_parse.parse(input_file, print_header, output_file)

        input_file.close()
        output_file.close()

        if config.output_file_stdout:
            with open(input_filename+'.norm', "r") as f:
                shutil.copyfileobj(f, sys.stdout)

if __name__ == "__main__":
    parser = OptionParser()
    (options, arguments) = parser.parse_args()

    # Filename on the command line have precedence over configuration
    files = config.input_files
    if arguments:
        files = arguments

    run(files)

    #if config.verbose:
    #    with open(input_filename+'.norm', "r") as f:
    #        shutil.copyfileobj(f, sys.stdout)

