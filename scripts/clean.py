#!/usr/bin/python

import sys
import config
import os.path
import dependency
from optparse import OptionParser

def run(files):
    for input_filename in files:
        f = config.get_current_file(input_filename, '.pre')
        if os.path.isfile(f):
            os.remove(f)

        f = config.get_current_file(input_filename, '.norm')
        if os.path.isfile(f):
            os.remove(f)

        f = config.get_current_file(input_filename, '.sort')
        if os.path.isfile(f):
            os.remove(f)

        f = config.get_current_file(input_filename, '.pv')
        if os.path.isfile(f):
            os.remove(f)

        f = config.get_current_file(input_filename, '.cat_pv')
        if os.path.isfile(f):
            os.remove(f)


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
