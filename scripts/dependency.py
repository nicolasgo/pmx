#!/usr/bin/python

import config
import os.path

def precondition(files, extension):
    valid = True
    for input_filename in files:
        if not os.path.isfile(config.get_current_file(input_filename, extension)):
            valid = False 
    return valid

