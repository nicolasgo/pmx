#!/usr/bin/python

import os.path

def precondition(files, extension):
    valid = True
    for input_filename in files:
        if not os.path.isfile(input_filename+extension):
            valid = False 
    return valid

