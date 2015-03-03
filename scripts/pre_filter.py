#!/usr/bin/python

import re
import sys
import traceback
import config
import datetime
import shutil
from optparse import OptionParser

import config
import nginx_parse

# Regular expressions
#
line_nginx_full_re = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST|HEAD) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<referer>(\-)|(.*))["]) (["](?P<useragent>.*)["]) (?P<id>\w+)""", re.IGNORECASE)

line_nginx_wifi_re = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST|HEAD) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<referer>(\-)|(.*))["]) (["](?P<useragent>.*)["])""", re.IGNORECASE)

line_nginx_re = [line_nginx_full_re, line_nginx_wifi_re]

# Filtering
#
# For each method return True if you want to keep the line parsed

def filter_time(time):
    return len(config.time) == 0 or time.hour in config.time 

def filter_msisdn(msisdn):
    return len(config.msisdn) == 0 or msisdn in config.msisdn 

def filter_retcode(retcode):
    return len(config.retcode) == 0 or retcode in config.retcode  

def filter_numbytes(numbytes):
    return True

def filter_ip(ip):
    return len(config.ip) == 0 or ip in config.ip  

def filter_useragent(useragent):
    return len(config.useragent) == 0 or any(useragent.find(ua) != -1 for ua in config.useragent)

def filter_path(path):
    return len(config.path) == 0 or any(path.startswith(p) for p in config.path)

def pre_filter(output, time, msisdn, retcode, numbytes, ip, useragent, path):
    return  filter_time(time) and \
            filter_msisdn(msisdn) and \
            filter_retcode(retcode) and \
            filter_numbytes(numbytes) and \
            filter_ip(ip) and \
            filter_useragent(useragent) and \
            filter_path(path)

def run(files):
    for input_filename in files:
        if config.verbose: print "pre_fiter(ing)", input_filename

        input_file = open(input_filename, 'r')
        output_file = open(input_filename+'.pre', 'w')

        nginx_parse.parse(input_file, pre_filter, output_file)

        input_file.close()
        output_file.close()

        if config.output_file_stdout:
            with open(input_filename+'.pre', "r") as f:
                shutil.copyfileobj(f, sys.stdout)

if __name__ == "__main__":
    parser = OptionParser()
    (options, arguments) = parser.parse_args()

    # Filename on the command line have precedence over configuration
    files = config.input_files
    if arguments:
        files = arguments

    run(files)

