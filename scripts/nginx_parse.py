#!/usr/bin/python

import re
import sys
import traceback
import datetime

# Regular expressions
#
line_nginx_full_re = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST|HEAD) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<referer>(\-)|(.*))["]) (["](?P<useragent>.*)["]) (?P<id>\w+)""", re.IGNORECASE)

line_nginx_wifi_re = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST|HEAD) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<referer>(\-)|(.*))["]) (["](?P<useragent>.*)["])""", re.IGNORECASE)

line_nginx_re = [line_nginx_full_re, line_nginx_wifi_re]

def parse(input_file, filter_function, output_file = None):
    for line in input_file:
        try :
            first = line.split(' - ')
            ip = first[0].split(' ')[0].strip()
            for regexp in line_nginx_re:
                match = regexp.search(first[1])
                if match:
                    dct = match.groupdict()
                    time = datetime.datetime.strptime(dct['dateandtime'].rpartition(' ')[0],  "%d/%b/%Y:%H:%M:%S" )
                    path = dct['url']
                    numbytes = int(dct['bytessent'])
                    msisdn = dct['id']
                    retcode = int(dct['statuscode'])
                    useragent = dct['useragent']

                    if filter_function(output_file, time, msisdn, retcode, numbytes, ip, useragent, path):
                        if output_file:
                            output_file.write(line)
                    break;
                else:
                    sys.stderr.write(' '.join(['No regexp match', line, '\n']))
        
        except :
            print "ERROR:", line
            traceback.print_exc()

