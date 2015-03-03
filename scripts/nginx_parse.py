#!/usr/bin/python

import re
import sys
import traceback
import datetime

# Regular expressions
#
line_nginx_full_re = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST|HEAD) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<referer>(\-)|(.*))["]) (["](?P<useragent>.*)["]) (?P<id>\w+)""", re.IGNORECASE)

line_nginx_wifi_re = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST|HEAD) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<referer>(\-)|(.*))["]) (["](?P<useragent>.*)["]) - (?P<nexa>\w+) (["](?P<wifiid>(\-)|(.*))["])""", re.IGNORECASE)

line_nginx_re = [line_nginx_full_re, line_nginx_wifi_re]

wifiid_re = re.compile("""((userid=)(?P<userid>(\w+)))""")

# Two cases for msisdn
# First, an 'id' if we are in telco data mode
# or a 'wifiid' if we are in wifi only 
def get_msisdn(dct):
    # default value
    # 88 will be used to count the % of unknown user entries in the log    
    id = '88'
    if 'id' in dct:
        # telco data 
        id = dct['id']
    else:
        if 'wifiid' in dct:
            # wifi
            match = wifiid_re.search(dct['wifiid'])
            if match:
                # No 'else' because if we don't have a userid, we have a '-', 
                # so default value 88
                if 'userid' in match.groupdict():
                    id = match.groupdict()['userid']
    return id


def parse(input_file, filter_function, output_file = None):
    for line in input_file:
        try :
            # check for empty lines
            if not line.strip():
                continue
 
            idx = line.find('[', 0)
            if idx == -1:
                print "ERROR:-1", line
                continue

            ip = line[0:idx].replace(' - ', '')
            ip = ip.strip().split(' ')[0]
            rest = line[idx:]
   
            matched = False
            for regexp in line_nginx_re:
                match = regexp.search(rest)
                if match:
                    dct = match.groupdict()
                    time = datetime.datetime.strptime(dct['dateandtime'].rpartition(' ')[0],  "%d/%b/%Y:%H:%M:%S" )
                    path = dct['url']
                    numbytes = int(dct['bytessent'])
                    msisdn = get_msisdn(dct)
                    retcode = int(dct['statuscode'])
                    useragent = dct['useragent']

                    if filter_function(output_file, time, msisdn, retcode, numbytes, ip, useragent, path):
                        if output_file:
                            output_file.write(line)

                    # we have a match so set it, this is only for debugging (stderr) purposes
                    matched = True

                    break;

            if not matched:
                sys.stderr.write(' '.join(['No regexp match', line, '\n']))
        
        except re.error as e:
            print "ERROR:({0}): {1}".format(e.errno, e.strerror)
            print "ERROR:", line
            traceback.print_exc()

