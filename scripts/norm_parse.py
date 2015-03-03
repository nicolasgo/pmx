#!/usr/bin/python

import sys
import csv
import traceback
import datetime

def parse(input_file, filter_function, output_file = None):
    reader = csv.reader(input_file, delimiter='\t', skipinitialspace=True)
    for row in reader:
        try:
            time = datetime.datetime.strptime(row[0],  "%Y-%m-%d %H:%M:%S" )
            msisdn =  row[1]
            retcode = int(row[2])
            numbytes = int(row[3])
            ip = row[4]
            useragent = row[5]
            path = row[6]
            if filter_function(output_file, time, msisdn, retcode, numbytes, ip, useragent, path):
                if output_file:
                    output_file.write(row)
        except :
            traceback.print_exc()

