import os
import csv
import sys
import glob
import shutil
import datetime
from optparse import OptionParser

import pv
 
def load_country_iso_code():
    filename = os.getenv('SPARK_HOME', '.')+'/oid.csv'
    with open(filename, mode='r') as input_file:
        reader = csv.reader(input_file)
        dct = dict((rows[2].strip(),rows[0].strip()) for rows in reader)
    return dct


def run():
    # expand user directory
    home_path = os.path.expanduser('~')
    # get today as string, example : 20141213
    today = datetime.datetime.today().strftime('%Y%m%d')

    # output path 
    output_path = home_path + '/data/pv/'
    print "output path ", output_path
         
    country_iso_code = load_country_iso_code()
    for iso, code in country_iso_code.iteritems():
        full_path = ''.join([home_path, '/al/vc*/*/*/', iso, '*access*', today, '.gz'])
        
        # only to check of there's any files
        files = glob.glob(full_path)
        if len(files) == 0:
            continue

        print files    
        
        output_directory = output_path + code
        output_file = output_directory + '/' + today + '.pv'
        print "output_file", output_file

        # create output directory if necessary
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # if output file exists, remove it
        if os.path.isdir(output_file):
            shutil.rmtree(output_file)

        pv.run(full_path, output_file)


if __name__ == "__main__":
    run()

