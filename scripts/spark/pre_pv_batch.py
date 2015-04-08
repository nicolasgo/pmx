import os
import csv
import sys
import glob
import shutil
import datetime
import calendar
import subprocess
from optparse import OptionParser

import pv
 
def load_country_iso_code():
    filename = os.getenv('SPARK_HOME', '.')+'/iso.tsv'
    with open(filename, mode='r') as input_file:
        reader = csv.reader(input_file, delimiter='\t')
        next(reader) # skip the header
        dct = dict((rows[0].strip().lower(),rows[1].strip()) for rows in reader if not rows[0].strip().startswith('#'))
    print 'loaded iso.tsv: ',dct
    return dct


def gunzip_untar_pre(directory, directory_pre):
    basename = os.path.basename(os.path.normpath(directory_pre))
    cmd = "cd %s; tar -xvf %s; cd %s; gunzip part-*" % (directory, basename+'.tar', basename)
    subprocess.call(cmd, shell=True)


def gunzip_iso(day, iso):
    print "Decompressing files for", day, iso
    iso_country = iso
    cmd = """cd /home/nicolas/al; find . -type f -name "[%s][0.]*access.log-%s.gz" -print0 | xargs -0 -I {} -P 10 gunzip {}""" % (iso_country, day)
    subprocess.call(cmd, shell=True)

 
def run(day):
    # expand user directory
    home_path = os.path.expanduser('~')

    # output path 
    output_path = home_path + '/data/pv/'
    print "output path ", output_path
         
    country_iso_code = load_country_iso_code()
    for iso, code in country_iso_code.iteritems():

        print 'processing %s -> %s'%(iso,code)

        output_directory = output_path + code
        output_file = output_directory + '/' + day + '.pv'
        print "output_file", output_file

        # create output directory if necessary
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # if output file exists, remove it
        if os.path.isdir(output_file):
            shutil.rmtree(output_file)

        full_path = output_file + '.pre'
        if not os.path.exists(output_file + '.pre.tar'):
            # no .pre.tar
            # get all uncompressed files
            full_path = ''.join([home_path, '/al/vc*/*/*/', iso, '*access*', day])
            files = glob.glob(full_path)
            if len(files) == 0:
                # we have no regular access files, move on to .gz
                full_path = ''.join([home_path, '/al/vc*/*/*/', iso, '*access*', day, '.gz'])
                files = glob.glob(full_path)
                if len(files) == 0:
                    # we have nothing, move on
                    continue
                # gunzip
                gunzip_iso(day, iso)

            pv.run(full_path, output_file)

        else:
            if os.path.isdir(output_file+'.pre'):
                print 'removing tree for pre'
                shutil.rmtree(output_file+'.pre') # TODO: we could reuse the previous .pre instead.. 
            gunzip_untar_pre(output_directory, output_file+'.pre')
            pv.run_pre(full_path, output_file)
                
        if os.path.isdir(output_file+'.pre'):
            shutil.rmtree(output_file+'.pre') # TODO: Suggesting to remove the .pre once tared. pv_batch could have an option to prevent that.. 

 
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-m', '--month', type="int",
        dest="month", help='Specifies the month.  The default is current month.')
    parser.add_option('-y', '--year', type="int",
        dest="year", help='Specifies the year.  The default is current year.')
    parser.add_option('-d', '--day', type="int",
        dest="day", help='Specifies the day.  The default is current day.')

    options, arguments = parser.parse_args()

    # TODO: This is a quick kludge to allow simple passing of a date (e.g., 20141225)) via an argument.
    if len(arguments)>0:
        options.year = int(arguments[0][:4])
        options.month= int(arguments[0][4:6])
        if len(arguments[0])>6: # only YYYYMM were given 
            options.day  = int(arguments[0][6:])

    now = datetime.datetime.now()

    days = []

    # special case, if we have no options, take current day
    if options.year is None and options.month is None and options.day is None:
        days.append('%d%s%s' % (now.year, '{:02d}'.format(now.month), '{:02d}'.format(now.day)))
    else:
        year, month, day = options.year, options.month, options.day
        if options.year is None:
            year = now.year

        if options.month is None:
            month = now.month
        else:
            if options.month > 0 and options.month <= 12:
                month = options.month 
            else:
                print "Invalid month, should be between 1 and 12"
                exit(0)

        if options.day is None:
            (first_day, no_of_days) = calendar.monthrange(year, month)
            for d in range(1, no_of_days+1):
                days.append('%d%s%s' % (year, '{:02d}'.format(month), '{:02d}'.format(d)))
        else:
            (first_day, no_of_days) = calendar.monthrange(year, month)
            if options.day > 0 and options.day <= no_of_days:
                days.append('%d%s%s' % (year, '{:02d}'.format(month), '{:02d}'.format(options.day)))
            else:
                print "Invalid day. the day should be between 1 and", no_of_days, "for", calendar.month_name[month]
                exit(0)

    print "Starting Pre PV for", days
    for d in days:
        run(d)

