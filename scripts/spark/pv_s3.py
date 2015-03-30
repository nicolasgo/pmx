import os
import csv
import sys
import glob
import shutil
import datetime
import calendar
import subprocess
from optparse import OptionParser


def load_country_iso_code():
    filename = os.getenv('SPARK_HOME', '.')+'/oid.csv'
    with open(filename, mode='r') as input_file:
        reader = csv.reader(input_file)
        dct = dict((rows[2].strip(),rows[0].strip()) for rows in reader)
    return dct


def merge(directory, day):
    print "Merging",  directory, day
    cmd = """cd %s; find . -maxdepth 1 -type f -name 'part*' -print0 | sort -z | xargs -0 cat -- >../%s""" % (directory, day)
    subprocess.call(cmd, shell=True)


def puts3(country_path, day):
    print "Putting on s3",  country_path, day
    #cmd = """cd %s; s3put -r -b plugger -p '/home/nicolas/' %s""" % (country_path, day)
    cmd = """cd %s; s3put -r -b p-root-001 -p '/home/nicolas/' %s""" % (country_path, day)
    subprocess.call(cmd, shell=True)


def run(day):
    # expand user directory
    home_path = os.path.expanduser('~')

    # output path 
    output_path = home_path + '/data/pv/'
    print "output path ", output_path
         
    country_iso_code = load_country_iso_code()
    for iso, code in country_iso_code.iteritems():

        if iso == 'iso':
            # csv header
            continue

        #if iso != 'fs' or iso == 'mx':
        if iso != 'mx':
            continue

        # get all files
        country_path = ''.join([output_path, code, '/'])
        full_path = ''.join([output_path, code, '/', day, '.pv'])
        files = glob.glob(full_path)
        if len(files) == 0:
            # we have no files, move on
            print "No data for", iso
            continue 

        print merge(full_path, day)
        print puts3(country_path, day)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-m', '--month', type="int",
        dest="month", help='Specifies the month.  The default is current month.')
    parser.add_option('-y', '--year', type="int",
        dest="year", help='Specifies the year.  The default is current year.')
    parser.add_option('-d', '--day', type="int",
        dest="day", help='Specifies the day.  The default is current day.')

    options, arguments = parser.parse_args()

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

    print "Starting pv_s3 for", days
    for d in days:
        run(d)

