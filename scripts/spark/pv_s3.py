import os
import csv
import sys
import glob
import math
import shutil
import datetime
import calendar
import subprocess
from filechunkio import FileChunkIO
from optparse import OptionParser
#from boto.s3.connection import S3Connection
import boto


def load_country_iso_code():
    filename = os.getenv('SPARK_HOME', '.')+'/iso.tsv'
    with open(filename, mode='r') as input_file:
        reader = csv.reader(input_file, delimiter='\t')
        next(reader) # skip the header
        dct = dict((rows[0].strip().lower(),rows[1].strip()) for rows in reader if not rows[0].strip().startswith('#'))
    print 'loaded iso.tsv: ',dct
    return dct


def merge(directory, day):
    print "Merging",  directory, day
    cmd = """cd %s; find . -maxdepth 1 -type f -name 'part*' -print0 | sort -z | xargs -0 cat -- >../%s""" % (directory, day)
    subprocess.call(cmd, shell=True)


def puts3(country_path, day):
    print "Putting on s3",  country_path, day
    #cmd = """cd %s; s3put -r -b plugger -p '/home/nicolas/' %s""" % (country_path, day)
    cmd = """cd %s; s3put -r -b p-root-001 -p '/home/nicolas/' %s""" % (country_path, day)
    print "cmd", cmd
    #subprocess.call(cmd, shell=True)


def puts3_multipart(country_path, day):
    print "Putting on s3 multi part",  country_path, day

    source_path = ''.join([country_path, day])
    try:
      source_size = os.path.getsize(source_path)
    except:
      return
    
    #/Users/alainlav/data/
    #destination_path = source_path.replace('/home/nicolas/', '')
    destination_path = source_path.replace('/Users/alainlav/', '')
    print "^^^^^^^ destination path", destination_path

    s3 = boto.connect_s3()
    b = s3.get_bucket('p-root-001')
    mp = b.initiate_multipart_upload(destination_path)

    chunk_size = 1024 * 1024 * 1024 # 1 gig
    chunk_count = int(math.ceil(source_size / float(chunk_size)))

    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    mp.complete_upload()
    print "done"


def run(day):
    # expand user directory
    home_path = os.path.expanduser('~')

    # output path 
    output_path = home_path + '/data/pv/'
    print "output path ", output_path
         
    country_iso_code = load_country_iso_code()
    for iso, code in country_iso_code.iteritems():

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
        #print puts3_multipart(country_path, day)


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

    print "Starting pv_s3 for", days
    for d in days:
        run(d)

