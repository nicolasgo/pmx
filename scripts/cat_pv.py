#!/usr/bin/python

'''
 cat_pv aggregates user sessions files (built using pv) and outputs a csv file
 
 ex: cat_pv $input.pv.csv -g1440 -d -o $input.pv.day.csv

 Copyright (c) 2014, 2015, Pragmex Inc, All Right Reserved
 http://pragmex.com/
 
'''

# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

# Example:
# time zcat vc?.prod.s.plugger.com/2014/07/mx.plugger.com-access.log-20140719.gz | grep -v 'GET /apple\|GET /ga\|GET /favicon' >20140719&
# time split -l 16000000 20140918 20140918_&
# pv 20140919_ac >20140919_ac_pv.csv

# <codecell>

# Use the following command to cat multiple csv files without repeating the csv header:
# cat <(cat 20140919_aa_pv.csv) <(tail -n +2 20140919_ab_pv.csv) <(tail -n +2 20140919_ac_pv.csv) > 20140919_session.csv
# cat <(cat 20140919_ad_pv.csv) <(tail -n +2 20140919_ae_pv.csv) <(tail -n +2 20140919_af_pv.csv) >> 20140919_session.csv

# Then, they must be sorted using the following: (this sorts all lines but the first one (the header))
# cat  20140919_session.csv | awk 'NR == 1; NR > 1 {print $0 | "sort -n"}' > 20140919_session.s.csv

# <markdowncell>

# Useful link to convert times:
#  -    http://www.epochconverter.com

# <codecell>

# Use cat_pv to concatenate session files
# It will merge continuous sessions when they are splitted amongst two different files

# join_sessions

import sys
from datetime import datetime, date, time, timedelta

import csv,operator
from optparse import OptionParser
import shutil
from collections import Counter

import config
import dependency
import norm_parse

#session_gap=30*60 # minimum gap between sessions (in seconds)
session_gap=10*60*60 # minimum gap between sessions (in seconds)

# Array 'csv' index constants
MSISDN = 0
TIME = 1
DURATION = 2
BYTES = 3
IP = 4
PV = 5
VARIETY = 6

userdata = {}
users = Counter()

def calc_end_time(s):

    if options.dailysum: #make the endtime at 00:01 tomorrow
        startday=datetime.strptime(s[TIME].split(' ')[0], '%Y-%m-%d')
        endday=startday+timedelta(days=1)
        end=int(endday.strftime("%s"))
        #start=int(startday.strftime("%s"))

	#print end-start, datetime.fromtimestamp(start),datetime.fromtimestamp(end)
	
    else:
        start = int(datetime.strptime(s[1], '%Y-%m-%d %H:%M:%S').strftime("%s"))
        duration = int(s[DURATION])
        end = start+duration

    return end

def sum_sessions(s1, s2):
    if options.dailysum:
        s1[DURATION] = int(s1[DURATION])+int(s2[DURATION]) # in dailysum mode we simply add the duration times
    else:
        # compute new duration
        start = int(datetime.strptime(s1[TIME], '%Y-%m-%d %H:%M:%S').strftime("%s"))
        try:
            end=calc_end_time(s2)
        except:
            print 'Error in sum_session():', s2[TIME]
            end=calc_end_time(s1)

        if (start+int(s1[DURATION])) < end: # Make sure that s2 is not contained within s1's interval
            s1[DURATION] = end-start # new duration

    s1[BYTES] = int(s1[BYTES])+int(s2[BYTES])
    
    s1[VARIETY] = 0
    try:
        for i in range(9,len(s1)-1):
            s1[i] = int(s1[i])+int(s2[i])
            if s1[i]>0:
                s1[VARIETY] +=1
    except ValueError as e:
        print e
        print

    return s1

# merge the sessions of a given user that intersect
def merge_sessions(sessions):
    sessions = sorted(sessions, key=operator.itemgetter(1)) # make sure the sessions are sorted by time

    newlist= [sessions[0]]
    end=calc_end_time(sessions[0])+session_gap
    
    for s in sessions[1:]:
        start = int(datetime.strptime(s[1], '%Y-%m-%d %H:%M:%S').strftime("%s"))
        
        if start < end:
            #print '--eureka', newlist[-1][1],newlist[-1][2], s[0],s[1],s[2], 'start,end',start,end,end-start
            sum_sessions(newlist[-1], s) # we aggregate
            #print 'new duration:', newlist[-1][2]
        else:
            #print '--------', newlist[-1][1],newlist[-1][2], s[0],s[1],s[2]

            newlist.append(s)
            end=calc_end_time(s)+session_gap
           
#    if len(sessions) != len(newlist):
#        print len(sessions), len(newlist)
    
    return newlist

def print_oneuser(output, msisdn):
    d=userdata[msisdn]
    d['session']= sorted(d['session'],key=lambda s: s['time']) # sort sessions by time otherwise this is messy

    for s in d['session']:
        print >>output, '%s,%s,%s,%s,%s,' % (msisdn, s['time'], s['duration'], s['bytes'], s['ip']),
        print >>output, '%s,%s,%s,%s,' % (s['pv'], s['variety'], s['landpage'], s['device']),
        print >>output, '%s,%s,%s,%s,' % (s['inbox'], s['mymed'], s['conve'], s['activ']),
        print >>output, '%s,%s,%s,%s,' % (s['home'], s['frien'], s['info'], s['media']),
        print >>output, '%s,%s,%s,%s,%s' % (s['uploa'], s['avata'], s['publi'], s['help'], s['ua'])



# 
# targetcount: only prints users with at least targetcount pageviews
# limit: limits the number of users to print
#
# output is csv-like
# 
def print_users(output):
    for key in userdata:
        print_oneuser(output, key)

#
#
# msisdn, time, duration, bytes, ip, pv, variety, landpage, device, inbox, mymed, conve, activ, home, frien, info, media, uploa, avata, publi, help, ua
def add_session(fields):
    msisdn = fields[0]
    users[msisdn] += 1
    if msisdn not in userdata:
        newsession = {'time':fields[1], 'duration':int(fields[2]), 'bytes':int(fields[3]), 'ip':fields[4], 'pv':fields[5], 'variety':fields[6], 'landpage':fields[7], 'device':fields[8], 'inbox':int(fields[9]), 'mymed':int(fields[10]), 'conve':int(fields[11]), 'activ':int(fields[12]), 'home':int(fields[13]), 'frien':int(fields[14]), 'info':int(fields[15]), 'media':int(fields[16]), 'uploa':int(fields[17]), 'avata':int(fields[18]), 'publi':int(fields[19]), 'help':int(fields[20]), 'ua':fields[21]}
        userdata[msisdn]={'session':[newsession]}
        
        if msisdn == '68f8c9a97f66':
            print newsession['time'], newsession['duration']
    else: 
        d=userdata[msisdn]

        current_time = datetime.strptime(fields[1],  "%Y-%m-%d %H:%M:%S" )
        current_duration = int(fields[2])

        # We need to find current session
        current_session = None
        for session in d['session']:
            session_time = datetime.strptime(session['time'],  "%Y-%m-%d %H:%M:%S" )
            delta = (current_time - session_time).total_seconds()
            if abs(delta) <= session_gap:
                current_session = session 
                break

        if current_session:
            session_time = datetime.strptime(current_session['time'],  "%Y-%m-%d %H:%M:%S" )
               
            # maybe we gonna have a new start time
            new_start_time = session_time

            current_end_time = current_time+timedelta(seconds=current_duration)
            session_end_time = session_time+timedelta(seconds=current_session['duration'])

            new_duration = 0

            delta = int((current_time - session_time).total_seconds())
            if delta < 0:
                # we need to change start time of session and add the delta of start time
                current_session['time'] = fields[1] 
                new_start_time = current_time
                new_duration += abs(delta)

            if current_end_time > session_end_time:
                current_session['duration'] = new_duration + (current_end_time - new_start_time).total_seconds() 
            else:
                current_session['duration'] = new_duration + current_session['duration']

            if msisdn == '68f8c9a97f66':
                print new_duration, current_session['time'], current_session['duration'], fields[1], fields[2]
                        
            current_session['bytes'] += int(fields[3]) 
                
            current_session['inbox'] += int(fields[9])                
            current_session['mymed'] += int(fields[10])                
            current_session['conve'] += int(fields[11])                
            current_session['activ'] += int(fields[12])                
            current_session['home'] += int(fields[13])                
            current_session['frien'] += int(fields[14])                
            current_session['info'] += int(fields[15])                
            current_session['media'] += int(fields[16])                
            current_session['uploa'] += int(fields[17])                
            current_session['avata'] += int(fields[18])                
            current_session['publi'] += int(fields[19])                
            current_session['help'] += int(fields[20])                

        else: 
            newsession = {'time':fields[1], 'duration':int(fields[2]), 'bytes':int(fields[3]), 'ip':fields[4], 'pv':fields[5], 'variety':fields[6], 'landpage':fields[7], 'device':fields[8], 'inbox':int(fields[9]), 'mymed':int(fields[10]), 'conve':int(fields[11]), 'activ':int(fields[12]), 'home':int(fields[13]), 'frien':int(fields[14]), 'info':int(fields[15]), 'media':int(fields[16]), 'uploa':int(fields[17]), 'avata':int(fields[18]), 'publi':int(fields[19]), 'help':int(fields[20]), 'ua':fields[21]}
            #newsession = {'time':fields[1], 'duration':int(fields[2].strip()), 'bytes':fields[3], 'ip':fields[4], 'pv':fields[5], 'variety':fields[6], 'landpage':fields[7], 'device':fields[8], 'inbox':fields[9], 'mymed':fields[10], 'conve':fields[11], 'activ':fields[12], 'home':fields[13], 'frien':fields[14], 'info':fields[15], 'media':fields[16], 'uploa':fields[17], 'avata':fields[18], 'publi':fields[19], 'help':fields[20], 'ua':fields[21]}
            d['session'].append(newsession)


def cat_pv_filter(lines):
                
    add_session(lines.split(','))
        
    return False

def parse(input_file, filter_function, output_file = None):
    for line in input_file:
        # check for empty lines
        if not line.strip():
            continue
        if line.startswith('msisdn'):
            continue
        filter_function(line)


def run(files):
    # Check are pre-condition
    if not dependency.precondition(files, '.pv'):
        import pv
        if config.verbose: print "running dependencies... '.pv' files doesn't exist"
        pv.run(files)

    for input_filename in files:
        if config.verbose: print "cat_pv(ing)", input_filename

        input_file = open(config.get_current_file(input_filename,'.pv'), 'r')

        output_file = open(config.get_current_file(input_filename, '.cat_pv'), 'w')

        if options.dailysum:
            session_gap=0;

        parse(input_file, cat_pv_filter, output_file)
    
        print_users(output_file)

        input_file.close()
        output_file.close()

        if config.output_file_stdout:
            with open(config.get_current_file(input_filename,'.pv'), "r") as f:
                shutil.copyfileobj(f, sys.stdout)

"""
def run(files):
    # Check are pre-condition
    if not dependency.precondition(files, '.pv'):
        import pv
        if config.verbose: print "running dependencies... '.pv' files doesn't exist"
        pv.run(files)

    for input_filename in files:
        if config.verbose: print "cat_pv(ing)", input_filename

        input_file = open(config.get_current_file(input_filename,'.pv'), 'r')

        output_file = open(config.get_current_file(input_filename, '.cat_pv'), 'w')
        writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        if options.dailysum:
            session_gap=0;

        linenum=0

        reader = csv.reader(input_file, skipinitialspace=True)
        header = next(reader)
        writer.writerow(header) 
        
        msisdn=''
        sessions=[]
        for row in reader:
            linenum+=1
          
            if row[0].startswith('msisdn'): # removes any subsequent header row (due to a cat file1 file2 >thisfile.csv)
                continue

            if msisdn != row[0]:
                if len(sessions) > 0:
                    sessions= merge_sessions(sessions)
                    writer.writerows(sessions)
                msisdn = row[0]

                #print '***',linenum,'****',row[:2]
                sessions = []
                sessions.append(row)
            else:
                sessions.append(row)

        if len(sessions) > 0:
            sessions=merge_sessions(sessions)
            writer.writerows(sessions) 

        input_file.close()
        output_file.close()

        if config.output_file_stdout:
            with open(config.get_current_file(input_filename,'.cat_pv'), "r") as f:
                shutil.copyfileobj(f, sys.stdout)
"""

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-o', '--output',
        help='Specifies the output file.  The default is stdout.')
    parser.add_option('-g', '--session_gap', type="int",
        help='The session gap in minutes. Default is 30 minutes')
    parser.add_option('-d', '--daily',
        dest="dailysum", default=False, action="store_true",)

    options, arguments = parser.parse_args()

    if options.session_gap and options.session_gap != 0:
        session_gap= int(options.session_gap)*60 # store the session length in seconds

    # Filename on the command line have precedence over configuration
    files = config.input_files
    if arguments:
        files = arguments

    if config.verbose: print "files", files

    run(files)

