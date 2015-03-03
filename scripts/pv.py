#!/usr/bin/python

'''
 PV parses nginx access logs to compute user sessions

 Copyright (c) 2014, 2015, Pragmex Inc, All Right Reserved
 http://pragmex.com/
 
'''

# TODO: compute POSTs (search, uploads etc)

import sys 
import operator
import datetime
from collections import Counter
import fileinput
from optparse import OptionParser
import binascii
import socket
import shutil

import traceback

import re
import csv

import config
import dependency
import norm_parse

#
# some variables
# Global variables

DEBUG = True 

pageviews = Counter()

coverage = 100 # a number from 1 to 100.
session_length=30*60 # session length in seconds

hoursum=[0]*24

undefined = {} # to store all undefined strings 
users = Counter()
userdata = {}
output = sys.stdout

notneeded = ['apple-touch-icon-57x57.png','static','favicon.ico','ga','facebook','twitter','proxy','apple-touch-icon-72x72.png','apple-touch-icon-114x114.png','apple-touch-icon-144x144.png']

##
## Here we go!
##

parser = OptionParser()
parser.add_option('-o', '--output',
    help='Specifies the output file.  The default is stdout.')
parser.add_option('-f', '--filter',
    help='Specifies the file containing the msisdn to filter. The default is no filter list.')
parser.add_option('-s', '--summary',
    help='List a summary of the top. The default is YES.')
parser.add_option('-l', '--session_length',
    help='The session length in minutes. Default is 20 minutes')
parser.add_option('-p', '--coverage',
    help='A number from 1 to 100 to define the % of users to scan. The default coverage is 100.')


options, arguments = parser.parse_args()

if options.output and options.output != '-':
   output=sys.stdout = open(options.output, 'w')

if options.coverage and options.coverage != '-':
  coverage=int(options.coverage)
  if coverage < 1:
    coverage=1
  elif coverage > 100:
    coverage=100

if options.session_length and options.session_length != '-':
  session_length= int(options.session_length)*60 # store the session length in seconds

#
# inc_pageviews increments the number of pageviews for each root and store that in the pageviews Counter
# inputs:
#    - path: the url path (e.g., /profile/22084494/inbox)
def inc_pageviews(path):
    path= path.split('/')
    root=path[1] # all paths start with a '/', so item #1 is what is after the first slash
    
    if root in notneeded: # we do not need to count this request as a pageview
        root=None

    # TODO: we will need to add a elif to check if this is a media request. In that case, we do not count this as a pageview either.
    # or should we move this elsewhere??
    
    else:
        if root.startswith('?'):
            root='__qparam__'
        elif root.startswith('profile'): 
            # profile is too vague so we need to add the 3rd component. 
            # The 2nd component is for the user_id like in /profile/17548/inbox, /profile/17548/activities, /profile/17548/media
            try:
                part='/'+path[3].split('?')[0]
                root=part[1:]
            except:
                pass

        root = root.split('?')[0]

        pageviews[root] += 1

    return root

# <codecell>


pages_to_count=['inbox', 'mymedia', 'conversaciones', 'activities', 'home', 'friends', 'info', 'media', 'upload', 'avatar', 'publish', 'help']
def print_pagecounts(output, pages):
    for k in pages_to_count:
        print >>output, '%d,'%(pages[k]),
    
def print_header(output):
    print >>output, 'msisdn, time, duration, bytes, ip, pv, variety, landpage, device,',
    for i in range(0,len(pages_to_count)):
        print >>output, '%s,'%(pages_to_count[i][:5]),
    print >>output, 'ua'
        

def print_oneuser(output, msisdn, count):
    d=userdata[msisdn]
    d['session']= sorted(d['session'],key=lambda s: s['time']) # sort sessions by time otherwise this is messy

    for s in d['session']:
        device='0'
        idx=s['ua'].find('ndroid')
        if idx>0:
            device=s['ua'][idx-1:idx+11].strip(',')
            
        print >>output, '%s, %s, %s, %d, %8s,'%(msisdn,s['time'], s['duration'], s['bytes'], s['ip'] ),
        print >>output, '%3d,%d,%-6s,%s,' % (s['pv'], len(s['pages']), '/'+s['path'][:5],device),
        print_pagecounts(output, s['pages'])
        print >>output, '\"'+s['ua']+'\"'


# 
# targetcount: only prints users with at least targetcount pageviews
# limit: limits the number of users to print
#
# output is csv-like
# 
def print_users(output,targetcount=0,limit=None):
    #print_header(output) # csv header

    for msisdn, count in users.most_common(limit):
        if msisdn > 10: # msisdn <1000 are for internal use.
            if count < targetcount:
                break
            print_oneuser(output,msisdn,count)



#
# add_user() to the users Counter and computes the session
# msisdn: msisdn of the user
# time: time of the event
# pathroot: root of the url path (e.g., /home, /profile etc). Set to None will only compute the numbytes and not the pageview.
#
def add_user(msisdn, time, pathroot, useragent, ip, numbytes):
    if msisdn > 10: # msisdn below 1000 are for error codes. all msisdn are fairly large numbers: 525145946440
        users[msisdn] += 1

    if msisdn not in userdata: # create the first session entry (min session is 10 secs)
        session={'time':time,'pv':1,'duration':0,'path':pathroot, 'ip':ip, 'ua':useragent, 'pages':Counter(), 'bytes':numbytes} 
        session['pages'][pathroot] +=1
        userdata[msisdn]={'time':time,'sessioncount':1,'session':[session], 'total_duration':0}
    else: 
        d=userdata[msisdn]

        # We need to find current session
        current_session = None
        for session in d['session']:
            delta = (time - session['time']).total_seconds()
            #print "time", time, "delta", delta
            if delta <= session_length:
                current_session = session 
                break

        if current_session:
            duration = int((time - current_session['time']).total_seconds())

            current_session['duration'] += duration
            current_session['pv'] +=1
            current_session['pages'][pathroot] +=1
            current_session['bytes'] += numbytes

            d['total_duration'] += duration
        else: 
            newsession={'time':time,'pv':1,'duration':0,'path':pathroot, 'ip':ip, 'ua':useragent, 'pages':Counter(), 'bytes':numbytes}
            newsession['pages'][pathroot] +=1
            d['session'].append(newsession)
            d['sessioncount']=d['sessioncount']+1


def pv_filter(output, time, msisdn, retcode, numbytes, ip, useragent, path):
    pathroot=inc_pageviews(path)
            
    if pathroot != None:
        hour=time.hour
        hoursum[hour] += 1
                
        add_user(msisdn,time,pathroot, useragent, ip, numbytes)
        
    users[0] += 1 # user[0] counts all pageviews regardless if the patg is right

    # Returns False because we don't want any output during parsing/filtering
    return False


def run(files):
    # Check are pre-condition
    if not dependency.precondition(files, '.sort'):
        import sort
        if config.verbose: print "running dependencies... '.sort' files doesn't exist"
        sort.run(files)

    for input_filename in files:
        if config.verbose: print "pv(ing)", input_filename

        input_file = open(input_filename+'.sort', 'r')
        output_file = open(input_filename+'.pv', 'w')

        #nginx_parse.parse(input_file, pv_filter)
        norm_parse.parse(input_file, pv_filter, output_file)
    
        print_users(output_file)

        input_file.close()
        output_file.close()

        if config.output_file_stdout:
            with open(input_filename+'.pv', "r") as f:
                shutil.copyfileobj(f, sys.stdout)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-o', '--output',
        help='Specifies the output file.  The default is stdout.')
    parser.add_option('-f', '--filter',
        help='Specifies the file containing the msisdn to filter. The default is no filter list.')
    parser.add_option('-s', '--summary',
        help='List a summary of the top. The default is YES.')
    parser.add_option('-l', '--session_length',
        help='The session length in minutes. Default is 20 minutes')
    parser.add_option('-p', '--coverage',
        help='A number from 1 to 100 to define the % of users to scan. The default coverage is 100.')


    options, arguments = parser.parse_args()

    if options.output and options.output != '-':
        output=sys.stdout = open(options.output, 'w')

    if options.coverage and options.coverage != '-':
        coverage=int(options.coverage)
        if coverage < 1:
            coverage=1
        elif coverage > 100:
            coverage=100

    if options.session_length and options.session_length != '-':
        session_length= int(options.session_length)*60 # store the session length in seconds

    # Filename on the command line have precedence over configuration
    files = config.input_files
    if arguments:
        files = arguments

    run(files)
