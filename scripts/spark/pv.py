from pyspark import SparkConf, SparkContext, StorageLevel

import os
import re
import sys
import redis
import traceback
import datetime
import shutil
import hashlib
from optparse import OptionParser
from collections import Counter

EPOCH = datetime.datetime.utcfromtimestamp(0)

MAXIMUM_SESSION_INACTIVITY = 30*60

URL_NOT_NEEDED = ['apple-touch-icon-57x57.png','static','healthcheck','favicon.ico','ga','facebook','twitter','proxy','apple-touch-icon-72x72.png','apple-touch-icon-114x114.png','apple-touch-icon-144x144.png']
 
# Our key, value repository
try:
    redis_server = redis.Redis('127.0.0.1')
    xx_ip_saved = redis_server.hgetall('pmx:ip')
    xx_saved = redis_server.hgetall('pmx:msisdn')
    xx_rsaved = redis_server.hgetall('pmx:rmsisdn')
    xx_ua_saved = redis_server.hgetall('pmx:ua')
except:
    # Any exceptions, log it, we don't want to stop execution and initialize our key, value 
    print "REDIS exception. Running without our key, value database"
    xx_ua_saved = {}
    xx_ip_saved = {}
    xx_saved = {}
    xx_rsaved = {}  # a reverse dictionary can be used to lookup original msisdn from hashed ids


def xx_ua(useragent, keylen=32):
    ua = useragent.encode('ascii','ignore')
    try:
        if ua not in xx_ua_saved:
            m = hashlib.md5()
            m.update(ua)
            dg = m.hexdigest()[:keylen]
            xx_ua_saved[ua]=dg
            redis_server.hset('pmx:ua', ua, dg)
    except:
        traceback.print_exc()
        print type(useragent),useragent

    return xx_ua_saved[ua]


def xx_msisdn(msisdn, keylen=16):
    ms = msisdn.encode('ascii','ignore')
    try:
        if ms not in xx_saved:
            m = hashlib.md5()
            m.update(ms)
            dg = m.hexdigest()[:keylen]
            xx_saved[ms]=dg
            xx_rsaved[dg]=ms
            redis_server.hset('pmx:msisdn', ms, dg)
            redis_server.hset('pmx:rmsisdn', dg, ms)
    except:
        traceback.print_exc()
        print type(msisdn),msisdn

    return xx_saved[ms]


def xx_ip(ip_address, keylen=12):
    ip = ip_address.encode('ascii','ignore')
    try:
        if ip not in xx_ip_saved:
            m = hashlib.md5()
            m.update(ip)
            dg = m.hexdigest()[:keylen]
            xx_ip_saved[ip]=dg
            redis_server.hset('pmx:ip', ip, dg)
    except:
        traceback.print_exc()
        print type(ip_address),ip_address

    return xx_ip_saved[ip]


#
# Regular expressions
#
line_nginx_re_1 = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] (["][\w]+ (?P<url>.+)(http\/[1-5]\.[0
1]+["])) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<refferer>(\-)|(.*))["]) (["](?P<useragent>.*)["]) (?P<id>\w+)""", re.IGNORECASE)

line_nginx_re_2 = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] (["][\w]+ (?P<url>.+)(http\/[1-5]\.[0
1]+["])) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<refferer>(\-)|(.*))["]) (["](?P<useragent>.*)["]) - (?P<nexa>\w+) (["](?P<wifiid>(\
-)|(.*))["])""", re.IGNORECASE)

line_nginx_re_3 = re.compile(r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] (["][\w]+ (?P<url>.+)(http\/[1-5]\.[0
1]+["])) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<refferer>(\-)|(.*))["]) (["](?P<useragent>.*)["]) -""", re.IGNORECASE)

line_nginx_re = [line_nginx_re_1, line_nginx_re_2, line_nginx_re_3]

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

# TODO: Make sures that parse line never returns a None type as this will cause an exception later on.
# Suggestion: have a '_Error_' id
def parse(line):
    fields = None
    try :
        idx = line.find('[')
        ip = xx_ip(line[0:idx].split(' ')[0].strip())
        rest = line[idx:]

        matched = False
        for regexp in line_nginx_re:
            match = regexp.match(rest)
            if match:
                dct = match.groupdict()
                time = datetime.datetime.strptime(dct['dateandtime'].rpartition(' ')[0],  "%d/%b/%Y:%H:%M:%S" )
                path = dct['url']
                numbytes = int(dct['bytessent'])
                msisdn = xx_msisdn(get_msisdn(dct))
                retcode = int(dct['statuscode'])
                useragent_xx = xx_ua(dct['useragent'])
                useragent = dct['useragent']

                fields = '\t'.join([msisdn, str(int((time - EPOCH).total_seconds())), str(retcode), str(numbytes), ip, useragent_xx, path])

                # we have a match so set it, this is only for debugging (stderr) purposes
                matched = True

                break;

        if not matched:
            sys.stderr.write(' '.join(['No regexp match', line, '\n']))

    except re.error as e:
        print "ERROR:({0}): {1}".format(e.errno, e.strerror)
        print "ERROR:", line
        traceback.print_exc()

    return fields


def session_finder(session):
    """ session is a tuple containing the id and an array of string (original session line)""" 
    id = session[0]
    unsorted_data = session[1] 
    sorted_data = sorted(map(lambda t: (int(t.split('\t')[1]), t), unsorted_data), key=lambda k: k[0])
    start_index, total_secs, total_bytes = 0, 0, 0
    pages = Counter() 
    ua, ip, land_page = None, None, None
    for index, data in enumerate(sorted_data):
        if index > 0:
            delta = data[0] - sorted_data[index - 1][0]
            if delta > MAXIMUM_SESSION_INACTIVITY:
                yield '%s\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s' % (id, datetime.datetime.utcfromtimestamp(sorted_data[start_index][0]).strftime("%Y-%m-%d %H:%M:%S"), total_secs, total_bytes, pages['total_pages'], land_page, pages['inbox'], pages['mymedia'], pages['conversaciones'], pages['activities'], pages['home'], pages['friends'], pages['info'], pages['media'], pages['upload'], pages['avatar'], pages['publish'], pages['help'], ua, ip)
                total_secs = 0
                total_bytes = 0
                land_page = None
                pages = Counter()
                start_index = index
            else:
                total_secs += delta
        d = data[1].split('\t')
        total_bytes += int(d[3])
        path = inc_pageviews(d[6])
        if path:
            pages[path] += 1
            pages['total_pages'] += 1
        ua, ip = d[5], d[4]
        if land_page is None:
            land_page = d[6].split('?')[0] 
 
    yield '%s\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s' % (id, datetime.datetime.utcfromtimestamp(sorted_data[start_index][0]).strftime("%Y-%m-%d %H:%M:%S"), total_secs, total_bytes, pages['total_pages'], land_page, pages['inbox'], pages['mymedia'], pages['conversaciones'], pages['activities'], pages['home'], pages['friends'], pages['info'], pages['media'], pages['upload'], pages['avatar'], pages['publish'], pages['help'], ua, ip)


 
def inc_pageviews(path):
    # Remove URI query and fragment
    hierarchical_path = path.split('?')[0]

    # split again on '/'
    paths = hierarchical_path.split('/')
    if len(paths) < 2:
        return None
 
    # get root, all paths start with a '/', so item #1 is what is after the first slash 
    root = paths[1]  
    if root in URL_NOT_NEEDED: # we do not need to count this request as a pageview
        root = None
    elif root.startswith('profile'): 
        # profile is too vague so we need to add the 3rd component. 
        # The 2nd component is for the user_id like in /profile/17548/inbox,
        # /profile/17548/activities, /profile/17548/media
        if len(paths) >= 4:
            root = paths[3]        

    if root:
        root = root.strip()

    return root


def cleaning_tmp_directory(directory):
    for root, dirs, files in os.walk(directory):
        for f in files:
            print os.path.join(root, f)
            #os.unlink(os.path.join(root, f))
        for d in dirs:
            print os.path.join(root, d)
            #shutil.rmtree(os.path.join(root, d))


def run(files, output_filename=None):
   
    conf = SparkConf().setAppName("pv")
    sc = SparkContext(conf=conf)

    # clean tmp directory before running our spark tasks
    cleaning_tmp_directory(conf.get('spark.local.dir', '/tmp'))

    if output_filename is None:
        output_filename = 'output_pv'

    tf = sc.textFile(files).map(lambda line: parse(line)).filter(lambda x: x is not None)

    tf.saveAsTextFile(output_filename+'.pre', compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

    pre = tf.map(lambda s: (s.split('\t')[0].strip(), s))

    sessions = pre.groupByKey().flatMap(session_finder).saveAsTextFile(output_filename)

    sc.stop()    


def run_pre(files, output_filename=None):
   
    conf = SparkConf().setAppName("pre_pv")
    sc = SparkContext(conf=conf)

    # clean tmp directory before running our spark tasks
    cleaning_tmp_directory(conf.get('spark.local.dir', '/tmp'))

    if output_filename is None:
        output_filename = 'output_pv'

    #pre = sc.textFile(files).filter(lambda x: x is not None).map(lambda s: (s.split('\t')[0].strip(), s))
    pre = sc.textFile(files).map(lambda s: (s.split('\t')[0].strip(), s))

    sessions = pre.groupByKey().flatMap(session_finder).saveAsTextFile(output_filename)

    sc.stop()    


if __name__ == "__main__":
    parser = OptionParser()
    (options, arguments) = parser.parse_args()

    if len(arguments) == 0:
        print "Usage : pv <filename filename ...>"
        exit

    run(arguments[0])

