#!/usr/bin/python

import sys
import datetime
import fnmatch
import time
from os import system, walk, stat, remove, environ
from os.path import join, dirname
from shutil import copyfile
from re import match
from subprocess import check_call

try:
  import yaml
except ImportError, e:
    print "You do not have yaml module installed, please run 'sudo easy_install pyaml' to install it"
    sys.exit(0)

try:
  import boto
except ImportError, e:
    print "You do not have boto module installed, please run 'sudo easy_install boto' to install it"
    sys.exit(0)


now = datetime.datetime.now()
timestamp = time.time()
filedate = now.strftime("%Y-%m-%d_%H-%M-%S")
filedate_no_sec = now.strftime("%Y-%m-%d_%H-%M")

gnconfig_file = open(join(dirname(__file__), 'noris.yml'), 'r')
gnconfig = yaml.load(gnconfig_file)
gnconfig_file.close()

config_file = open(join(dirname(__file__), 'noris-%s.yml' % environ.get('USER')), 'r')
config = yaml.load(config_file)
config_file.close()

backups = config.get('backup', [])
rotates = config.get('rotate_log', [])
gzips = config.get('gzip_log', [])
purges = config.get('purge_log', [])

def putting_on_s3(src,dst):
    bucket_name = dst.split("/")[0]
    dstfile = dst.replace("%s/" % bucket_name,"")
    conn = boto.connect_s3(gnconfig["general"]["aws_access_key"], gnconfig["general"]["aws_secret_key"])
    bucket = conn.get_bucket(bucket_name)
    print 'Uploading %s to Amazon S3 bucket s3://%s' % (src, dst)
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    from boto.s3.key import Key
    k = Key(bucket)
    k.key = dstfile
    k.set_contents_from_filename(src,cb=percent_cb, num_cb=10)

def check_execution_time(dom, dow, mon, hour, minute):
    # Can I execute this in that or those day(s) or time?
    ## Matching days of Months
    dom_match = False
    for day in dom:
        if ( day == now.day ) or ( day == 'All' ):
            dom_match = True

    ## Matching days of Week
    dow_match = False
    for day in dow:
        if ( day == now.strftime("%a") ) or ( day == 'All' ):
            dow_match = True
    
    ## Matching Months
    mon_match = False
    for month in mon:
        if ( month == now.strftime("%b") ) or ( month == 'All' ):
            mon_match = True
    
    ## Matching Hours
    hr_match = False
    for hr in hour:
        if ( hr == now.hour ) or ( hr == 'All' ):
            hr_match = True
    
    ## Matching Minutes
    mn_match = False
    for mn in minute:
        if ( mn == now.minute ) or ( mn == 'All' ):
            mn_match = True

    if (dom_match) and (dow_match) and (mon_match) and (hr_match) and (mn_match):
        return True

def name_transformation(fmt,name):
    fmt = fmt.replace('%name',name)
    fmt = fmt.replace('%aaaa',"[0-9][0-9][0-9][0-9]")
    fmt = fmt.replace('%aa',"[0-9][0-9]")
    fmt = fmt.replace('%mm',"[0-9][0-9]")
    fmt = fmt.replace('%dd',"[0-9][0-9]")
    fmt = fmt.replace('%HH',"[0-9][0-9]")
    fmt = fmt.replace('%MM',"[0-9][0-9]")
    fmt = fmt.replace('%SS',"[0-9][0-9]")
    fmt = fmt.replace('%?',"[?]")
    fmt = fmt.replace('%*',"[*]")
    return fmt

def find_files(fmt,name,src,days_before=0):
    new_fmt = name_transformation(fmt, name)
    time_days_before = timestamp-(86400*days_before)

    matches = []

    for root, dirnames, filenames in walk(src):
        for filename in fnmatch.filter(filenames, new_fmt):
            new_filename = join(root, filename)
            if stat(new_filename).st_mtime < time_days_before:
                matches.append(join(root, filename))

    return matches

for rotate in rotates:
    en = rotates[rotate]["enabled"]
    dom = rotates[rotate]["runs_at"]["days_of_month"]
    dow = rotates[rotate]["runs_at"]["days_of_week"]
    mon = rotates[rotate]["runs_at"]["months"]
    hour = rotates[rotate]["runs_at"]["hour"]
    minute = rotates[rotate]["runs_at"]["minute"]
    src = rotates[rotate]["source"]
    dst = rotates[rotate]["destination"]
    
    def rotate_file():
        copyfile(src, '%s/%s.%s' % (dst, rotate, filedate_no_sec))
        current_log = open(src, "rw+")
        current_log.truncate()
        current_log.close()

    if (en == 1) and check_execution_time(dom, dow, mon, hour, minute):
        rotate_file()

for gzip in gzips:
    en = gzips[gzip]["enabled"]
    dom = gzips[gzip]["runs_at"]["days_of_month"]
    dow = gzips[gzip]["runs_at"]["days_of_week"]
    mon = gzips[gzip]["runs_at"]["months"]
    hour = gzips[gzip]["runs_at"]["hour"]
    minute = gzips[gzip]["runs_at"]["minute"]
    name_format = gzips[gzip]["name_format"]
    src = gzips[gzip]["source"]
    dst = gzips[gzip]["destination"]
    days_before = gzips[gzip]["days_before"]

    if (en == 1) and check_execution_time(dom, dow, mon, hour, minute):
        for filename in find_files(name_format, gzip, src, days_before):
            sys.stdout.write("Gziping %s..." % filename)
            check_call(['gzip', filename])
            os.system("mv %s.gz %s" % (filename, dst))
            sys.stdout.write("[OK]")
            sys.stdout.write("\n")

for purge in purges:
    en = purges[purge]["enabled"]
    dom = purges[purge]["runs_at"]["days_of_month"]
    dow = purges[purge]["runs_at"]["days_of_week"]
    mon = purges[purge]["runs_at"]["months"]
    hour = purges[purge]["runs_at"]["hour"]
    minute = purges[purge]["runs_at"]["minute"]
    name_format = purges[purge]["name_format"]
    src = purges[purge]["source"]
    days_before = purges[purge]["days_before"]

    if (en == 1) and check_execution_time(dom, dow, mon, hour, minute):
        for filename in find_files(name_format, purge, src, days_before):
            sys.stdout.write("Throwing away %s..." % filename)
            remove(filename)
            sys.stdout.write("[OK]")
            sys.stdout.write("\n")
    
    
for bkp in backups:
    en = backups[bkp]["enabled"]
    dom = backups[bkp]["runs_at"]["days_of_month"]
    dow = backups[bkp]["runs_at"]["days_of_week"]
    mon = backups[bkp]["runs_at"]["months"]
    hour = backups[bkp]["runs_at"]["hour"]
    minute = backups[bkp]["runs_at"]["minute"]
    src = backups[bkp]["source"]
    mtd = backups[bkp]["copy_method"]
    dst = backups[bkp]["destination"]
    
    def cp_s3():
        sys.stdout.write("Initializing upload to S3...\n")
        putting_on_s3("/tmp/%s.tar.gz" % bkp, "%s/%s_%s" % (dst, bkp, filedate))
        sys.stdout.write("[OK]")
        sys.stdout.write("\n")

    def cp_disk():
        sys.stdout.write("Copying %s to %s...\n" % (bkp, dst))
        os.system("cp /tmp/%s.tar.gz %s.%s" % (bkp, dst, filedate))
        sys.stdout.write("[OK]")
        sys.stdout.write("\n")

    def cp_scp():
        sys.stdout.write("Sending %s to %s...\n" % (bkp, dst))
        os.system("scp /tmp/%s.tar.gz %s.%s" % (bkp, dst, filedate))
        sys.stdout.write("[OK]")
        sys.stdout.write("\n")

    methods = {
        "s3": cp_s3,
        "disk": cp_disk,
        "scp": cp_scp
    }

    if (en == 1) and check_execution_time(dom, dow, mon, hour, minute):
        sys.stdout.write("Creating the archive...\n")
        system("tar -zcf /tmp/%s.tar.gz %s" % (bkp, src))
        sys.stdout.write("[OK]")
        sys.stdout.write("\n")
        methods[mtd]()
        remove("/tmp/%s.tar.gz" % bkp)
