#!/usr/bin/python -u

import os
import re
import sys
import time
import shlex
import getopt
import subprocess

from PadmeMCDB import PadmeMCDB

# Get some info about running script
thisscript = sys.argv[0]
SCRIPT_PATH,SCRIPT_NAME = os.path.split(thisscript)
# Solve all symbolic links to reach installation directory
while os.path.islink(thisscript): thisscript = os.readlink(thisscript)
SCRIPT_DIR,SCRIPT_FILE = os.path.split(os.path.abspath(thisscript))
#print SCRIPT_PATH,SCRIPT_NAME,SCRIPT_DIR,SCRIPT_FILE

# Define correct path to TranferFile script
COPYFILE = "%s/CopyFile.py"%SCRIPT_DIR

# Create global handler to PadmeMCDB
DB = PadmeMCDB()

# User running CDR
CDR_USER = os.environ['USER']

# List of available sites
SITE_LIST = [ "LNF", "LNF2", "CNAF", "CNAF2", "LOCAL", "KLOE" ]

# Access information for DAQ data server
DAQ_USER = "daq"
DAQ_KEYFILE = "/home/%s/.ssh/id_rsa_cdr"%CDR_USER
DAQ_SERVERS = [ "l1padme3", "l1padme4" ]

# SRM addresses
SRM = {
    "LNF"   : "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org",
    "LNF2"  : "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org_scratch",
    "CNAF"  : "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape",
    "CNAF2" : "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padme"
}

# Default source and destination
SRC_DEFAULT = "CNAF"
DST_DEFAULT = "LNF"

# Default number of jobs to use
JOBS_DEFAULT = "20"

# Verbose level (no messages by default)
VERBOSE = 0

def print_help():
    print '%s -P prod_name [-S src_site] [-D dst_site] [-s src_dir]  [-d dst_dir] [-j jobs] [-h]'%SCRIPT_NAME
    print '  -P prod_name    Name of production to copy'
    print '  -S src_site     Source site. Default: %s'%SRC_DEFAULT
    print '  -D dst_site     Destination site. Default: %s'%DST_DEFAULT
    print '  -s src_dir      Path to data directory if source is LOCAL.'
    print '  -d dst_dir      Path to data directory if destination is LOCAL.'
    print '  -j jobs         Number of parallel jobs to use. Default: %s'%JOBS_DEFAULT
    print '  -h              Show this help message and exit'
    print '  Available sites:   %s'%SITE_LIST

def end_error(msg):
    print msg
    print_help()
    sys.exit(2)

def execute_command(command):
    #print "%s > %s"%(now_str(),command)
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate()
    return (p.returncode,out,err)

def run_command(command):
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

def get_file_list(site,top_dir,prod_dir):
    if (site == "LNF" or site == "LNF2" or site == "CNAF" or site == "CNAF2"):
        return get_file_list_srm(site,prod_dir)
    elif (site == "LOCAL"):
        return get_file_list_local(top_dir,prod_dir)

def get_file_list_srm(site,prod_dir):
    file_list = []
    cmd = "gfal-ls %s%s"%(SRM[site],prod_dir)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            file_list.append("%s/%s"%(prod_dir,line))
    else:
        print "WARNING Unable to retrieve list of files in %s from %s at %s"%(prod_dir,SRM[site],site)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return file_list

def get_file_list_local(top_dir,prod_dir):
    file_list = []
    cmd = "ls %s%s"%(top_dir,prod_dir)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            file_list.append("%s/%s"%(prod_dir,line))
    else:
        print "WARNING Unable to retrieve list of LOCAL files in %s%s"%(top_dir,prod_dir)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return file_list

def main(argv):

    prod = ""
    src_site = SRC_DEFAULT
    src_dir = ""
    dst_site = DST_DEFAULT
    dst_dir = ""
    year = ""
    src_srm = ""
    dst_srm = ""
    jobs = JOBS_DEFAULT

    try:
        opts,args = getopt.getopt(argv,"P:S:D:s:d:j:h")
    except getopt.GetoptError as e:
        end_error("Option error: %s"%str(e))

    for opt,arg in opts:
        if opt == '-h':
            print_help()
            sys.exit()
        elif opt == '-P':
            prod = arg
        elif opt == '-S':
            if (not arg in SITE_LIST):
                end_error("ERROR - Invalid source site %s"%arg)
            src_site = arg
        elif opt == '-D':
            if (not arg in SITE_LIST):
                end_error("ERROR - Invalid destination site %s"%arg)
            dst_site = arg
        elif opt == '-s':
            src_dir = arg
        elif opt == '-d':
            dst_dir = arg
        elif opt == '-j':
            jobs = arg

    # Check if a production was specified
    if not prod:
        end_error("ERROR - No production name specified")

    # Check if production exists in DB
    if not DB.is_prod_in_db(prod):
        end_error("ERROR - Production %s not found in database"%prod)

    # Verify if source and destination sites are consistent
    if (src_site == "KLOE"):
        end_error("ERROR - KLOE is currently not allowed as source site.")

    if (src_site == "LOCAL"):
        if (src_dir == ""):
            print "WARNING: source is LOCAL but no dir specified. Using current directory"
            src_dir = "."
        src_dir = os.path.abspath(src_dir)

    if (dst_site == "LOCAL"):
        if (dst_dir == ""):
            print "WARNING: destination is LOCAL but no dir specified. Using current directory"
            dst_dir = "."
        dst_dir = os.path.abspath(dst_dir)

    if (src_site == dst_site):
        if (src_site == "LOCAL"):
            if (src_dir == dst_dir):
               end_error("ERROR - Source and destination sites are LOCAL and directories are the same: %s and %s"%(src_dir,dst_dir))
        else:
            end_error("ERROR - Source and destination sites are the same: %s and %s"%(src_site,dst_site))

    # Define string to use to represent sites
    src_string = src_site
    if (src_site == "LOCAL"): src_string += "(%s)"%src_dir
    dst_string = dst_site
    if (dst_site == "LOCAL"): dst_string += "(%s)"%dst_dir

    # Get from DB directory where file are stored
    prod_dir = DB.get_prod_dir(prod)

    # Get from DB list of files in production
    prod_file_list = DB.get_prod_file_list(prod)

    # Close DB connection as we do not need it anymore
    DB.close_db()

    # Get from source site the list of files available for this production
    src_file_list = get_file_list(src_site,src_dir,prod_dir)
    if not src_file_list:
        end_error("ERROR - Production %s is not available at source site %s"%(prod,src_string))

    # Check which files are really available at source site
    copy_file_list = []
    for f in prod_file_list:
        if f in src_file_list:
            copy_file_list.append(f)
        else:
            print "WARNING - File %s is missing at source site %s"%(f,src_string)
    if not copy_file_list:
        end_error("ERROR - No files for production %s are available at source site %s"%(prod,src_string))

    print
    print "%s === %s - copying production %s from %s to %s ==="%(now_str(),SCRIPT_NAME,prod,src_string,dst_string)
    print "%s - Start copying production %s (%d files)"%(now_str(),prod,len(copy_file_list))

    cmd = "parallel -j %s %s -F {} -S %s -D %s"%(jobs,COPYFILE,src_site,dst_site)
    if src_dir: cmd += " -s %s"%src_dir
    if dst_dir: cmd += " -d %s"%dst_dir
    cmd += " :::"
    for f in copy_file_list: cmd += " %s"%f
    #print "> %s"%cmd
    for line in run_command(cmd): print line.rstrip()

    print
    print "%s === %s - finished copy of production %s ==="%(now_str(),SCRIPT_NAME,prod)

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
