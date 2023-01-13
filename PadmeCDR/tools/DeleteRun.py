#!/usr/bin/python -u

import os
import re
import sys
import time
import shlex
import getopt
import subprocess

# Get some info about running script
thisscript = sys.argv[0]
SCRIPT_PATH,SCRIPT_NAME = os.path.split(thisscript)
# Solve all symbolic links to reach installation directory
while os.path.islink(thisscript): thisscript = os.readlink(thisscript)
SCRIPT_DIR,SCRIPT_FILE = os.path.split(os.path.abspath(thisscript))
#print SCRIPT_PATH,SCRIPT_NAME,SCRIPT_DIR,SCRIPT_FILE

# List of available sites (exclude local/daq dirs and tape libraries)
#SITE_LIST = [ "LNF", "LNF2", "CNAF", "CNAF2", "LOCAL", "DAQ", "KLOE" ]
SITE_LIST = [ "LNF", "LNF2", "CNAF2" ]

# SRM addresses
SRM = {
    "LNF"   : "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org",
    "LNF2"  : "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org_scratch",
    "CNAF2" : "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padme"
}

# Default source and destination
SITE_DEFAULT = "CNAF2"

# Default number of jobs to use
JOBS_DEFAULT = "1"

# Maximum number of retries before giving up
RETRIES_MAX = 10

# Verbose level (no messages by default)
VERBOSE = 0

def print_help():
    print '%s -R run_name [-S site] [-j jobs] [-h]'%SCRIPT_NAME
    print '  -R run_name     Name of run to recover'
    print '  -S src_site     Site where run must be deleted. Default: %s'%SITE_DEFAULT
    print '  -j jobs         Number of parallel jobs to use. Default: %s'%JOBS_DEFAULT
    print '  -h              Show this help message and exit'
    print '  Available sites:   %s'%SITE_LIST

def end_error(msg):
    print msg
    print_help()
    sys.exit(2)

def run_command(command):
    #print "> %s"%command
    #p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

def get_run_year(run):
    m = re.match("run_\d+_(\d\d\d\d)\d\d\d\d_\d\d\d\d\d\d",run)
    if not m: end_error("ERROR - Unable to extract year from run name %s"%run)
    return m.group(1)

def get_run_path(run):
    year = get_run_year(run)
    return "/daq/%s/rawdata/%s"%(year,run)

def get_file_list(run,site):
    file_list = []
    run_path = get_run_path(run)
    cmd = "gfal-ls %s%s"%(SRM[site],run_path)
    print "> %s"%cmd
    for line in run_command(cmd):
        if re.match("^gfal-ls error: ",line):
            print line.rstrip()
            print "***ERROR*** gfal-ls returned error status while retrieving file list from %s%s"%(SRM[site],run_path)
            return ["error"]
        file_list.append(line.rstrip())
    file_list.sort()
    return file_list

def main(argv):

    run = ""
    site = SITE_DEFAULT
    year = ""
    jobs = JOBS_DEFAULT

    try:
        opts,args = getopt.getopt(argv,"R:S:j:h")
    except getopt.GetoptError as err:
        end_error("ERROR - %s"%err)

    for opt,arg in opts:
        if opt == '-h':
            print_help()
            sys.exit()
        elif opt == '-R':
            run = arg
        elif opt == '-S':
            if (not arg in SITE_LIST): end_error("ERROR - Invalid site %s"%arg)
            site = arg
        elif opt == '-j':
            jobs = arg

    if (not run): end_error("ERROR - No run name specified")

    # Define string to use to represent sites
    site_string = site

    print
    print "%s === DeleteRun - delete run %s from %s ==="%(now_str(),run,site_string)

    # Define full run path, including SRM address
    run_path = "%s%s"%(SRM[site],get_run_path(run))

    # Get list of files
    file_list = get_file_list(run,site)
    if file_list[0] == "error": end_error("ERROR - Unable to get list of files for run %s from %s"%(run,site_string))

    retries = 0
    while len(file_list) > 0:

        if retries >= RETRIES_MAX:
            end_error("ERROR - Unable to delete run %s from %s after %d retries"%(run,site_string,retries))

        print "%s - Start deleting run %s (%d files)"%(now_str(),run,len(file_list))

        # If parallel is switched off (jobs=1) rely on "gfal-rm -r" to do the job
        if jobs > 1:
            cmd = "parallel -j %s gfal-rm {} :::"%jobs
            for f in file_list: cmd += " %s/%s"%(run_path,f)
            #print "> %s"%cmd
            for line in run_command(cmd): print line.rstrip()

        # When done, remove run directory
        cmd = "gfal-rm -r %s"%run_path
        print "> %s"%cmd
        for line in run_command(cmd): print line.rstrip()

        # Check if all files were deleted. An error means that the directory was correctly removed
        file_list = get_file_list(run,site)
        if file_list[0] == "error": break

        retires += 1

    print
    print "%s === DeleteRun - run %s deleted from %s ==="%(now_str(),run,site)

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
