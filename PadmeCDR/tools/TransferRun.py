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

# Define correct path to TranferFile script
TRANSFERFILE = "%s/TransferFile.py"%SCRIPT_DIR

# User running CDR
CDR_USER = os.environ['USER']

# List of available sites
SITE_LIST = [ "LNF", "LNF2", "CNAF", "CNAF2", "LOCAL", "DAQ", "KLOE" ]

# Access information for DAQ data server
DAQ_USER = "daq"
DAQ_KEYFILE = "/home/%s/.ssh/id_rsa_cdr"%CDR_USER
DAQ_SERVERS = [ "l1padme3", "l1padme4" ]

# SRM addresses
SRM = {
    "LNF"   : "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org",
    "LNF2"  : "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org_scratch",
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
    print '%s -R run_name [-S src_site] [-D dst_site] [-s src_dir]  [-d dst_dir] [-j jobs] [-h]'%SCRIPT_NAME
    print '  -R run_name     Name of run to transfer'
    print '  -S src_site     Source site. Default: %s'%SRC_DEFAULT
    print '  -D dst_site     Destination site. Default: %s'%DST_DEFAULT
    print '  -s src_dir      Path to data directory if source is LOCAL, name of data server if source is DAQ.'
    print '  -d dst_dir      Path to data directory if destination is LOCAL, name of data server if destination is DAQ.'
    print '  -j jobs         Number of parallel jobs to use. Default: %s'%JOBS_DEFAULT
    print '  -h              Show this help message and exit'
    print '  Available sites:   %s'%SITE_LIST
    print '  Available DAQ servers: %s'%DAQ_SERVERS

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

def get_run_path_srm(run):
    year = get_run_year(run)
    return "/daq/%s/rawdata/%s"%(year,run)

def get_run_path_daq(run):
    year = get_run_year(run)
    return "/data/DAQ/%s/rawdata/%s"%(year,run)

def get_file_list(run,site,source):
    if site == "CNAF" or site == "CNAF2" or site == "LNF" or site == "LNF2":
        return get_file_list_srm(run,site)
    elif site == "LOCAL":
        return get_file_list_local(run,source)
    elif site == "DAQ":
        return get_file_list_daq(run,source)
    else:
        return ["error"]

def get_file_list_srm(run,site):
    file_list = []
    run_path = get_run_path_srm(run)
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

def get_file_list_local(run,loc_dir):
    file_list = []
    run_path = "%s/%s"(loc_dir,run)
    cmd = "ls %s"%run_path
    print "> %s"%cmd
    for line in run_command(cmd):
        if re.match("^ls: cannot access ",line):
            print line.rstrip()
            print "***ERROR*** ls returned error status while retrieving file list from %s"%run_path
            return ["error"]
        file_list.append(line.rstrip())
    file_list.sort()
    return file_list

def get_file_list_daq(run,server):
    file_list = []
    run_path = get_run_path_daq(run)
    cmd = "ssh -i %s -l %s %s ls %s"%(DAQ_KEYFILE,DAQ_USER,server,run_path)
    print "> %s"%cmd
    for line in run_command(cmd):
        if re.match("^ls: cannot access ",line):
            print line.rstrip()
            print "***ERROR*** ls returned error status while retrieving file list from %s"%run_path
            return ["error"]
        file_list.append(line.rstrip())
    file_list.sort()
    return file_list

def main(argv):

    run = ""
    src_site = SRC_DEFAULT
    src_dir = ""
    dst_site = DST_DEFAULT
    dst_dir = ""
    year = ""
    src_srm = ""
    dst_srm = ""
    jobs = JOBS_DEFAULT

    try:
        opts,args = getopt.getopt(argv,"R:S:D:s:d:j:h")
    except getopt.GetoptError as err:
        end_error("ERROR - %s"%err)

    for opt,arg in opts:
        if opt == '-h':
            print_help()
            sys.exit()
        elif opt == '-R':
            run = arg
        elif opt == '-S':
            if (not arg in SITE_LIST): end_error("ERROR - Invalid source site %s"%arg)
            src_site = arg
        elif opt == '-D':
            if (not arg in SITE_LIST): end_error("ERROR - Invalid destination site %s"%arg)
            dst_site = arg
        elif opt == '-s':
            src_dir = arg
        elif opt == '-d':
            dst_dir = arg
        elif opt == '-j':
            jobs = arg

    if (not run): end_error("ERROR - No run name specified")

    if (src_site == "KLOE"): end_error("ERROR - KLOE is currently not allowed as source site.")

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

    if (src_site == "DAQ"):
        if (src_dir == ""):
            end_error("ERROR - Source site is DAQ but data server was not specified")
        elif (not src_dir in DAQ_SERVERS):
           end_error("ERROR - Source site is DAQ but data server %s is unknown"%src_dir)

    if (dst_site == "DAQ"):
        if (dst_dir == ""):
            end_error("ERROR - Destination site is DAQ but data server was not specified")
        elif (not dst_dir in DAQ_SERVERS):
            end_error("ERROR - Destination site is DAQ but data server %s is unknown"%dst_dir)

    if (src_site == dst_site):
        if (src_site == "LOCAL"):
            if (src_dir == dst_dir):
               end_error("ERROR - Source and destination sites are LOCAL and directories are the same: %s and %s"%(src_dir,dst_dir))
        elif (src_site == "DAQ"):
            if (src_dir == dst_dir):
                end_error("ERROR - Source and destination sites are DAQ and data servers are the same: %s and %s"%(src_dir,dst_dir))
        else:
            end_error("ERROR - Source and destination sites are the same: %s and %s"%(src_site,dst_site))

    # Define string to use to represent sites
    src_string = src_site
    if (src_site == "DAQ" or src_site == "LOCAL"): src_string += "(%s)"%src_dir
    dst_string = dst_site
    if (dst_site == "DAQ" or dst_site == "LOCAL"): dst_string += "(%s)"%dst_dir

    print
    print "%s === TransferRun - copying run %s from %s to %s ==="%(now_str(),run,src_string,dst_string)

    file_list = get_file_list(run,src_site,src_dir)
    if file_list[0] == "error": end_error("ERROR - Unable to get list of files for run %s from %s"%(run,src_string))

    print "%s - Start copying run %s (%d files)"%(now_str(),run,len(file_list))

    cmd = "parallel -j %s %s -F {} -S %s -D %s"%(jobs,TRANSFERFILE,src_site,dst_site)
    if src_dir: cmd += " -s %s"%src_dir
    if dst_dir: cmd += " -d %s"%dst_dir
    cmd += " :::"
    for f in file_list: cmd += " %s"%f
    #print "> %s"%cmd
    for line in run_command(cmd): print line.rstrip()

    print
    print "%s === TransferRun - finished copy of run %s ==="%(now_str(),run)

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
