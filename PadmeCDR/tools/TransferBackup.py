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
SITE_LIST = [ "LNF", "CNAF" ]

# SRM addresses
SRM = {
    #"LNF"   : "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org",
    "LNF"   : "root://atlasse.lnf.infn.it//dpm/lnf.infn.it/home/vo.padme.org",
    #"CNAF"  : "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape"
    "CNAF"  : "davs://xfer-archive.cr.cnaf.infn.it:8443/padmeTape"
}

# Top storage directory for backups
BACKUP_DIR = "/backup/daq"

# Default source and destination
SRC_DEFAULT = "LNF"
DST_DEFAULT = "CNAF"

# Delay between parallel job submissions
PARALLEL_DELAY = "0.1s"

def print_help():
    print '%s [-S src_site] [-D dst_site] [-h]'%SCRIPT_NAME
    print '  -S src_site     Source site. Default: %s'%SRC_DEFAULT
    print '  -D dst_site     Destination site. Default: %s'%DST_DEFAULT
    print '  -h              Show this help message and exit'
    print '  Available sites:   %s'%SITE_LIST

def end_error(msg):
    print msg
    print_help()
    sys.exit(2)

def run_command(command):
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

def get_file_list(dirf,site):
    file_list = []
    cmd = "gfal-ls %s%s"%(SRM[site],dirf)
    print "> %s"%cmd
    for line in run_command(cmd):
        if re.match("^gfal-ls error: ",line):
            #print line.rstrip()
            #print "***ERROR*** gfal-ls returned error status while retrieving file list from %s%s"%(SRM[site],run_path)
            return ["error"]
        file_list.append(line.rstrip())
    file_list.sort()
    return file_list

def main(argv):

    src_site = SRC_DEFAULT
    dst_site = DST_DEFAULT
    src_srm = ""
    dst_srm = ""

    try:
        opts,args = getopt.getopt(argv,"S:D:j:h")
    except getopt.GetoptError as err:
        end_error("ERROR - %s"%err)

    for opt,arg in opts:
        if opt == '-h':
            print_help()
            sys.exit()
        elif opt == '-S':
            if (not arg in SITE_LIST): end_error("ERROR - Invalid source site %s"%arg)
            src_site = arg
        elif opt == '-D':
            if (not arg in SITE_LIST): end_error("ERROR - Invalid destination site %s"%arg)
            dst_site = arg

    if (src_site == dst_site):
        end_error("ERROR - Source and destination sites are the same: %s and %s"%(src_site,dst_site))

    # Define string to use to represent sites
    src_string = src_site
    dst_string = dst_site

    print
    print "%s === TransferBackup - copying backups from %s to %s ==="%(now_str(),src_string,dst_string)

    src_backup_list = get_file_list(BACKUP_DIR,src_site)
    if len(src_backup_list) == 0:
        end_error("ERROR - Empty list of backup dirs from %s"%src_string)
    if src_backup_list[0] == "error":
        end_error("ERROR - Unable to get list of backup dirs from %s"%src_string)

    # Loop over all source directories
    for d in src_backup_list:

        # Get list of files in both sites
        file_list_src = get_file_list("%s/%s"%(BACKUP_DIR,d),src_site)
        if len(file_list_src) == 0:
            print "=== WARNING === Empty list of files for backup dir %s from %s"%(d,src_string)
            continue
        if file_list_src[0] == "error":
            end_error("ERROR - Unable to get list of files for backup dir %s from %s"%(d,src_string))
        file_list_dst = get_file_list("%s/%s"%(BACKUP_DIR,d),dst_site)
        if len(file_list_dst) > 0 and file_list_dst[0] == "error": file_list_dst = []

        print "=== %s - Start copying backup dir %s (%d files) ==="%(now_str(),d,len(file_list_src))

        for f in file_list_src:
            if not f in file_list_dst:
                cmd = "gfal-copy -p -t 3600 -T 3600 --checksum ADLER32 %s%s/%s/%s %s%s/%s/%s"%(SRM[src_site],BACKUP_DIR,d,f,SRM[dst_site],BACKUP_DIR,d,f)
                print "> %s"%cmd
                for line in run_command(cmd): print line.rstrip()
        
    print
    print "%s === TransferBackup - finished copy of backups ==="%now_str()

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
