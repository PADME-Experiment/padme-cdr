#!/usr/bin/python -u

import os
import re
import sys
import time
import getopt
import subprocess

# List of available sites
SITE_LIST = [ "LNF", "LNF2", "CNAF", "CNAF2", "KLOE", "DAQ", "LOCAL" ]

# User running CDR
CDR_USER = os.environ['USER']

# Access information for DAQ data server
DAQ_USER = "daq"
DAQ_KEYFILE = "/home/%s/.ssh/id_rsa_cdr"%CDR_USER
DAQ_SERVERS = [ "l1padme3", "l1padme4" ]
DAQ_PATH = "/data/DAQ"
DAQ_ADLER32_CMD = "/home/daq/DAQ/tools/adler32"

# Access information for KLOE tape library
KLOE_SERVER = "fibm15"
KLOE_USER = "pdm"
KLOE_KEYFILE = "/home/%s/.ssh/id_rsa_cdr"%CDR_USER
KLOE_ADLER32_CMD = "/pdm/bin/adler32"
KLOE_TMPDIR = "/pdm/tmp"

# SRM addresses
SRM = {
    "LNF"   : "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org",
    "LNF2"  : "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org_scratch",
    "CNAF"  : "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape",
    "CNAF2" : "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padme"
}

def print_help():
    print 'TransferFile -F file_name [-S src_site] [-D dst_site] [-s src_dir] [-d dst_dir] [-c] [-v] [-h]'
    print '  -F file_name    Name of file to transfer'
    print '  -S src_site     Source site.'
    print '  -D dst_site     Destination site.'
    print '  -s src_dir      Path to data directory if source is LOCAL, name of data server if source is DAQ.'
    print '  -d dst_dir      Path to data directory if destination is LOCAL, name of data server if destination is DAQ.'
    print '  -v              Enable verbose mode (repeat to increase level)'
    print '  -h              Show this help message and exit'
    print '  Available sites:   %s'%SITE_LIST
    print '  Available DAQ servers: %s'%DAQ_SERVERS

def end_error(msg):
    print msg
    print_help()
    sys.exit(2)

def run_command(command):
    print "> %s"%command
    p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    return iter(p.stdout.readline, b'')

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

#def get_checksum_srm(file,year,srm):
#    a32 = ""
#    path = "/daq/%s/rawdata/%s"%(year,file)
#    cmd = "gfal-sum %s%s adler32"%(srm,path);
#    for line in run_command(cmd):
#        try:
#            (fdummy,a32) = line.rstrip().split()
#        except:
#            a32 = ""
#    return a32

#def get_checksum_ssh(keyfile,user,server,a32cmd,path):
#    a32 = ""
#    cmd = "ssh -n -i %s -l %s %s %s %s"%(keyfile,user,server,a32cmd,path)
#    for line in run_command(cmd):
#        try:
#            (a32,fdummy) = line.rstrip().split()
#        except:
#            a32 = ""
#    return a32

def get_path_srm(filename):
    run = ""
    year = ""
    m = re.match("(run_\d+_(\d\d\d\d)\d\d\d\d_\d\d\d\d\d\d)_",filename)
    if m:
        run = m.group(1)
        year = m.group(2)
    if (run and year):
        return "/daq/%s/rawdata/%s/%s"%(year,run,filename)
    else:
        return "error"

def check_file(filename,site,sdir):
    if (site == "LNF" or site == "LNF2" or site == "CNAF" or site == "CNAF2"):
        return check_file_srm(filename,site)
    elif (site == "DAQ"):
        return check_file_daq(filename,sdir)
    elif (site == "LOCAL"):
        return check_file_local(filename,sdir)
    elif (site == "KLOE"):
        return check_file_kloe(filename)

def check_file_srm(filename,site):
    size = ""
    chksum = ""
    filepath = get_path_srm(filename)
    if filepath == "error":
        return ("error","","","")
    cmd = "gfal-ls -l %s%s"%(SRM[site],filepath)
    for line in run_command(cmd):
        if ( re.match("^gfal-ls error: ",line) ):
            return ("missing",filepath,size,chksum)
            break
        m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s*$",line.rstrip())
        if (m):
            size = m.group(1)
    cmd = "gfal-sum %s%s adler32"%(SRM[site],filepath);
    for line in run_command(cmd):
        try:
            (fdummy,chksum) = line.rstrip().split()
        except:
            chksum = ""
    return ("ok",filepath,size,chksum)

def copy_file_srm_srm(filename,src_site,dst_site):

    copy_failed = False
    print "- File %s - Starting copy from %s to %s"%(filename,src_site,dst_site)

    filepath = get_path_srm(filename)
    cmd = "gfal-copy -t 3600 -T 3600 -p --checksum ADLER32 %s%s %s%s"%(SRM[src_site],filepath,SRM[dst_site],filepath)
    print cmd
    #for line in run_command(cmd):
    #    print line.rstrip()
    #    if ( re.match("^gfal-copy error: ",line) or re.match("^Command timed out",line) ):
    #        copy_failed = True
    #
    #if copy_failed:
    #    print "- File %s - ***ERROR*** gfal-copy returned error status while copying from %s to %s"%(rawfile,src_site,dst_site)
    #    cmd = "gfal-rm %s%s"%(SRM[dst_site],filepath)
    #    for line in self.run_command(cmd): print line.rstrip()
    #    return "error"

    return "ok"


def copy_file(filename,src_site,src_dir,dst_site,dst_dir):

    if (src_site == "LNF" or src_site == "LNF2" or src_site == "CNAF" or src_site == "CNAF2"):
        # LNF,LNF2,CNAF,CNAF2 -> LNF,LNF2,CNAF,CNAF2
        if (dst_site == "LNF" or dst_site == "LNF2" or dst_site == "CNAF" or dst_site == "CNAF2"):
            return copy_file_srm_srm(filename,src_site,dst_site)
        # LNF,LNF2,CNAF,CNAF2 -> DAQ
        elif (dst_site == "DAQ"):
            return copy_file_srm_daq(filename,src_site,dst_dir)
        # LNF,LNF2,CNAF,CNAF2 -> KLOE
        elif (dst_site == "KLOE"):
            return copy_file_srm_kloe(filename,src_site)
        # LNF,LNF2,CNAF,CNAF2 -> LOCAL
        elif (dst_site == "LOCAL"):
            return copy_file_srm_local(filename,src_site,dst_dir)

    elif (src_site == "DAQ"):
        # DAQ -> LNF,LNF2,CNAF,CNAF2
        if (dst_site == "LNF" or dst_site == "LNF2" or dst_site == "CNAF" or dst_site == "CNAF2"):
            return copy_file_daq_srm(filename,src_dir,dst_site)
        # DAQ -> DAQ
        elif (dst_site == "DAQ"):
            return copy_file_daq_daq(filename,src_dir,dst_dir)
        # DAQ -> KLOE
        elif (dst_site == "KLOE"):
            return copy_file_daq_kloe(filename,src_dir)
        # DAQ -> LOCAL
        elif (dst_site == "LOCAL"):
            return copy_file_daq_local(filename,src_dir,dst_dir)

    elif (src_site == "LOCAL"):
        # LOCAL -> LNF,LNF2,CNAF,CNAF2
        if (dst_site == "LNF" or dst_site == "LNF2" or dst_site == "CNAF" or dst_site == "CNAF2"):
            return copy_file_local_srm(filename,src_dir,dst_site)
        # LOCAL -> DAQ
        elif (dst_site == "DAQ"):
            return copy_file_local_daq(filename,src_dir,dst_dir)
        # LOCAL -> DAQ
        elif (dst_site == "KLOE"):
            return copy_file_local_kloe(filename,src_dir)
        # LOCAL -> LOCAL
        elif (dst_site == "LOCAL"):
            return copy_file_local_local(filename,src_dir,dst_dir)

    # Anything else is forbidden
    print "- WARNING - Copy from %s to %s is not supported"%(src_site,dst_site)
    return "error"

def main(argv):

    filename = ""
    src_site = "CNAF"
    src_string = ""
    src_dir = ""
    dst_site = "LNF"
    dst_string = ""
    dst_dir = ""
    verbose = 0

    try:
        opts,args = getopt.getopt(argv,"F:S:D:s:d:vh")
    except getopt.GetoptError as err:
        end_error("ERROR - %s"%err)

    for opt,arg in opts:
        if opt == '-h':
            print_help()
            sys.exit()
        elif opt == '-F':
            filename = arg
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
        elif opt == '-v':
            verbose += 1

    if (not filename):
        end_error("ERROR - No filename specified")

    if (src_site == "LOCAL"):
        if (src_dir == ""):
            print "WARNING: source is LOCAL but no dir specified. Using current directory"
            src_dir = "."

    if (dst_site == "LOCAL"):
        if (dst_dir == ""):
            print "WARNING: destination is LOCAL but no dir specified. Using current directory"
            dst_dir = "."

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

    if (src_site == "KLOE"):
        end_error("ERROR - KLOE is not supported as source site")

    # Define string to use to represent sites
    src_string = src_site
    if (src_site == "DAQ" or src_site == "LOCAL"): src_string += "(%s)"%src_dir
    dst_string = dst_site
    if (dst_site == "DAQ" or dst_site == "LOCAL"): dst_string += "(%s)"%dst_dir

    if verbose:
        print
        print "=== TransferFile %s from %s to %s ==="%(filename,src_string,dst_string)

    # Check if file exists at source site
    (src_status,src_file_path,src_file_size,src_file_chksum) = check_file(filename,src_site,src_dir)
    if src_status == "error":
        print "ERROR - file %s cannot be parsed to a valid PADME file name at source site %s"%(filename,src_site)
        sys.exit(1)
    elif src_status == "missing":
        print "ERROR - file %s is missing at source site %s"%(filename,src_string)
        sys.exit(1)

    # If file exists at destination site just return but issue a WARNING if size/checksum are not the same
    (dst_status,dst_file_path,dst_file_size,dst_file_chksum) = check_file(filename,dst_site,dst_dir)
    if dst_status == "error":
        print "ERROR - file %s cannot be parsed to a valid PADME file name at destination site %s"%(filename,dst_site)
        sys.exit(1)
    if not dst_status == "missing":
        if verbose:
            print "- File %s already exists at destination site %s"%(filename,dst_site)
        if (src_file_size != dst_file_size):
            print "WARNING - file %s exists at destination site %s but has wrong size - S: %s - D: %s"%(filename,dst_string,src_file_size,dst_file_size)
        elif (src_file_chksum and dst_file_chksum and (src_file_chksum != dst_file_chksum)):
            print "WARNING - file %s exists at destination site %s but has wrong checksum - S: %s - D: %s"%(filename,dst_string,src_file_chksum,dst_file_chksum)
        sys.exit()

    if copy_file(filename,src_site,src_dir,dst_site,dst_dir) == "ok":
        print "- File %s - Copy from %s to %s successful"%(filename,src_string,dst_string)
    else:
        print "- File %s - Copy from %s to %s failed"%(filename,src_string,dst_string)

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
