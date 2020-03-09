#!/usr/bin/python -u

import os
import re
import sys
import time
import getopt
import subprocess

from PadmeMCDB import PadmeMCDB

# Create global handler to PadmeMCDB
DB = PadmeMCDB()

# List of available sites
SITE_LIST = [ "LNF", "LNF2", "CNAF", "CNAF2", "KLOE" , "LOCAL" ]

# User running CDR
CDR_USER = os.environ['USER']

# Access information for KLOE tape library
KLOE_SERVER = "fibm15"
KLOE_USER = "pdm"
KLOE_KEYFILE = "/home/%s/.ssh/id_rsa_cdr"%CDR_USER

# SRM addresses for storage elements at LNF and CNAF
#LNF_SRM = "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org"
LNF_SRM = "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org"
LNF2_SRM = "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org_scratch"
CNAF_SRM = "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape"
CNAF2_SRM = "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padme"

# Timeout for gfal-ls and gfal-sum commands (in seconds)
GFAL_TIMEOUT = 600

def print_help():
    print 'VerifyProd -P prod_name [-S src_site] [-s src_dir] [-c] [-v] [-h]'
    print '  -P prod_name    Name of production to verify'
    print '  -S src_site     Source site.'
    print '  -s src_dir      Path to data directory if source is LOCAL.'
    print '  -c              Enable checksum verification (very time consuming!)'
    print '  -v              Enable verbose mode (repeat to increase level)'
    print '  -h              Show this help message and exit'
    print '  Available sites:   %s'%SITE_LIST

def end_error(msg):
    print msg
    print_help()
    sys.exit(2)

def get_checksum_srm(file,year,srm):
    a32 = ""
    path = "/daq/%s/rawdata/%s"%(year,file)
    cmd = "gfal-sum -t %d %s%s adler32"%(GFAL_TIMEOUT,srm,path);
    for line in run_command(cmd):
        m = re.match("^gfal-sum error: (\d+) \((.*)\) - ",line)
        if (m):
            err_code = m.group(1)
            err_msg = m.group(2)
            print line.rstrip()                
            break
        m = re.match("^\s*\S+\s+(\S+)\s*$",line.rstrip())
        if (m): a32 = m.group(1)
    return a32

def get_file_list_local(prod_dir,loc_dir):
    file_list = []
    file_size = {}
    missing = False
    run_dir = "%s/%s"%(loc_dir,prod_dir)
    cmd = "/bin/bash -c \'( cd %s; ls -l )\'"%run_dir
    for line in run_command(cmd):
        if ( re.match("^ls: cannot access ",line) ):
            missing = True
            break
        if ( re.match("^.*No such file or directory",line) ):
            missing = True
            break
        m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s*$",line.rstrip())
        if (m):
            file_list.append(m.group(2))
            file_size[m.group(2)] = int(m.group(1))
    return (missing,file_list,file_size)

def get_file_list_srm(prod_dir,srm):
    file_list = []
    file_size = {}
    missing = False
    cmd = "gfal-ls -t %d -l %s%s"%(GFAL_TIMEOUT,srm,prod_dir)
    for line in run_command(cmd):
        m = re.match("^gfal-ls error:\s+(\d+)\s+\((.*)\) - ",line)
        if (m):
            err_code = m.group(1)
            err_msg = m.group(2)
            if (err_code != "2"): print line.rstrip()                
            missing = True
            break
        m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s*$",line.rstrip())
        if (m):
            file_list.append(m.group(2))
            file_size[m.group(2)] = int(m.group(1))
    return (missing,file_list,file_size)

def get_file_list_kloe(prod_dir,year):
    file_list = []
    file_size = {}
    missing = False
    kloe_ssh = "ssh -n -i %s -l %s %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER)
    cmd = "%s \'( dsmc query archive /pdm/padme/%s/\*.root )\'"%(kloe_ssh,prod_dir)
    for line in run_command(cmd):
        if ( re.match("^ANS1092W No files matching search criteria were found",line) ):
            missing = True
            break
        #m = re.match("^\s*(\S+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s+.*$",line.rstrip())
        m = re.match("^\s*([0-9,]+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s+.*$",line.rstrip())
        if (m):
            #print "Match %s %s - %s\n"%(m.group(1),m.group(2),line.rstrip())
            file_name = os.path.basename(m.group(2))
            file_list.append(file_name)
            file_size[file_name] = int(m.group(1).replace(',',''))
    return (missing,file_list,file_size)

def main(argv):

    prod = ""
    src_site = "CNAF"
    src_dir = ""
    checksum = False
    verbose = 0

    try:
        opts,args = getopt.getopt(argv,"P:S:s:cvh")
    except getopt.GetoptError as err:
        end_error("ERROR - %s"%err)

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
        elif opt == '-s':
            src_dir = arg
        elif opt == '-c':
            checksum = True
        elif opt == '-v':
            verbose += 1

    if (not prod):
        end_error("ERROR - No production specified")

    if (src_site == "LOCAL"):
        if (src_dir == ""):
            print "WARNING: source is LOCAL but no dir specified. Using current directory"
            src_dir = "."

    if ( src_site == "KLOE" and checksum ):
        print "WARNING - KLOE site does not support checksum verification: switching checksum off"
        checksum = False

    # This can be fixed by using adler32 from padme-cdr
    if ( src_site == "LOCAL" and checksum ):
        print "WARNING - LOCAL site does not support checksum verification: switching checksum off"
        checksum = False

    if verbose:
        print
        print "=== VerifyProd %s at %s ==="%(prod,src_site)
        if checksum:
            print "WARNING - Checksum is enabled: verification will take a long time..."

    # Get from DB directory where file are stored
    prod_dir = DB.get_prod_dir(prod)

    # Get from DB list of files in production
    prod_file_list = DB.get_prod_file_list(prod)
    prod_file_list.sort()

    # Get from DB attributes (size,adler32) of files in production
    (prod_file_size,prod_file_checksum) = DB.get_prod_files_attr(prod)

    # Close DB connection as we do not need it anymore
    DB.close_db()

    # Get list of files at source site
    if (src_site == "LNF"):
        (src_missing,src_file_list,src_file_size) = get_file_list_srm(prod_dir,LNF_SRM)
    elif (src_site == "LNF2"):
        (src_missing,src_file_list,src_file_size) = get_file_list_srm(prod_dir,LNF2_SRM)
    elif (src_site == "CNAF"):
        (src_missing,src_file_list,src_file_size) = get_file_list_srm(prod_dir,CNAF_SRM)
    elif (src_site == "CNAF2"):
        (src_missing,src_file_list,src_file_size) = get_file_list_srm(prod_dir,CNAF2_SRM)
    elif (src_site == "KLOE"):
        (src_missing,src_file_list,src_file_size) = get_file_list_kloe(prod_dir)
    elif (src_site == "LOCAL"):
        (src_missing,src_file_list,src_file_size) = get_file_list_local(prod_dir,src_dir)
    if verbose:
        if src_missing:
            print "%s - at %-13s production %s is missing"%(now_str(),src_site,prod)
        else:
            print "%s - at %-13s production %s contains %d files"%(now_str(),src_site,prod,len(src_file_list))

    if src_missing:
        print "=== Production %s - ERROR: missing at %s ==="%(prod,src_site)
        sys.exit()

    if (len(src_file_list) != len(prod_file_list)):
        if verbose: print "%s - production %s has %d files but only %d are at %s"%(now_str(),prod,len(prod_file_list),len(src_file_list),src_site)

    # Check file lists for differences
    if (verbose > 1): print "%s - Starting verification of production %s (%d files) at %s"%(now_str(),prod,len(prod_file_list),src_site)
    warnings = 0
    miss_at_src = 0
    wrong_size = 0
    miss_chksum_src = 0
    wrong_checksum = 0
    for rawfile in prod_file_list:

        # Check if file is at source site
        if not rawfile in src_file_list:
            warnings += 1
            miss_at_src += 1
            if verbose: print "%s - not at %s"%(rawfile,src_string)

        # Check if files have the right size
        elif src_file_size[rawfile] != prod_file_size[rawfile]:
            warnings += 1
            wrong_size += 1
            if verbose: print "%s - wrong file size: expect %d but %d at %s"%(rawfile,prod_file_size[rawfile],src_file_size[rawfile],src_site)

        else:

            # Verify checksum only if requested by the user
            if checksum:

                # Get checksum at source site
                src_checksum = ""
                if (src_site == "LNF"):
                    src_checksum = get_checksum_srm("%s/%s"%(prod_dir,rawfile),LNF_SRM)
                elif (src_site == "LNF2"):
                    src_checksum = get_checksum_srm("%s/%s"%(prod_dir,rawfile),LNF2_SRM)
                elif (src_site == "CNAF"):
                    src_checksum = get_checksum_srm("%s/%s"%(prod_dir,rawfile),CNAF_SRM)
                elif (src_site == "CNAF2"):
                    src_checksum = get_checksum_srm("%s/%s"%(prod_dir,rawfile),CNAF2_SRM)

                # Check if checksums are consistent
                if (src_checksum == ""):
                    miss_chksum_src += 1
                    warnings += 1
                    if verbose: print "%s - unable to get checksum at %s"%(rawfile,src_site)
                elif (src_checksum != checksum[rawfile]):
                    wrong_checksum += 1
                    warnings += 1
                    if verbose: print "%s - wrong checksum: expect %s but %s found at %s"%(rawfile,checksum[rawfile],src_checksum,src_site)
                else:
                    if (verbose > 1): print "%s - OK - size %10d checksum %8s"%(rawfile,src_file_size[rawfile],src_checksum)

            else:
                if (verbose > 1): print "%s - OK - size %10d"%(rawfile,src_file_size[rawfile])

    if warnings:
        report = ""
        if  miss_at_src: report += " - %s: %d missing"%(src_site,miss_at_src)
        if  wrong_size: report += " - %d wrong size"%wrong_size
        if checksum:
            if  miss_chksum_src: report += " - %s: %d no checksum"%(src_site,miss_chksum_src)
            if  wrong_checksum: report += " - %d wrong checksum"%wrong_checksum
        print "=== Production %s - WARNING: CHECK FAILED at %s%s ==="%(prod,src_site,report)
    else:
        print "=== Production %s - Successful check at %s ==="%(run,src_site)

def run_command(command):
    #print "> %s"%command
    p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    return iter(p.stdout.readline, b'')

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
