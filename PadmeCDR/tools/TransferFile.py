#!/usr/bin/python -u

import os
import re
import sys
import time
import getopt
import subprocess

# Get some info about running script
thisscript = sys.argv[0]
SCRIPT_PATH,SCRIPT_NAME = os.path.split(thisscript)
# Solve all symbolic links to reach installation directory
while os.path.islink(thisscript): thisscript = os.readlink(thisscript)
SCRIPT_DIR,SCRIPT_FILE = os.path.split(os.path.abspath(thisscript))
#print SCRIPT_PATH,SCRIPT_NAME,SCRIPT_DIR,SCRIPT_FILE

# List of available sites
SITE_LIST = [ "LNF", "LNF2", "CNAF", "CNAF2", "KLOE", "DAQ", "LOCAL" ]

# User running CDR
CDR_USER = os.environ['USER']

# Look for local checksum command in same dir as current script
LOCAL_ADLER32_CMD = "%s/adler32.py"%SCRIPT_DIR
if not os.access(LOCAL_ADLER32_CMD,os.X_OK): LOCAL_ADLER32_CMD = ""

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

# Special space token option needed to write data to some sites (currently only LNF2)
SPACE_TOKEN = {
    "LNF"   : "",
    "LNF2"  : "-S PADME_SCRATCH",
    "CNAF"  : "",
    "CNAF2" : ""
}

# Default source and destination
SRC_DEFAULT = "CNAF"
DST_DEFAULT = "LNF"

# Verbose level (no messages by default)
VERBOSE = 0

def print_help():
    print '%s -F file_name [-S src_site] [-D dst_site] [-s src_dir] [-d dst_dir] [-c] [-v] [-h]'%SCRIPT_NAME
    print '  -F file_name    Name of file to transfer'
    print '  -S src_site     Source site. Default: %s'%SRC_DEFAULT
    print '  -D dst_site     Destination site. Default: %s'%DST_DEFAULT
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
    print "%s > %s"%(now_str(),command)
    p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    return iter(p.stdout.readline, b'')

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

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

def get_path_daq(filename):
    run = ""
    year = ""
    m = re.match("(run_\d+_(\d\d\d\d)\d\d\d\d_\d\d\d\d\d\d)_",filename)
    if m:
        run = m.group(1)
        year = m.group(2)
    if (run and year):
        return "/data/DAQ/%s/rawdata/%s/%s"%(year,run,filename)
    else:
        return "error"

def get_path_local(filename,sdir):
    run = ""
    m = re.match("(run_\d+_\d\d\d\d\d\d\d\d_\d\d\d\d\d\d)_",filename)
    if m:
        run = m.group(1)
    if run:
        return "%s/%s/%s"%(sdir,run,filename)
    else:
        return "error"

def get_path_kloe(filename):
    run = ""
    year = ""
    m = re.match("(run_\d+_(\d\d\d\d)\d\d\d\d_\d\d\d\d\d\d)_",filename)
    if m:
        run = m.group(1)
        year = m.group(2)
    if (run and year):
        return "/pdm/padme/daq/%s/rawdata/%s/%s"%(year,run,filename)
    else:
        return "error"

def get_size_srm(filepath,site):
    size = ""
    cmd = "gfal-ls -l %s%s"%(SRM[site],filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+\S+\s*$",line.rstrip())
        if (m): size = m.group(1)
    return size

def get_size_daq(filepath,server):
    size = ""
    cmd = "ssh -i %s -l %s %s ls -l %s"%(DAQ_KEYFILE,DAQ_USER,server,filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+\S+\s*$",line.rstrip())
        if (m): size = m.group(1)
    return size

def get_size_local(filepath):
    size = ""
    cmd = "ls -l %s"%filepath
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+\S+\s*$",line.rstrip())
        if (m): size = m.group(1)
    return size

def get_size_kloe_disk(filepath):
    size = ""
    cmd = "ssh -i %s -l %s %s ls -l %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+\S+\s*$",line.rstrip())
        if (m): size = m.group(1)
    return size

def get_size_kloe_tape(filepath):
    size = ""
    cmd = "ssh -i %s -l %s %s dsmc query archive %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        m = re.match("^\s*([0-9,]+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s+.*$",line.rstrip())
        if (m and m.group(2) == filepath): size = m.group(1).replace(',','')
    return size

def get_checksum_srm(filepath,site):
    a32 = ""
    cmd = "gfal-sum %s%s adler32"%(SRM[site],filepath);
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        try:
            (fdummy,a32) = line.rstrip().split()
        except:
            a32 = ""
    return a32

def get_checksum_daq(filepath,server):
    a32 = ""
    cmd = "ssh -i %s -l %s %s %s %s"%(DAQ_KEYFILE,DAQ_USER,server,DAQ_ADLER32_CMD,filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        try:
            (a32,fdummy) = line.rstrip().split()
        except:
            a32 = ""
    return a32

def get_checksum_local(filepath):
    if not LOCAL_ADLER32_CMD: return ""
    a32 = ""
    cmd = "%s %s"%(LOCAL_ADLER32_CMD,filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        try:
            (a32,fdummy) = line.rstrip().split()
        except:
            a32 = ""
    return a32

def get_checksum_kloe(filepath):
    a32 = ""
    cmd = "ssh -i %s -l %s %s %s %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_ADLER32_CMD,filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        try:
            (a32,fdummy) = line.rstrip().split()
        except:
            a32 = ""
    return a32

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
    filepath = get_path_srm(filename)
    if filepath == "error": return ("error","","","")
    size = get_size_srm(filepath,site)
    if not size: return ("missing",filepath,"","")
    chksum = get_checksum_srm(filepath,site)
    return ("ok",filepath,size,chksum)

def check_file_daq(filename,server):
    filepath = get_path_daq(filename)
    if filepath == "error": return ("error","","","")
    size = get_size_daq(filepath,server)
    if not size: return ("missing",filepath,"","")
    chksum = get_checksum_daq(filepath,server)
    return ("ok",filepath,size,chksum)

def check_file_local(filename,sdir):
    filepath = get_path_local(filename,sdir)
    if filepath == "error": return ("error","","","")
    size = get_size_local(filepath)
    if not size: return ("missing",filepath,"","")
    chksum = get_checksum_local(filepath)
    return ("ok",filepath,size,chksum)

def check_file_kloe(filename):
    size = ""
    chksum = ""
    filepath = get_path_kloe(filename)
    if filepath == "error": return ("error","","","")
    # First check if file is on disk
    size = get_size_kloe_disk(filepath)
    if size:
        # File is on disk: get its checksum
        chksum = get_checksum_kloe(filepath)
    else:
        # File is not on disk: see if it is on tape
        size = get_size_kloe_tape(filepath)
        if not size: return ("missing",filepath,size,chksum)
        # Unfortunately if file is on tape there is no way to get its checksum
    return ("ok",filepath,size,chksum)

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
        # LOCAL -> KLOE
        elif (dst_site == "KLOE"):
            return copy_file_local_kloe(filename,src_dir)
        # LOCAL -> LOCAL
        elif (dst_site == "LOCAL"):
            return copy_file_local_local(filename,src_dir,dst_dir)

    # Anything else is forbidden
    print "- WARNING - Copy from %s to %s is not supported"%(src_site,dst_site)
    return "error"

def copy_file_srm_srm(filename,src_site,dst_site):

    copy_failed = False
    print "%s - File %s - Starting copy from %s to %s"%(now_str(),filename,src_site,dst_site)

    filepath = get_path_srm(filename)
    cmd = "gfal-copy -t 3600 -T 3600 -p --checksum ADLER32 %s %s%s %s%s"%(SPACE_TOKEN[dst_site],SRM[src_site],filepath,SRM[dst_site],filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        if ( re.match("^gfal-copy error: ",line) or re.match("^Command timed out",line) ): copy_failed = True
    
    if copy_failed:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from %s to %s"%(now_str(),filename,src_site,dst_site)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    # Checksum verification is automatically handled by the GFAL protocol

    return "ok"

def copy_file_srm_daq(filename,src_site,daq_server):

    copy_failed = False
    print "%s - File %s - Starting copy from %s to DAQ(%s)"%(now_str(),filename,src_site,daq_server)

    src_filepath = get_path_srm(filename)
    dst_filepath = get_path_daq(filename)

    # Create destination directory on DAQ server
    dst_dir = os.path.dirname(dst_filepath)
    cmd = "ssh -i %s -l %s %s mkdir -p %s"%(DAQ_KEYFILE,DAQ_USER,daq_server,dst_dir)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Copy file from SRM to local tmp file
    tmp_file = "/tmp/%s"%filename
    cmd = "gfal-copy -t 3600 -T 3600 -p %s%s file://%s"%(SRM[src_site],src_filepath,tmp_file)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        if ( re.match("^gfal-copy error: ",line) or re.match("^Command timed out",line) ): copy_failed = True

    if copy_failed:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from %s to local file"%(now_str(),filename,src_site)
        cmd = "rm -f %s"%tmp_file
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    # Now send local copy to DAQ server using good old scp
    cmd = "scp -i %s %s %s@%s:%s"%(DAQ_KEYFILE,tmp_file,DAQ_USER,daq_server,dst_filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Clean up local temporary file
    cmd = "rm -f %s"%tmp_file
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,src_site,"")
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"DAQ",daq_server)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or a32_dst == "" or a32_src != a32_dst ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from %s to DAQ(%s)"%(now_str(),filename,src_site,daq_server)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(DAQ_KEYFILE,DAQ_USER,daq_server,dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_srm_kloe(filename,src_site):

    copy_failed = False
    print "%s - File %s - Starting copy from %s to KLOE"%(now_str(),filename,src_site)

    src_filepath = get_path_srm(filename)
    dst_filepath = get_path_kloe(filename)

    # Create destination directory on KLOE disk
    dst_dir = os.path.dirname(dst_filepath)
    cmd = "ssh -i %s -l %s %s mkdir -p %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,dst_dir)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Copy file from SRM to local tmp file
    tmp_file = "/tmp/%s"%filename
    cmd = "gfal-copy -t 3600 -T 3600 -p %s%s file://%s"%(SRM[src_site],src_filepath,tmp_file)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        if ( re.match("^gfal-copy error: ",line) or re.match("^Command timed out",line) ): copy_failed = True

    if copy_failed:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from %s to local file"%(now_str(),filename,src_site)
        for line in run_command("rm -f %s"%tmp_file): print "    %s"%line.rstrip()
        return "error"

    # Now send local copy to KLOE temporary directory using good old scp
    tmp_file_kloe = "%s/%s"%(KLOE_TMPDIR,filename)
    cmd = "scp -i %s %s %s@%s:%%s"%(KLOE_KEYFILE,tmp_file,KLOE_USER,KLOE_SERVER,tmp_file_kloe)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Clean up local temporary file
    cmd = "rm -f %s"%tmp_file
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Verify checksum
    (dum0,dum1,size_src,a32_src) = check_file(filename,src_site,"")
    size_dst = get_size_kloe_disk(tmp_file_kloe)
    a32_dst = get_checksum_kloe(tmp_file_kloe)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or a32_dst == "" or a32_src != a32_dst ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from %s to KLOE"%(now_str(),filename,src_site)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,tmp_file_kloe)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    # Move file from temporary directory to daq data directory
    cmd = "ssh -i %s -l %s %s mv %s %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,tmp_file_kloe,dst_filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    return "ok"

def copy_file_srm_local(filename,src_site,dst_dir):

    copy_failed = False
    print "%s - File %s - Starting copy from %s to LOCAL(%s)"%(now_str(),filename,src_site,dst_dir)

    src_filepath = get_path_srm(filename)
    dst_filepath = get_path_local(filename,dst_dir)
    cmd = "gfal-copy -t 3600 -T 3600 -p %s%s file://%s"%(SRM[src_site],src_filepath,dst_filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        if ( re.match("^gfal-copy error: ",line) or re.match("^Command timed out",line) ): copy_failed = True
    
    if copy_failed:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from %s to LOCAL(%s)"%(now_str(),filename,src_site,dst_dir)
        cmd = "rm -f %s"%dst_filepath
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,src_site,"")
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"LOCAL",dst_dir)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or (a32_dst != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from %s to LOCAL(%s)"%(now_str(),filename,src_site,dst_dir)
        cmd = "rm -f %s"%dst_filepath
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_daq_srm(filename,daq_server,dst_site):

    copy_failed = False
    print "%s - File %s - Starting copy from DAQ(%s) to %s"%(now_str(),filename,daq_server,dst_site)

    src_filepath = get_path_daq(filename)
    dst_filepath = get_path_srm(filename)
    cmd = "gfal-copy -t 3600 -T 3600 -p %s -D\"SFTP PLUGIN:USER=%s\" -D\"SFTP PLUGIN:PRIVKEY=%s\" sftp://%s%s %s%s"%(SPACE_TOKEN[dst_site],DAQ_USER,DAQ_KEYFILE,daq_server,src_filepath,SRM[dst_site],dst_filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        if ( re.match("^gfal-copy error: ",line) or re.match("^Command timed out",line) ): copy_failed = True

    if copy_failed:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from DAQ(%s) to %s"%(now_str(),filename,daq_server,dst_site)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"DAQ",daq_server)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,dst_site,"")
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or a32_dst == "" or a32_src != a32_dst ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from DAQ(%s) to %s"%(now_str(),filename,daq_server,dst_site)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_daq_daq(filename,src_server,dst_server):

    print "%s - File %s - Starting copy from DAQ(%s) to DAQ(%s)"%(now_str(),filename,src_server,dst_server)

    filepath = get_path_daq(filename)
    cmd = "scp -3 -i %s %s@%s%s %s@%s%s"%(DAQ_KEYFILE,DAQ_USER,src_server,filepath,DAQ_USER,dst_server,filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"DAQ",src_server)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"DAQ",dst_server)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or a32_dst == "" or a32_src != a32_dst ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from DAQ(%s) to DAQ(%s)"%(now_str(),filename,src_server,dst_server)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(DAQ_KEYFILE,DAQ_USER,dst_server,dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_daq_kloe(filename,daq_server):

    print "%s - File %s - Starting copy from DAQ(%s) to KLOE"%(now_str(),filename,daq_server)

    src_filepath = get_path_daq(filename)
    dst_filepath = get_path_kloe(filename)
    cmd = "scp -3 -i %s %s@%s%s %s@%s%s"%(DAQ_KEYFILE,DAQ_USER,daq_server,src_filepath,KLOE_USER,KLOE_SERVER,dst_filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"DAQ",daq_server)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"KLOE","")
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or a32_dst == "" or a32_src != a32_dst ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from DAQ(%s) to KLOE"%(now_str(),filename,daq_server)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_daq_local(filename,daq_server,dst_dir):

    print "%s - File %s - Starting copy from DAQ(%s) to LOCAL(%s)"%(now_str(),filename,daq_server,dst_dir)

    src_filepath = get_path_daq(filename)
    dst_filepath = get_path_local(filename,dst_dir)
    cmd = "scp -i %s %s@%s%s %s"%(DAQ_KEYFILE,DAQ_USER,daq_server,src_filepath,dst_filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"DAQ",daq_server)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"LOCAL",dst_dir)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or (a32_dst != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from DAQ(%s) to LOCAL(%s)"%(now_str(),filename,daq_server,dst_dir)
        cmd = "rm -f %s"%dst_filepath
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_local_srm(filename,src_dir,dst_site):

    copy_failed = False
    print "%s - File %s - Starting copy from LOCAL(%s) to %s"%(now_str(),filename,src_dir,dst_site)

    src_filepath = get_path_local(filename,src_dir)
    dst_filepath = get_path_srm(filename)
    cmd = "gfal-copy -t 3600 -T 3600 -p %s file://%s %s%s"%(SPACE_TOKEN[dst_site],src_filepath,SRM[dst_site],dst_filepath)
    for line in run_command(cmd):
        print "    %s"%line.rstrip()
        if ( re.match("^gfal-copy error: ",line) or re.match("^Command timed out",line) ): copy_failed = True
    
    if copy_failed:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from LOCAL(%s) to %s"%(now_str(),filename,src_dir,dst_site)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"LOCAL",src_dir)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,dst_site,"")
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_dst == "" or (a32_src != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from LOCAL(%s) to %s"%(now_str(),filename,src_dir,dst_site)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_local_daq(filename,src_dir,daq_server):

    print "%s - File %s - Starting copy from LOCAL(%s) to DAQ(%s)"%(now_str(),filename,src_dir,daq_server)

    src_filepath = get_path_local(filename,dst_dir)
    dst_filepath = get_path_daq(filename)
    cmd = "scp -i %s %s %s@%s%s"%(DAQ_KEYFILE,src_filepath,DAQ_USER,daq_server,dst_filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"LOCAL",src_dir)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"DAQ",daq_server)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_dst == "" or (a32_src != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from LOCAL(%s) to DAQ(%s)"%(now_str(),filename,src_dir,daq_server)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(DAQ_KEYFILE,DAQ_USER,daq_server,dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_local_kloe(filename,src_dir):

    print "%s - File %s - Starting copy from LOCAL(%s) to KLOE"%(now_str(),filename,src_dir)

    src_filepath = get_path_local(filename,src_dir)
    dst_filepath = get_path_kloe(filename)
    cmd = "scp -i %s %s %s@%s%s"%(KLOE_KEYFILE,src_filepath,KLOE_USER,KLOE_SERVER,dst_filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"LOCAL",src_dir)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"KLOE","")
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_dst == "" or (a32_src != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from LOCAL(%s) to KLOE"%(now_str(),filename,src_dir)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,dst_filepath)
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def copy_file_local_local(filename,src_dir,dst_dir):

    print "%s - File %s - Starting copy from LOCAL(%s) to LOCAL(%s)"%(now_str(),filename,src_dir,dst_dir)

    src_filepath = get_path_local(filename,src_dir)
    dst_filepath = get_path_local(filename,dst_dir)
    cmd = "cp %s %s"%(src_filepath,dst_filepath)
    for line in run_command(cmd): print "    %s"%line.rstrip()

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filename,"LOCAL",src_dir)
    (dum0,dum1,size_dst,a32_dst) = check_file(filename,"LOCAL",dst_dir)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filename,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or (a32_src != "" and a32_dst != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from LOCAL(%s) to LOCAL(%s)"%(now_str(),filename,src_dir,dst_dir)
        cmd = "rm -f %s"%dst_filepath
        for line in run_command(cmd): print "    %s"%line.rstrip()
        return "error"

    return "ok"

def main(argv):

    global VERBOSE

    filename = ""
    src_site = SRC_DEFAULT
    src_string = ""
    src_dir = ""
    dst_site = DST_DEFAULT
    dst_string = ""
    dst_dir = ""

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
            VERBOSE += 1

    if (not filename): end_error("ERROR - No filename specified")

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

    if (src_site == "LOCAL" or dst_site == "LOCAL"):
        if not LOCAL_ADLER32_CMD:
            print "WARNING: LOCAL version of ADLER32 program is not available. No checksum verification after copy"

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

    print
    print "%s === TransferFile %s from %s to %s ==="%(now_str(),filename,src_string,dst_string)

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
        print "%s - File %s already exists at destination site %s"%(now_str(),filename,dst_site)
        if (src_file_size != dst_file_size):
            print "WARNING - file %s exists at destination site %s but has wrong size - S: %s - D: %s"%(filename,dst_string,src_file_size,dst_file_size)
        elif (src_file_chksum and dst_file_chksum and (src_file_chksum != dst_file_chksum)):
            print "WARNING - file %s exists at destination site %s but has wrong checksum - S: %s - D: %s"%(filename,dst_string,src_file_chksum,dst_file_chksum)
        sys.exit()

    if copy_file(filename,src_site,src_dir,dst_site,dst_dir) == "ok":
        print "%s - File %s - Copy from %s to %s successful"%(now_str(),filename,src_string,dst_string)
    else:
        print "%s - File %s - Copy from %s to %s failed"%(now_str(),filename,src_string,dst_string)

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
