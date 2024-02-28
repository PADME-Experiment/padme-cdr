#!/usr/bin/python -u

import os
import re
import sys
import time
import errno
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

# List of available sites
SITE_LIST = [ "LNF", "LNF2", "CNAF", "CNAF2", "KLOE", "LOCAL" ]

# User running CDR
CDR_USER = os.environ['USER']

# Look for local checksum command in same dir as current script
LOCAL_ADLER32_CMD = "%s/adler32.py"%SCRIPT_DIR
if not os.access(LOCAL_ADLER32_CMD,os.X_OK): LOCAL_ADLER32_CMD = ""

# Access information for KLOE tape library
KLOE_SERVER = "fibm15"
KLOE_USER = "pdm"
KLOE_KEYFILE = "/home/%s/.ssh/id_rsa_cdr"%CDR_USER
KLOE_ADLER32_CMD = "/pdm/bin/adler32"
KLOE_TOPDIR = "/pdm/padme"
KLOE_TMPDIR = "/pdm/tmp"

# SRM addresses
SRM = {
    #"LNF"   : "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org",
    #"LNF2"  : "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org_scratch",
    "LNF"   : "root://atlasse.lnf.infn.it//dpm/lnf.infn.it/home/vo.padme.org",
    "LNF2"  : "root://atlasse.lnf.infn.it//dpm/lnf.infn.it/home/vo.padme.org_scratch",
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
SRC_DEFAULT = "CNAF2"
DST_DEFAULT = "CNAF"

# Verbose level (no messages by default)
VERBOSE = 0

def print_help():
    print '%s -F file_path [-S src_site] [-D dst_site] [-s src_dir] [-d dst_dir] [-c] [-v] [-h]'%SCRIPT_NAME
    print '  -F file_path    Full path (relative to storage system top dir) of file to copy. E.g. /mc/devel/prod_test/sim/prod_test_job000001.root'
    print '  -S src_site     Source site. Default: %s'%SRC_DEFAULT
    print '  -D dst_site     Destination site. Default: %s'%DST_DEFAULT
    print '  -s src_dir      Path to top dir when source is LOCAL.'
    print '  -d dst_dir      Path to top dir when destination is LOCAL.'
    print '  -v              Enable verbose mode (repeat to increase level)'
    print '  -h              Show this help message and exit'
    print '  Available sites:   %s'%SITE_LIST

def end_error(msg):
    print msg
    print_help()
    sys.exit(2)

def execute_command(command):

    print "%s > %s"%(now_str(),command)
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate()

    return (p.returncode,out,err)

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

def file_exists(filepath,site,top_dir):
    if (site == "LNF" or site == "LNF2" or site == "CNAF" or site == "CNAF2"):
        return file_exists_srm(filepath,site)
    elif (site == "LOCAL"):
        return file_exists_local(filepath,top_dir)
    elif (site == "KLOE"):
        return file_exists_kloe(filepath)

def file_exists_srm(filepath,site):
    cmd = "gfal-stat %s%s"%(SRM[site],filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0: return True
    return False

def file_exists_local(filepath,top_dir):
    full_path = "%s%s"%(top_dir,filepath)
    if os.path.exists(full_path): return True
    return False

def file_exists_kloe(filepath):

    # Check if it is on disk
    cmd = "ssh -i %s -l %s %s ls %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0: return True

    # Check if it is on tape
    cmd = "ssh -i %s -l %s %s dsmc query archive %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0: return True

    return False

def get_size_srm(filepath,site):
    size = ""
    cmd = "gfal-stat %s%s"%(SRM[site],filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            m = re.match("^\s*Size:\s+(\d+)\s.*$",line)
            if (m):
                print "    %s"%line.rstrip()
                size = m.group(1)
    else:
        print "WARNING Unable to retrieve size of %s from %s at %s"%(filepath,SRM[site],site)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return size

def get_size_local(filepath,top_dir):
    size = ""
    cmd = "ls -l %s%s"%(top_dir,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+\S+\s*$",line)
            if (m):
                print "    %s"%line.rstrip()
                size = m.group(1)
    else:
        print "WARNING Unable to retrieve size of LOCAL file %s%s"%(top_dir,filepath)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return size

def get_size_kloe_disk(filepath,top_dir):
    size = ""
    cmd = "ssh -i %s -l %s %s ls -l %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,top_dir,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            m = re.match("^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+\S+\s+\S+\s*$",line)
            if (m):
                print "    %s"%line.rstrip()
                size = m.group(1)
    else:
        print "WARNING Unable to retrieve size of %s%s from KLOE disk"%(top_dir,filepath)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return size

def get_size_kloe_tape(filepath):
    size = ""
    cmd = "ssh -i %s -l %s %s dsmc query archive %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            print "    %s"%line.rstrip()
            m = re.match("^\s*([0-9,]+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s+.*$",line.rstrip())
            if (m and m.group(2) == filepath):
                size = m.group(1).replace(',','')
    else:
        print "WARNING Unable to retrieve size of %s from KLOE tape"%filepath
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return size

def get_checksum_srm(filepath,site):
    a32 = ""
    cmd = "gfal-sum %s%s adler32"%(SRM[site],filepath);
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            print "    %s"%line.rstrip()
            try:
                (fdummy,a32) = line.rstrip().split()
            except:
                a32 = ""
    else:
        print "WARNING Unable to retrieve checksum of %s from %s at %s"%(filepath,SRM[site],site)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return a32

def get_checksum_local(filepath,sdir):
    if not LOCAL_ADLER32_CMD: return ""
    a32 = ""
    cmd = "%s %s%s"%(LOCAL_ADLER32_CMD,sdir,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            print "    %s"%line.rstrip()
            try:
                (a32,fdummy) = line.rstrip().split()
            except:
                a32 = ""
    else:
        print "WARNING Unable to retrieve checksum of LOCAL file %s%s"%(sdir,filepath)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return a32

def get_checksum_kloe(filepath,sdir):
    a32 = ""
    cmd = "ssh -i %s -l %s %s %s %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_ADLER32_CMD,sdir,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in iter(out.splitlines()):
            print "    %s"%line.rstrip()
            try:
                (a32,fdummy) = line.rstrip().split()
            except:
                a32 = ""
    else:
        print "WARNING Unable to retrieve checksum of %s%s from KLOE disk"%(sdir,filepath)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
    return a32

def check_file(filepath,site,top_dir):
    if (site == "LNF" or site == "LNF2" or site == "CNAF" or site == "CNAF2"):
        return check_file_srm(filepath,site)
    elif (site == "LOCAL"):
        return check_file_local(filepath,top_dir)
    elif (site == "KLOE"):
        return check_file_kloe(filepath)

def check_file_srm(filepath,site):
    size = get_size_srm(filepath,site)
    if not size: return ("missing",filepath,"","")
    chksum = get_checksum_srm(filepath,site)
    return ("ok",filepath,size,chksum)

def check_file_local(filepath,sdir):
    size = get_size_local(filepath,sdir)
    if not size: return ("missing",filepath,"","")
    chksum = get_checksum_local(filepath,sdir)
    return ("ok",filepath,size,chksum)

def check_file_kloe(filepath):
    size = ""
    chksum = ""
    # First check if file is on disk
    size = get_size_kloe_disk(filepath,KLOE_TOPDIR)
    if size:
        # File is on disk: get its checksum
        chksum = get_checksum_kloe(filepath,KLOE_TOPDIR)
    else:
        # File is not on disk: see if it is on tape
        size = get_size_kloe_tape(filepath)
        if not size: return ("missing",filepath,size,chksum)
        # Unfortunately if file is on tape there is no way to get its checksum
    return ("ok",filepath,size,chksum)

def copy_file(filepath,src_site,src_dir,dst_site,dst_dir):

    if (src_site == "LNF" or src_site == "LNF2" or src_site == "CNAF" or src_site == "CNAF2"):
        # LNF,LNF2,CNAF,CNAF2 -> LNF,LNF2,CNAF,CNAF2
        if (dst_site == "LNF" or dst_site == "LNF2" or dst_site == "CNAF" or dst_site == "CNAF2"):
            return copy_file_srm_srm(filepath,src_site,dst_site)
        # LNF,LNF2,CNAF,CNAF2 -> KLOE
        elif (dst_site == "KLOE"):
            return copy_file_srm_kloe(filepath,src_site)
        # LNF,LNF2,CNAF,CNAF2 -> LOCAL
        elif (dst_site == "LOCAL"):
            return copy_file_srm_local(filepath,src_site,dst_dir)

    elif (src_site == "LOCAL"):
        # LOCAL -> LNF,LNF2,CNAF,CNAF2
        if (dst_site == "LNF" or dst_site == "LNF2" or dst_site == "CNAF" or dst_site == "CNAF2"):
            return copy_file_local_srm(filepath,src_dir,dst_site)
        # LOCAL -> KLOE
        elif (dst_site == "KLOE"):
            return copy_file_local_kloe(filepath,src_dir)
        # LOCAL -> LOCAL
        elif (dst_site == "LOCAL"):
            return copy_file_local_local(filepath,src_dir,dst_dir)

    # Anything else is forbidden
    print "- WARNING - Copy from %s to %s is not supported"%(src_site,dst_site)
    return "error"

def copy_file_srm_srm(filepath,src_site,dst_site):

    print "%s - File %s - Starting copy from %s to %s"%(now_str(),filepath,src_site,dst_site)

    cmd = "gfal-copy -t 3600 -T 3600 -p --checksum ADLER32 %s %s%s %s%s"%(SPACE_TOKEN[dst_site],SRM[src_site],filepath,SRM[dst_site],filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from %s to %s"%(now_str(),filepath,src_site,dst_site)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    # Checksum verification is automatically handled by the GFAL protocol

    return "ok"

def copy_file_srm_kloe(filepath,src_site):

    print "%s - File %s - Starting copy from %s to KLOE"%(now_str(),filepath,src_site)

    # Get dir and file from path
    (dst_dir,dst_file) = os.path.split(filepath)

    # Create destination directory on KLOE disk
    cmd = "ssh -i %s -l %s %s mkdir -p %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,dst_dir)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** could not create directory %s at KLOE"%(now_str(),filepath,dst_dir)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        return "error"

    # Copy file from SRM to local tmp file
    tmp_file = "/tmp/%s/%s"%(SCRIPT_NAME,dst_file)
    cmd = "gfal-copy -t 3600 -T 3600 -p %s%s file://%s"%(SRM[src_site],filepath,tmp_file)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from %s to local file %s"%(now_str(),filepath,src_site,tmp_file)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "rm -f %s"%tmp_file
        subprocess.call(shlex.split(cmd))
        return "error"

    # Now send local copy to KLOE temporary directory using good old scp
    tmp_file_kloe = "%s/%s"%(KLOE_TMPDIR,dst_file)
    cmd = "scp -i %s %s %s@%s:%s"%(KLOE_KEYFILE,tmp_file,KLOE_USER,KLOE_SERVER,tmp_file_kloe)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** scp returned error status while copying from local file %s to KLOE tmp file %s"%(now_str(),filepath,tmp_file,tmp_file_kloe)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,tmp_file_kloe)
        subprocess.call(shlex.split(cmd))
        # Remove local temporary file
        cmd = "rm -f %s"%tmp_file
        subprocess.call(shlex.split(cmd))
        return "error"

    # Clean up local temporary file
    cmd = "rm -f %s"%tmp_file
    subprocess.call(shlex.split(cmd))

    # Verify checksum
    (dum0,dum1,size_src,a32_src) = check_file(filepath,src_site,"")
    size_dst = get_size_kloe_disk(tmp_file_kloe,"")
    a32_dst = get_checksum_kloe(tmp_file_kloe,"")
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filepath,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or a32_dst == "" or a32_src != a32_dst ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from %s to KLOE"%(now_str(),filepath,src_site)
        # Remove temporary file (if any)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,tmp_file_kloe)
        subprocess.call(shlex.split(cmd))
        return "error"

    # Move file from temporary directory to daq data directory
    cmd = "ssh -i %s -l %s %s mv %s %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,tmp_file_kloe,KLOE_TOPDIR,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** unable to move file to final position at KLOE"%(now_str(),filepath)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "ssh -i %s -l %s %s rm -f %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,filepath)
        subprocess.call(shlex.split(cmd))
        # Remove temporary file (if any)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,tmp_file_kloe)
        subprocess.call(shlex.split(cmd))
        return "error"

    return "ok"

def copy_file_srm_local(filepath,src_site,top_dir):

    print "%s - File %s - Starting copy from %s to LOCAL(%s)"%(now_str(),filepath,src_site,top_dir)

    cmd = "gfal-copy -t 3600 -T 3600 -p %s%s file://%s%s"%(SRM[src_site],filepath,top_dir,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from %s to LOCAL(%s)"%(now_str(),filepath,src_site,top_dir)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "rm -f %s%s"%(top_dir,filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filepath,src_site,"")
    (dum0,dum1,size_dst,a32_dst) = check_file(filepath,"LOCAL",top_dir)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filepath,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_src == "" or (a32_dst != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from %s to LOCAL(%s)"%(now_str(),filepath,src_site,dst_dir)
        # Remove destination file (if any)
        cmd = "rm -f %s%s"%(top_dir,filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    return "ok"

def copy_file_local_srm(filepath,top_dir,dst_site):

    print "%s - File %s - Starting copy from LOCAL(%s) to %s"%(now_str(),filepath,top_dir,dst_site)

    cmd = "gfal-copy -t 3600 -T 3600 -p %s file://%s%s %s%s"%(SPACE_TOKEN[dst_site],top_dir,filepath,SRM[dst_site],filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** gfal-copy returned error status while copying from LOCAL(%s) to %s"%(now_str(),filepath,top_dir,dst_site)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filepath,"LOCAL",top_dir)
    (dum0,dum1,size_dst,a32_dst) = check_file(filepath,dst_site,"")
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filepath,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_dst == "" or (a32_src != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from LOCAL(%s) to %s"%(now_str(),filepath,top_dir,dst_site)
        # Remove destination file (if any)
        cmd = "gfal-rm %s%s"%(SRM[dst_site],filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    return "ok"

def copy_file_local_kloe(filepath,top_dir):

    print "%s - File %s - Starting copy from LOCAL(%s) to KLOE"%(now_str(),filepath,top_dir)

    cmd = "scp -i %s %s%s %s@%s:%s%s"%(KLOE_KEYFILE,top_dir,filepath,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        print out,
    else:
        print "%s - File %s - ***ERROR*** scp returned error status while copying from LOCAL(%s) to KLOE"%(now_str(),filepath,top_dir)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "ssh -i %s -l %s %s rm -f %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filepath,"LOCAL",top_dir)
    (dum0,dum1,size_dst,a32_dst) = check_file(filepath,"KLOE","")
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filepath,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or a32_dst == "" or (a32_src != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from LOCAL(%s) to KLOE"%(now_str(),filepath,top_dir)
        cmd = "ssh -i %s -l %s %s rm -f %s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,dst_filepath)
        # Remove destination file
        cmd = "ssh -i %s -l %s %s rm -f %s%s"%(KLOE_KEYFILE,KLOE_USER,KLOE_SERVER,KLOE_TOPDIR,filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    return "ok"

def copy_file_local_local(filepath,src_dir,dst_dir):

    print "%s - File %s - Starting copy from LOCAL(%s) to LOCAL(%s)"%(now_str(),filepath,src_dir,dst_dir)

    # Make sure destination directory exists
    (file_dir,file_name) = os.path.split("%s%s"%(dst_dir,filepath))
    try:
        os.makedirs(file_dir)
    except OSError as ex:
        if ex.errno == errno.EEXIST and os.path.isdir(file_dir):
            pass
        else:
            print "%s - File %s - ***ERROR*** makedirs command failed while creating LOCAL dir %s - Error(%d) %s"%(now_str(),filepath,file_dir,ex.errno,ex.strerror)
            return "error"

    cmd = "cp -r %s%s %s%s"%(src_dir,filepath,dst_dir,filepath)
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        if out: print out,
    else:
        print "%s - File %s - ***ERROR*** copy command failed while copying from LOCAL(%s) to LOCAL(%s)"%(now_str(),filepath,src_dir,dst_dir)
        if out: print "- STDOUT -\n%s"%out,
        if err: print "- STDERR -\n%s"%err,
        # Remove destination file (if any)
        cmd = "rm -f %s"%(dst_dir,filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    # Compare source and destination
    (dum0,dum1,size_src,a32_src) = check_file(filepath,"LOCAL",src_dir)
    (dum0,dum1,size_dst,a32_dst) = check_file(filepath,"LOCAL",dst_dir)
    print "%s - File %s - Final check - Src: %s %s - Dst: %s %s"%(now_str(),filepath,size_src,a32_src,size_dst,a32_dst)
    if ( size_src != size_dst or (a32_src != "" and a32_dst != "" and a32_src != a32_dst) ):
        print "%s - File %s - ***ERROR*** file copies do not match while copying from LOCAL(%s) to LOCAL(%s)"%(now_str(),filepath,src_dir,dst_dir)
        # Remove destination file (if any)
        cmd = "rm -f %s"%(dst_dir,filepath)
        subprocess.call(shlex.split(cmd))
        return "error"

    return "ok"

def main(argv):

    global VERBOSE

    filepath = ""
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
            filepath = arg
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

    if (not filepath): end_error("ERROR - No filepath specified")

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

    if (src_site == dst_site):
        if (src_site == "LOCAL"):
            if (src_dir == dst_dir):
                end_error("ERROR - Source and destination sites are LOCAL and directories are the same: %s and %s"%(src_dir,dst_dir))
        else:
            end_error("ERROR - Source and destination sites are the same: %s and %s"%(src_site,dst_site))

    if (src_site == "KLOE"):
        end_error("ERROR - KLOE is not supported as source site")

    # Define string to use to represent sites
    src_string = src_site
    if (src_site == "LOCAL"): src_string += "(%s)"%src_dir
    dst_string = dst_site
    if (dst_site == "LOCAL"): dst_string += "(%s)"%dst_dir

    print
    print "%s === TransferFile %s from %s to %s ==="%(now_str(),filepath,src_string,dst_string)

    # Check if file exists at source site
    if not file_exists(filepath,src_site,src_dir):
        print "ERROR - file %s is missing at source site %s"%(filepath,src_string)
        sys.exit(1)

    # Check if file exists at destination site
    if file_exists(filepath,dst_site,dst_dir):

        print "%s - File %s already exists at destination site %s"%(now_str(),filepath,dst_string)

        # Get size and checksum of source and destination and issue a warning if they are not the same
        (src_status,src_file_path,src_file_size,src_file_chksum) = check_file(filepath,src_site,src_dir)
        (dst_status,dst_file_path,dst_file_size,dst_file_chksum) = check_file(filepath,dst_site,dst_dir)
        if (src_file_size != dst_file_size):
            print "WARNING - source and destination file sizes do not match - S: %s - D: %s"%(src_file_size,dst_file_size)
        elif (src_file_chksum and dst_file_chksum and (src_file_chksum != dst_file_chksum)):
            print "WARNING - source and destination file checksums do not match - S: %s - D: %s"%(src_file_chksum,dst_file_chksum)
        else:
            print "%s - The two copies look identical - S: %s %s - D: %s %s"%(now_str(),src_file_size,src_file_chksum,dst_file_size,dst_file_chksum)

        sys.exit()

    # Copy the file
    if copy_file(filepath,src_site,src_dir,dst_site,dst_dir) == "ok":
        print "%s - File %s - Copy from %s to %s successful"%(now_str(),filepath,src_string,dst_string)
    else:
        print "%s - File %s - Copy from %s to %s failed"%(now_str(),filepath,src_string,dst_string)

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
