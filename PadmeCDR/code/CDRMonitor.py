#!/usr/bin/python

import os
import sys
import re
import time
import getopt
import subprocess
import daemon
import daemon.pidfile

from Logger import Logger
from ProxyHandler import ProxyHandler

# Get position of CDR main directory from PADME_CDR_DIR environment variable
# Default to current dir if not set
cdr_dir = os.getenv('PADME_CDR_DIR',".")

# User running CDR
cdr_user = os.environ['USER']

# Path to stop_cdr file: when file appears, server will remove it and gently exit
stop_cdr_file = "%s/run/CDRMonitor.stop"%cdr_dir

# Define log and lock files
log_file = "%s/log/CDRMonitor.log"%cdr_dir
pid_file = "%s/run/CDRMonitor.lock"%cdr_dir

# Define long-term proxy location
long_proxy_file = "%s/run/long_proxy"%cdr_dir

# Define time in sec to pause between checks
monitor_pause = 300

# Access information for monitor server
monitor_server = "l0padme3"
monitor_user = "monitor"
monitor_keyfile = "/home/%s/.ssh/id_rsa_cdr"%cdr_user

# Directory where monitor file will be stored
monitor_dir = "/home/monitor/PadmeMonitor/watchdir"

# File with monitor information
monitor_file = "CDRMonitor.txt"

# Define background colors to use for none/ok/warn/alarm/off
color_none  = "#FFFFFF"
color_ok    = "#00CC00"
color_warn  = "#FFA500"
color_alarm = "#CC0000"
color_off   = "#0000CC"

disk_list = [
    {
        "Name": "l1padme3",
        "String": "l1padme3",
        "Host": "l1padme3",
        "Area": "/data",
        "User": "daq",
        "Color": "ff0000",
        "Mode": "lines"
    },
    {
        "Name": "l1padme4",
        "String": "l1padme4",
        "Host": "l1padme4",
        "Area": "/data",
        "User": "daq",
        "Color": "0000ff",
        "Mode": "lines"
    },
    {
        "Name": "l0padme1",
        "String": "l0padme1",
        "Host": "l0padme1",
        "Area": "/data",
        "User": "daq",
        "Color": "00ff00",
        "Mode": "lines"
    },
    {
        "Name": "l0padme2",
        "String": "l0padme2",
        "Host": "l0padme2",
        "Area": "/data",
        "User": "daq",
        "Color": "00ffff",
        "Mode": "lines"
    },
    {
        "Name": "padmeui",
        "String": "padmeui",
        "Host": "localhost",
        "Area": "/",
        "User": "",
        "Color": "ff00ff",
        "Mode": "lines"
    },
    {
        "Name": "data05",
        "String": "Tier2 data05",
        "Host": "localhost",
        "Area": "/data05",
        "User": "",
        "Color": "e74c3c",
        "Mode": "lines"
    }
]
for i in range(len(disk_list)):
    disk_list[i]["Timeline"] = "%s/log/timeline_%s.log"%(cdr_dir,disk_list[i]["Name"])

tape_list = [
    {
        "Name": "lnfdisk",
        "String": "LNF Disk",
        "Space": 280.,
        "Color": "ff0000",
        "Mode": "lines"
    },
    {
        "Name": "lnf2disk",
        "String": "LNF Scratch",
        "Space": 100.,
        "Color": "0000ff",
        "Mode": "lines"
    },
    {
        "Name": "cnaftape",
        "String": "CNAF Tape",
        "Space": 1780.,
        "Color": "00ff00",
        "Mode": "lines"
    },
    {
        "Name": "cnafdisk",
        "String": "CNAF Disk",
        "Space": 90.,
        "Color": "00ffff",
        "Mode": "lines"
    },
    {
        "Name": "kloetape",
        "String": "KLOE Tape",
        "Space": 600.,
        "Color": "ff00ff",
        "Mode": "lines"
    },
    {
        "Name": "kloedisk",
        "String": "KLOE Disk",
        "Space": 18.,
        "Color": "e74c3c",
        "Mode": "lines"
 }
]
for i in range(len(tape_list)):
    tape_list[i]["Timeline"] = "%s/log/timeline_%s.log"%(cdr_dir,tape_list[i]["Name"])

# Keyfile to use for data servers access. All data servers MUST accept it
daq_keyfile = "/home/%s/.ssh/id_rsa_cdr"%cdr_user

# Warning and alarm levels (in %) for DAQ disk servers
daq_level_warn = 60
daq_level_alarm = 85

################################
### LNF disk occupation data ###
################################

# URI to main PADME directory on LNF disk server
lnf_uri = "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org"

# Path to file with summary occupation info
#lnf_summary_file = "/home/%s/du-padme_dpm.ouput"%cdr_user
lnf_summary_file = "/home/%s/du-padme/padme_spazio-occupato.output"%cdr_user

################################
### LNF2 disk occupation data ###
################################

# URI to main PADME directory on LNF2 disk server
lnf2_uri = "davs://atlasse.lnf.infn.it:443/dpm/lnf.infn.it/home/vo.padme.org_scratch"

# Path to file with summary occupation info
lnf2_summary_file = "/home/%s/du-padme/padme_scratch_spazio-occupato.output"%cdr_user

#################################
### CNAF tape occupation data ###
#################################

# Path to file with summary occupation info
cnaf_summary_file = "/home/%s/du-padme/cnaf_spazio-occupato.output"%cdr_user

#################################
### CNAF disk occupation data ###
#################################

# Path to file with summary occupation info
cnaf2_summary_file = "/home/%s/du-padme/cnaf2_spazio-occupato.output"%cdr_user

##############################
### KLOE tape library data ###
##############################

# Access information for KLOE front end
kloe_server = "fibm15"
kloe_user = "pdm"
kloe_keyfile = "/home/%s/.ssh/id_rsa_cdr"%cdr_user

# SSH syntax to execute a command on KLOE front end
kloe_ssh = "ssh -i %s -l %s %s"%(kloe_keyfile,kloe_user,kloe_server)

# Path to top padme directory on KLOE front end
kloe_path = "/pdm"

# Tool to get KLOE tape occupation
kloe_tape_app = "/pdm/bin/padme_sum"

def check_stop_cdr():
    if (os.path.exists(stop_cdr_file)):
        if (os.path.isfile(stop_cdr_file)):
            print "- Stop request file %s found. Removing it and exiting..."%stop_cdr_file
            os.remove(stop_cdr_file)
        else:
            print "- WARNING - Stop request at path %s found but IT IS NOT A FILE."%stop_cdr_file
            print "- I will not try to remove it but I will exit anyway..."
        print ""
        print "=== Exiting CDRMonitor ==="
        sys.exit(0)

def main(argv):

    # Creat daemon contex for run monitoring
    context = daemon.DaemonContext(
        working_directory = cdr_dir,
        umask = 0o002,
        pidfile = daemon.pidfile.PIDLockFile(pid_file)
    )

    # Become a daemon and start the production
    print "Starting CDRMonitor in background"
    context.open()
    start_monitor()
    context.close()

def get_disk_info(disk):

    disk_total = 0.
    disk_used  = 0.
    disk_avail = 0.
    disk_usepc = 0.

    if disk["Host"] == "localhost":
        #cmd = "/bin/df -BG --output=size,used,avail,pcent %s"%disk["Area"]
        cmd = "/bin/df -BM --output=size,used,avail,pcent %s"%disk["Area"]
    else:
        daq_ssh = "ssh -i %s -l %s %s"%(daq_keyfile,disk["User"],disk["Host"])
        #cmd = "%s /bin/df -BG --output=size,used,avail,pcent %s"%(daq_ssh,disk["Area"])
        cmd = "%s /bin/df -BM --output=size,used,avail,pcent %s"%(daq_ssh,disk["Area"])
    for line in run_command(cmd):
        #rc = re.match("^\s*(\S+)G\s+(\S+)G\s+(\S+)G\s+(\d+).*",line)
        rc = re.match("^\s*(\S+)M\s+(\S+)M\s+(\S+)M\s+(\d+).*",line)
        if rc:
            disk_total = float(rc.group(1))/1024.
            disk_used  = float(rc.group(2))/1024.
            disk_avail = float(rc.group(3))/1024.
            disk_usepc = float(rc.group(4))

    return (disk_total,disk_used,disk_avail,disk_usepc,)

def get_tape_info(tape):

    if tape["Name"] == "lnfdisk":
        return get_lnfdisk_info()
    elif tape["Name"] == "lnf2disk":
        return get_lnf2disk_info()
    elif tape["Name"] == "cnaftape":
        return get_cnaftape_info()
    elif tape["Name"] == "cnafdisk":
        return get_cnafdisk_info()
    elif tape["Name"] == "kloetape":
        return get_kloetape_info()
    elif tape["Name"] == "kloedisk":
        return get_kloedisk_info()
    else:
        return "0"

def get_kloetape_info():
    tape_occ = 0.
    cmd = "%s %s"%(kloe_ssh,kloe_tape_app)
    for line in run_command(cmd):
        rc = re.match("^\S+\s+/pdm\s+(\S+)\s*$",line.rstrip())
        if rc: tape_occ = float(rc.group(1))
    return tape_occ

def get_kloedisk_info():
    disk_occ = 0.
    cmd = "%s df -g %s"%(kloe_ssh,kloe_path)
    for line in run_command(cmd):
        rc = re.match("^\S+\s+(\S+)\s+(\S+)\s+.*%s\s*$"%kloe_path,line.rstrip())
        if rc:
            disk_tot_gb = float(rc.group(1))
            disk_avl_gb = float(rc.group(2))
            disk_occ = (disk_tot_gb-disk_avl_gb)/1024.
    return disk_occ

def get_lnfdisk_info():
    disk_use = 0.
    cmd = "gfal-ls -ld %s"%lnf_uri
    for line in run_command(cmd):
        rc = re.match("^\s*\S+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+.*$",line)
        if rc:
            disk_use = float(rc.group(1))/1024./1024./1024./1024.
    return disk_use

def get_lnf2disk_info():
    disk_use = 0.
    cmd = "gfal-ls -ld %s"%lnf2_uri
    for line in run_command(cmd):
        rc = re.match("^\s*\S+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+.*$",line)
        if rc:
            disk_use = float(rc.group(1))/1024./1024./1024./1024.
    return disk_use

def get_cnaftape_info():
    tape_use = 0.
    cmd = "tail -1 %s"%cnaf_summary_file
    for line in run_command(cmd):
        rc = re.match("^\s*(\d\d\d\d\d\d\d\d)_(\d\d\d\d)\s+(\d+)\s*$",line)
        if rc:
            read_date = rc.group(1)
            read_time = rc.group(2)
            tape_use = float(rc.group(3))/1024./1024./1024./1024.
    return tape_use

def get_cnafdisk_info():
    disk_use = 0.
    cmd = "tail -1 %s"%cnaf2_summary_file
    for line in run_command(cmd):
        rc = re.match("^\s*(\d\d\d\d\d\d\d\d)_(\d\d\d\d)\s+(\d+)\s*$",line)
        if rc:
            read_date = rc.group(1)
            read_time = rc.group(2)
            disk_use = float(rc.group(3))/1024./1024./1024./1024.
    return disk_use

def append_timeline_info(tlfile,now,data_list):
    with open(tlfile,"a") as tlf:
        tlf.write("%.2f"%now)
        for d in data_list:
            tlf.write(" %.2f"%d)
        tlf.write("\n")

def format_timeline_info(timeline_file,mode,period):

    old_date = "0."
    old_used = "0."
    old_free = "0."
    old_percent = "0."

    if period == "FULL":
        start_date = 0.
    elif period == "DAY":
        start_date = time.time()-86400.
    elif period == "WEEK":
        start_date = time.time()-86400.*7
    elif period == "MONTH":
        start_date = time.time()-86400.*30
    elif period == "YEAR":
        start_date = time.time()-86400.*365
    else:
        print "- WARNING - Unknown period \"%s\" requested for timeline. Defaulting to \"FULL\"."%period
        start_date = "0."

    fmt = "["
    first = True
    used = True
    with open(timeline_file,"r") as tlf:
        for l in tlf:
            m = re.match("^(\S+) (\S+) (\S+) (\S+)",l)
            if m:

                # Extract new values
                new_date = m.group(1)
                new_used = m.group(2)
                new_free = str(float(m.group(3))-float(m.group(2)))
                new_percent = m.group(4)
                #print "%s %s ### %s %s ### %s %s ### %s %s"%(old_date,new_date,old_used,new_used,old_free,new_free,old_percent,new_percent)

                # Check if this point should be included in the plot
                try:
                    new_date_f = float(new_date)
                except ValueError:
                    new_date_f = 0.
                if new_date_f < start_date: continue

                # Do not prepend a comma before first value in list
                if first:
                    first = False
                elif used:
                    fmt += ","

                # Add new value to timeline plot only if it changed since previous reading
                if mode == "PERCENT":
                    if ( new_percent != old_percent ):
                        if not used: fmt += "[\"%s\",%s],"%(old_date,old_percent)
                        fmt += "[\"%s\",%s]"%(new_date,new_percent)
                        used = True
                    else:
                        used = False
                elif mode == "USED":
                    if ( new_used != old_used ):
                        if not used: fmt += "[\"%s\",%s],"%(old_date,old_used)
                        fmt += "[\"%s\",%s]"%(new_date,new_used)
                        used = True
                    else:
                        used = False
                elif mode == "FREE":
                    if ( new_free != old_free ):
                        if not used: fmt += "[\"%s\",%s],"%(old_date,old_free)
                        fmt += "[\"%s\",%s]"%(new_date,new_free)
                        used = True
                    else:
                        used = False

                # Store values for future checks
                old_date = new_date
                old_used = new_used
                old_free = new_free
                old_percent = new_percent

    # Last reading must be stored even if it did not change
    if not used:
        if mode == "PERCENT":
            fmt += "[\"%s\",%s]"%(old_date,old_percent)
        elif mode == "USED":
            fmt += "[\"%s\",%s]"%(old_date,old_used)
        elif mode == "FREE":
            fmt += "[\"%s\",%s]"%(old_date,old_free)

    fmt += "]"
    return fmt

def get_formatted_list(item_list,fmt,period):

    data   = "DATA [ "
    legend = "LEGEND [ "
    color  = "COLOR [ "
    mode   = "MODE [ "

    first = True
    for item in item_list:
        i_name  = item["Name"]
        i_color = item["Color"]
        i_mode  = item["Mode"]
        i_file  = item["Timeline"]
        if first:
            first = False
        else:
            data   += " , "
            legend += " , "
            color  += " , "
            mode   += " , "
        data   += format_timeline_info(i_file,fmt,period)
        legend += "\"%s\""%i_name
        color  += "\"%s\""%i_color
        mode   += "\"%s\""%i_mode

    data   += " ]\n"
    legend += " ]\n"
    color  += " ]\n"
    mode   += " ]\n"

    return (data,legend,color,mode)

def run_command(command):
    print "> %s"%command
    p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    return iter(p.stdout.readline,b'')

def start_monitor():

    # Redefine print to send output to log file
    sys.stdout = Logger(log_file)
    sys.stderr = sys.stdout
    #if mode == "i": sys.stdout.interactive = True

    # Define proxy-renewal handler
    PH = ProxyHandler()
    PH.long_proxy_file = long_proxy_file
    PH.debug = 1

    print "=== Starting CDRMonitor ==="

    while(True):

        PH.renew_voms_proxy()

        now_time = time.time()

        mh = open("/tmp/%s"%monitor_file,"w")

        mh.write("PLOTID CDR_status_1\n")
        mh.write("PLOTNAME PADME CDR Online Status - %s UTC\n"%now_str())
        mh.write("PLOTTYPE activetext\n")
        mh.write("DATA [ ")

        first = True
        for disk in disk_list:
            if first:
                first = False
            else:
                mh.write(",")
            (daq_tot,daq_use,daq_avl,opc) = get_disk_info(disk)
            try:
                daq_opc = 100.*daq_use/daq_tot
            except:
                daq_opc = opc
            daq_color = color_ok
            if (daq_opc>daq_level_warn): daq_color = color_warn
            if (daq_opc>daq_level_alarm): daq_color = color_alarm
            mh.write("{\"title\":\"%s\",\"current\":{\"value\":\"Used:%.1f GB of %.1f GB (%.1f%%)\",\"col\":\"%s\"}}"%(disk["String"],daq_use,daq_tot,daq_opc,daq_color))
            if daq_tot != 0.: append_timeline_info(disk["Timeline"],now_time,(daq_use,daq_tot,daq_opc))

        mh.write(" ]\n")

        mh.write("\n")

        mh.write("PLOTID CDR_status_2\n")
        mh.write("PLOTNAME PADME CDR Offline Status - %s UTC\n"%now_str())
        mh.write("PLOTTYPE activetext\n")
        mh.write("DATA [ ")

        first = True
        for tape in tape_list:
            if first:
                first = False
            else:
                mh.write(",")
            tape_use = get_tape_info(tape)
            tape_opc = 100.*tape_use/tape["Space"]
            tape_color = color_ok
            if (tape_use > tape["Space"]): tape_color = color_warn
            mh.write("{\"title\":\"%s\",\"current\":{\"value\":\"Used:%6.1f TB of %6.1f TB (%4.1f%%)\",\"col\":\"%s\"}}"%(tape["String"],tape_use,tape["Space"],tape_opc,tape_color))
            if tape_use != 0.: append_timeline_info(tape["Timeline"],now_time,(tape_use,tape["Space"],tape_opc))

        mh.write(" ]\n")

        mh.write("\n")

        mh.write("PLOTID CDR_DAQ_timeline\n")
        mh.write("PLOTNAME PADME CDR - DAQ Servers - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TIME_FORMAT extended\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(%)\n")
        mh.write("RANGE_Y 0. 100.\n")
        (data,legend,color,mode) = get_formatted_list(disk_list,"PERCENT","FULL")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_Tape_timeline\n")
        mh.write("PLOTNAME PADME CDR - Storage - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TIME_FORMAT extended\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(TB)\n")
        (data,legend,color,mode) = get_formatted_list(tape_list,"PERCENT","FULL")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_DAQ_timeline_DAY\n")
        mh.write("PLOTNAME PADME CDR - DAQ Servers - Daily - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(%)\n")
        mh.write("RANGE_Y 0. 100.\n")
        (data,legend,color,mode) = get_formatted_list(disk_list,"PERCENT","DAY")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_Tape_timeline_DAY\n")
        mh.write("PLOTNAME PADME CDR - Storage - Daily - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(TB)\n")
        (data,legend,color,mode) = get_formatted_list(tape_list,"PERCENT","DAY")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_DAQ_timeline_WEEK\n")
        mh.write("PLOTNAME PADME CDR - DAQ Servers - Weekly - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(%)\n")
        mh.write("RANGE_Y 0. 100.\n")
        (data,legend,color,mode) = get_formatted_list(disk_list,"PERCENT","WEEK")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_Tape_timeline_WEEK\n")
        mh.write("PLOTNAME PADME CDR - Storage - Weekly - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(TB)\n")
        (data,legend,color,mode) = get_formatted_list(tape_list,"PERCENT","WEEK")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_DAQ_timeline_MONTH\n")
        mh.write("PLOTNAME PADME CDR - DAQ Servers - Monthly - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TIME_FORMAT extended\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(%)\n")
        mh.write("RANGE_Y 0. 100.\n")
        (data,legend,color,mode) = get_formatted_list(disk_list,"PERCENT","MONTH")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_Tape_timeline_MONTH\n")
        mh.write("PLOTNAME PADME CDR - Storage - Monthly - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TIME_FORMAT extended\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(TB)\n")
        (data,legend,color,mode) = get_formatted_list(tape_list,"PERCENT","MONTH")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_DAQ_timeline_YEAR\n")
        mh.write("PLOTNAME PADME CDR - DAQ Servers - Yearly - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TIME_FORMAT extended\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(%)\n")
        mh.write("RANGE_Y 0. 100.\n")
        (data,legend,color,mode) = get_formatted_list(disk_list,"PERCENT","YEAR")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.write("\n")

        mh.write("PLOTID CDR_Tape_timeline_YEAR\n")
        mh.write("PLOTNAME PADME CDR - Storage - Yearly - %s UTC\n"%now_str())
        mh.write("PLOTTYPE timeline\n")
        mh.write("TIME_FORMAT extended\n")
        mh.write("TITLE_X Time\n")
        mh.write("TITLE_Y Occupation(TB)\n")
        (data,legend,color,mode) = get_formatted_list(tape_list,"PERCENT","YEAR")
        mh.write(mode)
        mh.write(color)
        mh.write(legend)
        mh.write(data)

        mh.close()

        cmd = "scp -i %s /tmp/%s %s@%s:%s/%s"%(monitor_keyfile,monitor_file,monitor_user,monitor_server,monitor_dir,monitor_file)
        for line in run_command(cmd): print line.rstrip()

        # Pause monitor_pause seconds while checking every 10sec for stop file
        n_pause = monitor_pause/10
        for n in range(n_pause):
            check_stop_cdr()
            time.sleep(10)

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

# Execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])
