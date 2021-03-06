#!/usr/bin/python

import os
import sys
import time
import subprocess
import re

from Logger import Logger

class PadmeCDRList:

    def __init__(self,site,daq_server):

        # Get position of CDR main directory from PADME_CDR_DIR environment variable
        # Default to current dir if not set
        self.cdr_dir = os.getenv('PADME_CDR_DIR',".")

        # Get source and destination sites and name of data server
        self.site = site
        self.daq_server = daq_server

        # Path to long-lived generic proxy file generated by calling program
        self.long_proxy_file =  self.cdr_dir+"/run/long_proxy"

        # User running CDR
        self.cdr_user = os.environ['USER']

        # Path of current year rawdata wrt top daq directory
        self.year = time.strftime("%Y",time.gmtime())
        self.data_dir = "%s/rawdata"%self.year

        ############################
        ### DAQ data server data ###
        ############################

        # Access information for DAQ data server
        self.daq_user = "daq"
        self.daq_keyfile = "/home/%s/.ssh/id_rsa_cdr"%self.cdr_user

        # Path to top daq data directory on DAQ data server
        self.daq_path = "/data/DAQ"

        # Path to adler32 command on DAQ data server
        self.daq_adler32_cmd = "/home/daq/DAQ/tools/adler32"

        # SFTP URL for rawdata on DAQ data server
        self.daq_sftp = "sftp://%s%s"%(self.daq_server,self.daq_path)

        # SSH syntax to execute a command on the DAQ data server
        self.daq_ssh = "ssh -i %s -l %s %s"%(self.daq_keyfile,self.daq_user,self.daq_server)

        ##############################
        ### KLOE tape library data ###
        ##############################

        # Access information for KLOE front end
        self.kloe_server = "fibm15"
        self.kloe_user = "pdm"
        self.kloe_keyfile = "/home/%s/.ssh/id_rsa_cdr"%self.cdr_user

        # Path to top daq data directory on KLOE front end
        self.kloe_path = "/pdm/padme/daq"

        # Path to adler32 command on KLOE front end
        self.kloe_adler32_cmd = "/pdm/bin/adler32"

        # SFTP URL for rawdata on KLOE front end
        self.kloe_sftp = "sftp://%s%s"%(self.kloe_server,self.kloe_path)

        # SSH syntax to execute a command on KLOE front end
        self.kloe_ssh = "ssh -n -i %s -l %s %s"%(self.kloe_keyfile,self.kloe_user,self.kloe_server)

        ###################################
        ### LNF and CNAF SRM sites data ###
        ###################################

        # SRM addresses for PADME DAQ data at LNF and at CNAF
        self.lnf_srm = "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org/daq"
        self.cnaf_srm = "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape/daq"

        # Initialization is finished: start the main PadmeList program
        self.main()

    def renew_voms_proxy(self):

        # Make sure proxy is valid or renew it
        # WARNING: we assume that processing a file will take less than 2 hours!

        # Generate VOMS proxy using long lived generic proxy
        renew = True

        # Check if current proxy is still valid and renew it if less than 1 hour before it expires
        for line in self.run_command("voms-proxy-info --timeleft"):
            r = re.match("^(\d+)$",line)
            if r and int(r.group(1))>=3600: renew = False

        if renew:
            cmd = "voms-proxy-init --noregen --cert %s --key %s --voms vo.padme.org --valid 24:00"%(self.long_proxy_file,self.long_proxy_file)
            #for line in self.run_command(cmd): print(line.rstrip())
            self.run_command(cmd)

    def get_file_list_daq(self):
        self.daq_list = []
        cmd = "%s \'( cd %s/%s; find -type f -name \*.root | sed -e s+\./++ )\'"%(self.daq_ssh,self.daq_path,self.data_dir)
        for line in self.run_command(cmd):
            self.daq_list.append(line.rstrip())
        return "ok"

    def get_file_list_kloe(self):

        # Compile regexp to extract file name (improves performance)
        re_get_rawdata_file = re.compile("^.* %s/%s/(.*\.root) .*$"%(self.kloe_path,self.data_dir))

        self.kloe_list = []

        # First we get list of files currently on disk buffer
        cmd = "%s \'( cd %s/%s; find . -type f -name \*.root | sed -e s+\./++ )\'"%(self.kloe_ssh,self.kloe_path,self.data_dir)
        for line in self.run_command(cmd):
            self.kloe_list.append(line.rstrip())

        # Second we get list of files already stored on the tape library
        cmd = "%s \'( dsmc query archive -subdir=yes %s/%s/\*.root )\'"%(self.kloe_ssh,self.kloe_path,self.data_dir)
        for line in self.run_command(cmd):
            m = re_get_rawdata_file.match(line)
            if (m): self.kloe_list.append(m.group(1))

        # Remove duplicates and sort
        self.kloe_list = sorted(set(self.kloe_list))

        return "ok"

    def get_file_list_lnf(self):
        self.renew_voms_proxy()
        self.lnf_list = []

        lnf_dir_list = []
        for line in self.run_command("gfal-ls %s/%s"%(self.lnf_srm,self.data_dir)):
            if re.match("^gfal-ls error: ",line):
                print "***ERROR*** gfal-ls returned error status while retrieving run list from LNF"
                return "error"
            lnf_dir_list.append(line.rstrip())
        lnf_dir_list.sort()

        for run_dir in lnf_dir_list:
            for line in self.run_command("gfal-ls %s/%s/%s"%(self.lnf_srm,self.data_dir,run_dir)):
                if re.match("^gfal-ls error: ",line):
                    print "***ERROR*** gfal-ls returned error status while retrieving file list from run dir %s from LNF"%run_dir
                    return "error"
                self.lnf_list.append("%s/%s"%(run_dir,line.rstrip()))

        return "ok"

    def get_file_list_cnaf(self):
        self.renew_voms_proxy()
        self.cnaf_list = []

        cnaf_dir_list = []
        for line in self.run_command("gfal-ls %s/%s"%(self.cnaf_srm,self.data_dir)):
            if re.match("^gfal-ls error: ",line):
                print "***ERROR*** gfal-ls returned error status while retrieving run list from CNAF"
                return "error"
            cnaf_dir_list.append(line.rstrip())
            cnaf_dir_list.sort()

        for run_dir in cnaf_dir_list:
            for line in self.run_command("gfal-ls %s/%s/%s"%(self.cnaf_srm,self.data_dir,run_dir)):
                if re.match("^gfal-ls error: ",line):
                    print "***ERROR*** gfal-ls returned error status while retrieving file list from run dir %s from CNAF"%run_dir
                    return "error"
                self.cnaf_list.append("%s/%s"%(run_dir,line.rstrip()))

        return "ok"

    def get_checksum_cnaf(self,rawfile):
        a32 = ""
        cmd = "gfal-sum %s/%s/%s adler32"%(self.cnaf_srm,self.data_dir,rawfile);
        for line in self.run_command(cmd):
            print line.rstrip()
            try:
                (fdummy,a32) = line.rstrip().split()
            except:
                a32 = ""
        return a32

    def get_checksum_lnf(self,rawfile):
        a32 = ""
        cmd = "gfal-sum %s/%s/%s adler32"%(self.lnf_srm,self.data_dir,rawfile);
        for line in self.run_command(cmd):
            print line.rstrip()
            try:
                (fdummy,a32) = line.rstrip().split()
            except:
                a32 = ""
        return a32

    def get_checksum_daq(self,rawfile):
        a32 = ""
        cmd = "%s %s %s/%s/%s"%(self.daq_ssh,self.daq_adler32_cmd,self.daq_path,self.data_dir,rawfile)
        for line in self.run_command(cmd):
            print line.rstrip()
            try:
                (a32,fdummy) = line.rstrip().split()
            except:
                a32 = ""
        return a32

    def get_checksum_kloe(self,rawfile):
        a32 = ""
        cmd = "%s %s %s/%s/%s"%(self.kloe_ssh,self.kloe_adler32_cmd,self.kloe_path,self.data_dir,rawfile)
        for line in self.run_command(cmd):
            print line.rstrip()
            try:
                (a32,fdummy) = line.rstrip().split()
            except:
                a32 = ""
        return a32

    def run_command(self,command):
        p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
        return iter(p.stdout.readline,b'')

    def now_str(self):
        return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

    def main(self):

        print "### PadmeCDRList - Source: %s %s - %s ###"%(self.site,self.daq_server,self.now_str())

        full_list = []

        if (self.site == "DAQ"):
            if self.get_file_list_daq() == "error":
                print "ERROR - DAQ server %s has problems: aborting"%self.daq_server
                sys.exit(2)
            full_list.extend(self.daq_list)

        elif (self.site == "LNF"):
            if self.get_file_list_lnf() == "error":
                print "ERROR - LNF site has problems: aborting"
                sys.exit(2)
            full_list.extend(self.lnf_list)

        elif (self.site == "CNAF"):
            if self.get_file_list_cnaf() == "error":
                print "ERROR - CNAF site has problems: aborting"
                sys.exit(2)
            full_list.extend(self.cnaf_list)

        elif (self.site == "KLOE"):
            if self.get_file_list_kloe() == "error":
                print "ERROR - KLOE site has problems: aborting"
                sys.exit(2)
            full_list.extend(self.kloe_list)

        elif (self.site == "ALL"):

            self.daq3_list = []
            self.daq_server = "l1padme3"
            self.daq_ssh = "ssh -i %s -l %s %s"%(self.daq_keyfile,self.daq_user,self.daq_server)
            if self.get_file_list_daq() == "error":
                print "ERROR - DAQ server %s has problems: skippping"%self.daq_server
            else:
                self.daq3_list = self.daq_list
                full_list.extend(self.daq_list)

            self.daq4_list = []
            self.daq_server = "l1padme4"
            self.daq_ssh = "ssh -i %s -l %s %s"%(self.daq_keyfile,self.daq_user,self.daq_server)
            if self.get_file_list_daq() == "error":
                print "ERROR - DAQ server %s has problems: skippping"%self.daq_server
            else:
                self.daq4_list = self.daq_list
                full_list.extend(self.daq_list)

            if self.get_file_list_lnf() == "error":
                print "ERROR - LNF site has problems: skipping"
            else:
                full_list.extend(self.lnf_list)

            if self.get_file_list_cnaf() == "error":
                print "ERROR - CNAF site has problems: skipping"
            else:
                full_list.extend(self.cnaf_list)

            if self.get_file_list_kloe() == "error":
                print "ERROR - KLOE site has problems: skipping"
            else:
                full_list.extend(self.kloe_list)

        # Remove duplicates and sort final list
        full_list = sorted(set(full_list))
        for rawfile in (full_list):
            if (self.site == "ALL"):
                match = ""
                if (rawfile in self.daq3_list):
                    match += "3"
                else:
                    match += "-"
                if (rawfile in self.daq4_list):
                    match += "4"
                else:
                    match += "-"
                if (rawfile in self.lnf_list):
                    match += "L"
                else:
                    match += "-"
                if (rawfile in self.cnaf_list):
                    match += "C"
                else:
                    match += "-"
                if (rawfile in self.kloe_list):
                    match += "K"
                else:
                    match += "-"
                print rawfile,match
            else:
                print rawfile
