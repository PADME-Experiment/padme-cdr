#!/usr/bin/python

import re
import subprocess
import shlex
import os

class ProxyHandler:

    def __init__(self):

        # Set to 1 or more to enable printout of executed commands
        self.debug = 0

        # Define VOMS proxy validity in hours
        self.proxy_validity = 24

        # If proxy validity is less than this time (seconds), renew it
        self.proxy_renew_threshold = 3600

        # VO to use
        self.proxy_vo = "vo.padme.org"

        # Check if the VOMS proxy is stored in a non-standard position
        self.voms_proxy = os.environ.get('X509_USER_PROXY','')

        # Long term non-VOMS proxy must be defined by calling program
        self.long_proxy_file = ""

    def renew_voms_proxy(self):

        # Check if current proxy is still valid and renew it if expiration is close
        info_cmd = "voms-proxy-info --actimeleft"
        if self.voms_proxy: info_cmd += " --file %s"%self.voms_proxy
        if self.debug: print "> %s"%info_cmd
        renew = True
        p = subprocess.Popen(shlex.split(info_cmd),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        (out,err) = p.communicate()
        if self.debug >= 2:
            print "- RC: %d"%p.returncode
            print "- STDOUT -\n%s"%out
            print "- STDERR -\n%s"%err
        if p.returncode == 0:
            for l in iter(out.splitlines()):
                r = re.match("^\s*(\d+)\s*$",l)
                if r and int(r.group(1))>=self.proxy_renew_threshold:
                    renew = False
        elif p.returncode != 1:
            print "  WARNING voms-proxy-info returned error code %d"%p.returncode
            print "- STDOUT -\n%s"%out
            print "- STDERR -\n%s"%err

        if renew:
            if self.debug:
                if self.voms_proxy:
                    print "- VOMS proxy %s is missing or will expire in less than %d seconds."%(self.voms_proxy,self.proxy_renew_threshold)
                else:
                    print "- Standard VOMS proxy is missing or will expire in less than %d seconds."%self.proxy_renew_threshold
            self.create_voms_proxy()

    def create_voms_proxy(self):

        # If no long-term proxy file was defined, we cannot create the VOMS proxy
        if self.long_proxy_file == "": return

        # Create a VOMS proxy using long lived non-VOMS proxy
        if self.debug:
            if self.voms_proxy:
                print "- Creating new %s VOMS proxy from long-lived proxy %s"%(self.voms_proxy,self.long_proxy_file)
            else:
                print "- Creating new standard VOMS proxy from long-lived proxy %s"%self.long_proxy_file

        renew_cmd = "voms-proxy-init --noregen --cert %s --key %s --voms vo.padme.org --valid 24:00"%(self.long_proxy_file,self.long_proxy_file)
        if self.debug: print "> %s"%renew_cmd
        p = subprocess.Popen(shlex.split(renew_cmd),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        (out,err) = p.communicate()
        if p.returncode == 0:
            if self.debug: print out
        else:
            print "  WARNING voms-proxy-init returned error code %d"%p.returncode
            print "- STDOUT -\n%s"%out
            print "- STDERR -\n%s"%err
