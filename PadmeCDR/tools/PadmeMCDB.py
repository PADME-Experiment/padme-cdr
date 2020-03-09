#!/usr/bin/python

import MySQLdb
import os
import sys
import time

class PadmeMCDB:

    def __init__(self):

        # Get DB connection parameters from environment variables
        self.DB_HOST   = os.getenv('PADME_MCDB_HOST'  ,'percona.lnf.infn.it')
        self.DB_PORT   = int(os.getenv('PADME_MCDB_PORT'  ,'3306'))
        self.DB_USER   = os.getenv('PADME_MCDB_USER'  ,'padmeMCDB')
        self.DB_PASSWD = os.getenv('PADME_MCDB_PASSWD','unknown')
        self.DB_NAME   = os.getenv('PADME_MCDB_NAME'  ,'PadmeMCDB')

        self.conn = None

    def __del__(self):

        self.close_db()

    def connect_db(self):

        self.close_db()

        try:
            self.conn = MySQLdb.connect(host   = self.DB_HOST,
                                        port   = self.DB_PORT,
                                        user   = self.DB_USER,
                                        passwd = self.DB_PASSWD,
                                        db     = self.DB_NAME)
        except:
            print "*** PadmeMCDB ERROR *** Unable to connect to DB. Exception: %s"%sys.exc_info()[0]
            sys.exit(2)

    def close_db(self):

        if (self.conn):
            self.conn.close()
            self.conn = None

    def check_db(self):

        if self.conn:
            try:
                self.conn.ping()
            except:
                self.connect_db()
        else:
            self.connect_db()

    def is_prod_in_db(self,prod_name):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT COUNT(id) FROM production WHERE name=%s""",(prod_name,))
        (n,) = c.fetchone()
        self.conn.commit()
        if n: return True
        return False

    def get_prod_dir(self,prod_name):

        prod_dir = ""
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""SELECT storage_dir FROM production WHERE name=%s""",(prod_name,))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            (prod_dir,) = c.fetchone()
        self.conn.commit()
        return prod_dir

    def get_prod_file_list(self,prod_name):

        file_list = []
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""
SELECT f.name 
FROM file f
    INNER JOIN job j ON j.id = f.job_id
    INNER JOIN production p ON p.id = j.production_id
WHERE p.name=%s
            """,(prod_name,))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            res = c.fetchall()
            for (prod_file,) in res:
                file_list.append("%s"%prod_file)
            file_list.sort()
        self.conn.commit()
        return file_list

    def get_prod_files_attr(self,prod_name):

        # Return file attributes (size and adler32 checksum) of all files in a production as dictionaries
        size = {}
        checksum = {}
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""
SELECT f.name,f.size,f.adler32 
FROM file f
    INNER JOIN job j ON j.id = f.job_id
    INNER JOIN production p ON p.id = j.production_id
WHERE p.name=%s
            """,(prod_name,))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            res = c.fetchall()
            for (file_name,file_size,file_checksum) in res:
                size[file_name] = int(file_size)
                checksum[file_name] = file_checksum
        self.conn.commit()
        return (size,checksum)
