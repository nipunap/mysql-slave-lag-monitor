#!/usr/bin/env python
import sys
import MySQLdb
import MySQLdb.cursors
import smtplib
import os
import datetime
import time
import logging

from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler

#	Configurations
host_list = '/[path]/conf/host_list.txt'
log_file = '/[path]/logs/slave_check.log'
#

logger = logging.getLogger('slave_shcek')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = RotatingFileHandler(log_file,maxBytes=20971520,backupCount=5)

handler.setFormatter(formatter)
logger.addHandler(handler)


email_file = '/tmp/' + str(os.getpid())
hostname = ''

def sendMail(textfile):
	try:
	        fp = open(textfile, 'rb')
	        msg = MIMEText(fp.read())
	        fp.close()
	except IOError:
		logger.error("File can not found in the location " + str(textfile))
		return 1
        me = 'alerts@gmail.com'
        you = 'nipunap@gmail.com'
        msg['Subject'] = "MySQL replication error: %s" % (hostname)
        msg['From'] = me
        msg['To'] = you

        s = smtplib.SMTP('localhost')
        s.sendmail(me, [you], msg.as_string())
        s.quit()

	try:
	    errReport("\n\nService report from AlerTs")
	    if os.path.exists(textfile):
		os.remove(textfile)
	    else:
		logger.info(str(textfile) + " is not exists")
	except OSError:
	    logger.error("File can not find to remove : OSError")
	except IOError:
	    logger.error("File can not find to remove : IOError")
	logger.info("temporary file removed")
        return 0
#---------------------------------

def executeQuery(query,host_name):
    try:
      conn = MySQLdb.connect (host=host_name,
                              port=3306,
                              user='cacti',
                              passwd='password',
                              db='mysql')
    except MySQLdb.Error, e:
      logger.error("Error %d: %s" % (e.args[0], e.args[1]))
      return 1

    try:
        cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()


    except MySQLdb.Error, e:
	logger.error("Error %d: %s > %s" % (e.args[0], e.args[1]))
        errReport("Error %d: %s > %s" % (e.args[0], e.args[1],host_name))
	sendMail(email_file)
        sys.exit (1)
  
    conn.close ()
    return row

#----------------------------------------

def errReport(report):
        f = open(email_file,'ab+')
        f.write(str(datetime.datetime.now()) + '\t' + report)
        f.close()

#--------------------------------------

def checkHosts(host):
	sqlCommand = ("SHOW SLAVE STATUS")
	host_name = host
	row = executeQuery(sqlCommand,host_name)
	seconds_behind_master = row['Seconds_Behind_Master']
	if seconds_behind_master is not None:
		logger.info("slave is runing with lag " + str(seconds_behind_master) + ' : %s' % (host_name))
		if seconds_behind_master > 200:
			errReport('MySQL slave lag is unexpectedly high : %s' % (host_name))
	else:
		errReport("\nMySQL replication is process is not running in %s \n" % (host_name))
	logger.info("email sending %s" % host)
	sendMail(email_file)

#--------------------------------------
logger.info("process started with %s" % str(os.getpid()))
file = open(host_list, 'r')
for line in file:
	if line.strip() is not None:
		logger.info("start prob on host : %s" % line.strip())
		hostname = line.strip()
		checkHosts(hostname)

