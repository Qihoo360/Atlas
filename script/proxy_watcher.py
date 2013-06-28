#!/usr/bin/env python2.6
#
# DB Proxy Alarmer
#   Support -  Slow Querys
#
# By wangchao@360.cn
#

import os, sys
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

## ====================================
slow_time = 2000

watch_path = "/usr/local/mysql-proxy/log/"
watch_instance = ["sql_s3"]
email_from = "dbproxy-alarm@360.cn"
email_to = ["wangchao3@360.cn", "zhuchao@360.cn", "guiyongzhe@360.cn", "chenchao@360.cn"]
email_admin = ["wangchao3@360.cn"]
## ====================================

def analyseLog(buffer):
    slow_querys = []

    for line in buffer:
        try:
            execute_time = float(line.split(" ")[5])
        except:
            continue

        if execute_time > slow_time:
            slow_querys.append(line)

    return slow_querys

def watchLog(filename, lastpos):
    if not lastpos:
        lastpos = 0

    if not os.path.isfile(filename):
        print "open error: " + filename
        return 0

    f = open(filename , "r")
    try:
        f.seek(int(lastpos), 0)
        buf = f.readlines()
        pos = f.tell()
    except IOError:
        print "read error : " + filename
    f.close()

    # Analyse Log
    slow_querys = analyseLog(buf)

    # Send mail
    if len(slow_querys) > 0:
        sendMail("\n".join(slow_querys))

    return pos

def sendMail(content):
    smtp = smtplib.SMTP('localhost')
    COMMASPACE = ', '

    msg = MIMEMultipart('alternative')
    msg['Subject'] = "slow query alarm -- DB Proxy Alarmer"
    msg['From'] = email_from
    msg['To'] = COMMASPACE.join(email_to)
    msg.attach(MIMEText(content, 'plain'))

    for mail in email_to:
        smtp.sendmail(email_from, mail, msg.as_string())
    smtp.quit()
    
def sendMailToAdmin(content):
    smtp = smtplib.SMTP('localhost')
    COMMASPACE = ', '

    msg = MIMEMultipart('alternative')
    msg['Subject'] = "DB Proxy Alarmer"
    msg['From'] = email_from
    msg['To'] = COMMASPACE.join(email_admin)
    msg.attach(MIMEText(content, 'plain'))

    for mail in email_to:
        smtp.sendmail(email_from, mail, msg.as_string())
    smtp.quit()

def getPos(posfile, filename):
    if not os.path.isfile(posfile):
        if not os.path.isfile(filename):
            print "open error: " + filename
            return 0

        f = open(filename , "r")
        f.seek(0, 2)
        pos = f.tell()
        f.close()
    else:
        f = open(posfile, 'r')
        pos = f.read()
        f.close()

    return pos

def main():
    for instance in watch_instance:
        posfile = "/tmp/proxy_watcher_" + instance
        parsefile = watch_path + instance + ".log"

        # get last position
        lastpos = getPos(posfile, parsefile)

        # check log
        current_pos = watchLog(parsefile, lastpos)
        #current_pos = watchLog(parsefile, 0) # debug

        # record position
        f = open(posfile, 'w')
        f.write(str(current_pos))
        f.close()

if __name__ == '__main__':
    main()
