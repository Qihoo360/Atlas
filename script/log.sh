#!/bin/sh

date=`date +%Y%m%d --date="-1 day"`
dir="/usr/local/mysql-proxy/log"
filelist=`ls $dir/*.log`

for filename in $filelist
do
	logfile=`echo $filename | cut -d '.' -f1`
	gzip -c "$filename" > "${logfile}_log.$date.gz"
	sh -c "cat /dev/null > $filename"
done
