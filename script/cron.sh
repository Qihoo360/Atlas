#!/bin/sh

process=`ps aux|grep "mysql-proxy --defaults-file=/etc/mysql-proxy.cnf"|grep -v grep|wc -l`
if [ "$process" != "2" ]; then
	dir="/usr/local/mysql-proxy/log/"
	mkdir -p $dir
	file=$dir"error.log"
	date=`date`
	sh -c "echo $date >> $file"
	sh -c "echo 'error: MySQL-Proxy is NOT running' >> $file"

	if [ "$process" == "1" ]; then
		killall mysql-proxy 2>/dev/null
		rm -f /usr/local/mysql-proxy/bin/pid
		if [ "$?" != "0" ]; then
			sh -c "echo 'error: failed to stop MySQL-Proxy' >> $file"
			exit 2
		fi
		sleep 2
	fi

	/usr/local/mysql-proxy/bin/mysql-proxy --defaults-file=/etc/mysql-proxy.cnf 2> /dev/null
	if [ "$?" != "0" ]; then
		sh -c "echo 'error: failed to start MySQL-Proxy' >> $file"
		exit 1
	fi

	sh -c "echo 'OK: MySQL-Proxy is started' >> $file"
	exit 0
fi
