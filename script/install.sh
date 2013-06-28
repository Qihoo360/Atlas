#!/bin/sh

if [ $# -lt 1 ] || [ "-h" = "$1" ] || [ "--help" = "$1" ]
then
	echo "用法: $0 HOST";
	echo "HOST : 填写要上线的机器"
	exit 0;
fi

instance=ilike
svn="https://ipt.src.corp.qihoo.net/svn/atlas/trunk"
ssh_user=sync360
SSH="sudo -u $ssh_user ssh -c blowfish"
SCP="sudo -u $ssh_user scp -c blowfish"
DIR="/home/q/system/mysql-proxy"
BIN="$DIR/bin"
CNF="$DIR/conf"
LIB="$DIR/lib"
LOG="$DIR/log"
LUA="$LIB/mysql-proxy/lua"
PRY="$LUA/proxy"
CON="$PRY/conf"
PLG="$LIB/mysql-proxy/plugins"

#rm -rf trunk
#echo "SVN EXPORT开始..."
#svn export $svn > /dev/null
#echo "SVN EXPORT结束"
#cd trunk
#sh bootstrap.sh
#make

hosts=$*
for host in ${hosts}
do
	CNF_FILE="$instance.cnf"
	if test ! -s $CNF_FILE; then
		echo "错误： 未找到$CNF_FILE"
		exit 1
	fi 

	CON_FILE="config_$instance.lua"
	if test ! -s $CON_FILE; then
		echo "错误： 未找到$CON_FILE"
		exit 1
	fi 

	echo "=== 正在解压... ==="
	tar zxf proxy.tar.gz
	echo -e "=== 解压完毕 ===\n"

	echo -e "=== 正在远程机器上创建目录... ==="
	$SSH $host "mkdir -p $BIN" >/dev/null
	$SSH $host "mkdir -p $CNF" >/dev/null
	$SSH $host "mkdir -p $CON" >/dev/null
	$SSH $host "mkdir -p $PLG" >/dev/null
	$SSH $host "mkdir -p $LOG" >/dev/null
	echo -e "=== 创建目录完毕 ===\n"

	echo "=== 正在复制文件... ==="
	### conf目录 ###
	$SCP $CNF_FILE $host:$CNF
	$SSH $host "chmod 600 $CNF/$CNF_FILE"

	### config目录 ###
	$SCP $CON_FILE $host:$CON

	cd trunk

	### bin目录 ###
	$SCP mysql-proxyd $host:$BIN

	### lib目录 ###
	$SCP *.so.* $host:$LIB
	$SCP liblua.so $host:$LIB
	rm -f liblua.so

	### plugins目录 ###
	$SCP lib*.so $host:$PLG
	rm -f lib*.so

	### lua目录 ###
	$SCP *.so $host:$LUA
	$SCP admin.lua $host:$LUA
	$SCP rw-splitting.lua $host:$LUA
	rm -f admin.lua rw-splitting.lua

	### proxy目录 ###
	$SCP *.lua $host:$PRY

	if $SSH $host "test -s $LOG/$instance.pid"; then
		$SSH $host $BIN/mysql-proxyd $instance stop >/dev/null 2>&1
	fi

	sleep 3s
	$SCP mysql-proxy $host:$BIN
	echo -e "=== 复制文件完毕 ===\n"

	$SSH $host $BIN/mysql-proxyd $instance start >/dev/null 2>&1 &
	echo -e "=== $host 上线成功 ===\n"

	sleep 1s
	PID=`ps aux|grep "ssh -c blowfish $host $BIN/mysql-proxyd $instance start"|grep -v grep|awk '{print $2}'`
	sudo kill $PID 2>/dev/null

	cd ..
done

rm -rf trunk
echo -e "\n=== 所有机器上线完毕 ===\n"
exit 0
