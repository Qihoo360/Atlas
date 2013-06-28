#!/bin/sh

function input()
{
	read -p "$1" param
	if [ "$param" == "" ]; then
		echo $2
	else
		echo $param
	fi
}

create="yes"

while [ $create == "yes" ]
do
	#1. 读server名
	server=`input "please input server: " $server`

	#2. 读DB名
	db=`input "please input DB: " $db`

	#3. 读用户名
	username=`input "please input username: " $username`

	#4. 读密码
	password=`input "please input password: " $password`

	#5. 读表名
	table=`input "please input table name: " ""`

	#6. 读子表张数
	num=`input "please input num of sub-tables: " ""`

	#7. 读建表语句
	sql=`input "please input SQL of create table(no return): " ""`

	#8. 建第一张子表table_0
	echo -e $sql
	sh -c "mysql -h$server -u$username -p$password $db -e'$sql'"

	#9. 建其他子表
	for (( i=1; i<$num; i=i+1 ))
	do
	    sql="CREATE TABLE ${table}_${i} LIKE ${table}_0"
	    echo -e $sql
	    sh -c "mysql -h$server -u$username -p$password $db -e'$sql'"
	done

	#10. 是否继续建
	read -p "continue to create table?(type yes or no)" create
done
