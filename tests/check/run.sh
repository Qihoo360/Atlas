#!/bin/sh

LD_LIBRARY_PATH=/usr/local/mysql/lib/mysql

#./check -t verify -c 1 -h 127.0.0.1 -P 4040 -u qtbuser -p qihoo.net -f sql_list
#./check -t short -c 10 -h 127.0.0.1 -P 4050 -u zc -p zc
./check -t long -c 10 -h 127.0.0.1 -P 4040 -u qtbuser -p qihoo.net -n 100000
