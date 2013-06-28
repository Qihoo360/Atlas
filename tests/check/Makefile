check : check.o
	gcc -ocheck check.o -L/usr/local/mysql/lib/mysql -ldl -lm -lmysqlclient_r -lpthread -g

check.o : check.c
	gcc -c check.c -D_GNU_SOURCE -std=c99 -Wall -g

clean:
	rm -f check check.o
