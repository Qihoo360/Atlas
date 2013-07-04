encrypt:encrypt.o
	gcc -o encrypt encrypt.o -lcrypto

encrypt.o:encrypt.c
	gcc -c encrypt.c

clean:
	rm -f encrypt encrypt.o
