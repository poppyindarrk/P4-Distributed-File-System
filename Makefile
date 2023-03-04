CC  = gcc
OPTS = -Wall

all: server client lib mkfs

# this generates the target executables
server: server.o udp.o
	$(CC) -o  server -g server.o udp.o 

main: main.o udp.o mfs.o
	$(CC) -o main -g main.o udp.o mfs.o 

client: client.o udp.o mfs.o
	$(CC) -o client -g client.o udp.o mfs.o 

lib:    mfs.o udp.o
	$(CC) -Wall -Werror -shared -fpic -g -o libmfs.so mfs.c udp.c
	#$(CC) -c -fpic mfs.c -Wall -Werror
	#$(CC) -shared -o libmfs.so mfs.o
mkfs:  mkfs.o udp.o mfs.o
	$(CC) -o mkfs -g mkfs.o udp.o mfs.o 

# this is a generic rule for .o files 
%.o: %.c 
	$(CC) $(OPTS) -c $< -o $@

clean:
	rm -f main.o server.o udp.o client.o mfs.o libmfs.so server client *.img