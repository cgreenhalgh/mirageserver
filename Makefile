# valid targets for various combinations :

all: socket

socket:
	mir-build unix-socket/server.bin

.PHONY:clean
clean:
	ocamlbuild -clean
