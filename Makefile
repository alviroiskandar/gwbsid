
CC ?= cc
CFLAGS = -Wall -Wextra -O2 -ggdb3
LDLIBS = -lpthread -lmysqlclient

all: parser web

parser: parser.c

web: web.c

clean:
	rm -f parser web

.PHONY: all clean
