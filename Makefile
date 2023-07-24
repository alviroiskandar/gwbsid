
CC ?= cc
CFLAGS = -Wall -Wextra -O2 -ggdb3
LDLIBS = -lpthread -lmysqlclient

all: parser

parser: parser.c

clean:
	rm -f parser

.PHONY: all clean