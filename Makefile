
CC ?= cc
CFLAGS = -Wall -Wextra -O2 -ggdb3 -fsanitize=address
LDLIBS = -lpthread -lmysqlclient -ljson-c

all: parser web

parser: parser.c

web: web.c

clean:
	rm -f parser web

.PHONY: all clean
