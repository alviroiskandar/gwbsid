
CC ?= cc
CFLAGS = -Wall -Wextra -O2 -ggdb3 -I. -Wno-unused-parameter
LDLIBS = -lpthread -lmysqlclient -ljson-c

WEB_OBJECTS := \
	web.o \
	web_src/route.o \
	web_src/controllers/index.o \

all: parser web

parser: parser.c

web: $(WEB_OBJECTS)

clean:
	rm -f parser web $(WEB_OBJECTS)

.PHONY: all clean
