.SUFFIXES: .c .o

CC=gcc

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.c
TARGET_OBJ:=$(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)bpt.c  $(SRCDIR)buffer.c  $(SRCDIR)join.c 
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.c=.o)

CFLAGS+= -g -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(TARGET_OBJ)
	$(CC) $(CFLAGS) -o $(SRCDIR)bpt.o -c $(SRCDIR)bpt.c
	$(CC) $(CFLAGS) -o $(SRCDIR)buffer.o -c $(SRCDIR)buffer.c
	$(CC) $(CFLAGS) -o $(SRCDIR)join.o -c $(SRCDIR)join.c
	make static_library
	$(CC) $(CFLAGS) -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)* *.txt *.db text* result 

library:
	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(SRCDIR)bpt.o $(SRCDIR)buffer.o $(SRCDIR)join.o 
