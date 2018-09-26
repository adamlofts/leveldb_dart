
# Users of this package should not need to run this makefile.
#

# First build a static leveldb lib.
# The build script already does this but you need to add a couple of options to the static build.
# Make the line look like:
# OPT ?= -O2 -DNDEBUG -fPIC -D_GLIBCXX_USE_CXX11_ABI=0

# The -fPIC enables linking the static lib into the object we will build.
# -D_GLIBCXX_USE_CXX11_ABI turns off the new c++11 abi. This means the build will be back compatible with
# older linux versions.

# Then set the source:

LEVELDB_SOURCE=/home/adam/dev/fp3/dart/leveldb-1.20
DART_SDK=/home/adam/dev/tools/dart-sdk

LIBS=$(LEVELDB_SOURCE)/out-static/libleveldb.a
# Select prod/debug args
ARGS=-O2 -Wall -D_GLIBCXX_USE_CXX11_ABI=0
# ARGS=-g -O0 -Wall -D_GLIBCXX_USE_CXX11_ABI=0

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
	LIB_NAME = libleveldb.dylib
	ARGS_LINK = -dynamic -undefined dynamic_lookup
endif
ifeq ($(UNAME_S),Linux)
	LIB_NAME = libleveldb.so
	ARGS_LINK = -shared -Wl,-soname,$(LIB_NAME)
endif

all: lib/libleveldb.so

lib/leveldb.o: lib/leveldb.cc
	g++ $(ARGS) -fPIC -I$(DART_SDK) -I$(LEVELDB_SOURCE)/include -DDART_SHARED_LIB -c lib/leveldb.cc -o lib/leveldb.o

lib/libleveldb.so: lib/leveldb.o
	gcc $(ARGS) lib/leveldb.o $(ARGS_LINK) -o lib/$(LIB_NAME) $(LIBS)

clean:
	rm -f lib/*.o lib/*.so lib/*.dylib
