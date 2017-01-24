
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

LEVELDB_SOURCE=/home/adam/dev/fp3/dart/leveldb-1.19
DART_SDK=/home/adam/dev/tools/dart-sdk

all: lib/libleveldb.so

LIBS=$(LEVELDB_SOURCE)/out-static/libleveldb.a
ARGS=-Wall -D_GLIBCXX_USE_CXX11_ABI=0

lib/leveldb.o: lib/leveldb.cc
	g++ -O2 $(ARGS) -fPIC -I$(DART_SDK) -I$(LEVELDB_SOURCE)/include -DDART_SHARED_LIB -c lib/leveldb.cc -o lib/leveldb.o

lib/libleveldb.so: lib/leveldb.o
	gcc -O2 $(ARGS) lib/leveldb.o -shared -Wl,-soname,libleveldb.so -o lib/libleveldb.so $(LIBS)

clean:
	rm -f lib/*.o lib/*.so
