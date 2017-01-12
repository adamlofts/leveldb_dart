
# Users of this package should not need to run this makefile.
#

# First build a static leveldb lib with -fPIC by adding it to the OPT variable in the makefile.
# Then set the source:

LEVELDB_SOURCE=/home/adam/dev/fp3/dart/leveldb-1.19

all: lib/libleveldb.so

LIBS=$(LEVELDB_SOURCE)/out-static/libleveldb.a
ARGS=-Wall

lib/leveldb.o: lib/leveldb.cc
	g++ -O2 $(ARGS) -fPIC -I$(DART_SDK) -I$(LEVELDB_SOURCE)/include -DDART_SHARED_LIB -c lib/leveldb.cc -o lib/leveldb.o -std=c++11

lib/libleveldb.so: lib/leveldb.o
	gcc -O2 $(ARGS) lib/leveldb.o -shared -Wl,-soname,libleveldb.so -o lib/libleveldb.so $(LIBS)

clean:
	rm -f lib/*.o lib/*.so
