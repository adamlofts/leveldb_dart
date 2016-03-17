

all: lib/libleveldb.so

LIBS=-lleveldb
ARGS=-Wall

lib/leveldb.o: lib/leveldb.cc
	g++ -O2 -fPIC $(ARGS) -I$(DART_SDK) -DDART_SHARED_LIB -c lib/leveldb.cc -o lib/leveldb.o -std=c++11

lib/libleveldb.so: lib/leveldb.o
	gcc -O2 $(ARGS) -shared -Wl,-soname,libleveldb.so -o lib/libleveldb.so lib/leveldb.o $(LIBS)

clean:
	rm -f lib/*.o lib/*.so
