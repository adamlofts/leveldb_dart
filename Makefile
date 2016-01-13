

all: libsample_extension.so

sample_extension.o: sample_extension.cc
	g++ -fPIC  -I$(DART_SDK) -DDART_SHARED_LIB -c sample_extension.cc #-m32

libsample_extension.so: sample_extension.o
	gcc -shared -Wl,-soname,libsample_extension.so -o libsample_extension.so sample_extension.o #-m32 
	cp libsample_extension.so lib/sample_extension/

clean:
	rm -f *.o *.so lib/sample_extension/*.so
