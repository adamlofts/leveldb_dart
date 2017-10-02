*Fast & simple storage - a Dart LevelDB wrapper*

<img alt="LevelDB Logo" height="100" src="http://leveldb.org/img/logo.svg">

<img alt="Build Status" src="https://travis-ci.org/adamlofts/leveldb_dart.svg?branch=master">

Introduction
------------

**[LevelDB](https://github.com/google/leveldb)** is a simple key/value data store built by Google, inspired by BigTable. It's used in Google
Chrome and many other products. LevelDB supports arbitrary byte arrays as both keys and values, singular *get*, *put* and *delete*
operations, *batched put and delete*, bi-directional iterators and simple compression using the very fast
[Snappy](http://google.github.io/snappy/) algorithm.

**leveldb_dart** aims to expose the features of LevelDB in a **Dart-friendly way**.

LevelDB stores entries **sorted lexicographically by keys**. This makes [LevelDB.getItems](https://www.dartdocs.org/documentation/leveldb/latest/leveldb/LevelDB/getItems.html) a very powerful query mechanism.

Platform Support
----------------

Only linux 64-bit platform is supported. The following distributions have been tested:

* Fedora 25
* Ubuntu 14.04
* Ubuntu 15.10

If your platform works and is not listed please let me know so I can add it.

Basic usage
-----------

Add `leveldb` to your `pubspec.yaml` file.

```
name: myproject
dependencies:
  leveldb:
```

Open a database and read/write some keys and values..

```
import 'dart:async';
import 'package:leveldb/leveldb.dart';

Future main() async {
  LevelDB<String, String> db = await LevelDB.openUtf8("/tmp/testdb");
  db.put("abc", "def");
  String value = db.get("abc");
  print("value is $value"); // value2 is def
}
```
Check out [example/main.dart](example/main.dart) to see how to read, write and iterate over keys and values.

Documentation
-------------

API Documentation is available at https://www.dartdocs.org/documentation/leveldb/latest/

Isolates (Threads)
------------------

*leveldb_dart* supports access to a database from multiple isolates by passing
`shared: true` to the
[LevelDB.open](https://www.dartdocs.org/documentation/leveldb/latest/leveldb/LevelDB/open.html) function. The `LevelDB` object
returned by this function will share an underlying reference to the object in other isolates and changes will
be visible between isolates.

See [example/isolate.dart](example/isolate.dart) for an example of using a database from multiple isolates (OS threads).


Feature Support
---------------

- [x] Read and write keys
- [x] Forward iteration
- [x] Multi-isolate
- [ ] Backward iteration
- [ ] Snapshots
- [ ] Bulk get / put


Custom Encoding and Decoding
----------------------------

By default you can use `LevelDB.openUtf8` to open a database with `String` keys and values which are encoded in UTF8. The `dart:codec` library 
can be used to create databases with custom encodings. See [example/json.dart](example/json.dart) 
for an example which stores dart objects to the database via JSON encoding.


