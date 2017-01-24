*Fast & simple storage - a Dart LevelDB wrapper*

<img alt="LevelDB Logo" height="100" src="http://leveldb.org/img/logo.svg">

Introduction
------------

**[LevelDB](https://github.com/google/leveldb)** is a simple key/value data store built by Google, inspired by BigTable. It's used in Google
Chrome and many other products. LevelDB supports arbitrary byte arrays as both keys and values, singular *get*, *put* and *delete*
operations, *batched put and delete*, bi-directional iterators and simple compression using the very fast
[Snappy](http://google.github.io/snappy/) algorithm.

**leveldb_dart** aims to expose the features of LevelDB in a **Dart-friendly way**.

LevelDB stores entries **sorted lexicographically by keys**. This makes leveldb's `getItems` interface a very powerful query mechanism.

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
  LevelDB db = await LevelDB.open("/tmp/testdb1");
  db.put("abc", "def");
  String value = db.get("abc");
  print("value is $value"); // value2 is def
}
```
Check out [example/main.dart](example/main.dart) to see how to read, write and iterate over keys and values.

Feature Support
---------------
- [x] Read and write keys
- [x] Forward iteration
- [ ] Backward iteration
- [ ] Snapshots
- [ ] Bulk get / put




