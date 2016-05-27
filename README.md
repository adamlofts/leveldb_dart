leveldb_dart
=======

<img alt="LevelDB Logo" height="100" src="http://leveldb.org/img/logo.svg">

**Fast & simple storage - a Dart LevelDB wrapper**

  * <a href="#intro">Introduction</a>
  * <a href="#basic">Basic usage</a>

<a name="intro"></a>
Introduction
------------

**[LevelDB](https://github.com/google/leveldb)** is a simple key/value data store built by Google, inspired by BigTable. It's used in Google Chrome and many other products. LevelDB supports arbitrary byte arrays as both keys and values, singular *get*, *put* and *delete* operations, *batched put and delete*, bi-directional iterators and simple compression using the very fast [Snappy](http://google.github.io/snappy/) algorithm.

**leveldb_dart** aims to expose the features of LevelDB in a **Dart-friendly way**.

LevelDB stores entries **sorted lexicographically by keys**. This makes leveldb_darts's <code>getItems</code> interface a very powerful query mechanism.

<a name="basic"></a>
Basic usage (Ubuntu / Debian)
-----------

leveldb_dart is a Dart native extension so adding it as a pub dependency is a two step process.

First you need to install and build leveldb_dart. 

```sh
$ apt-get install libleveldb-dev
$ git clone https://github.com/adamlofts/leveldb_dart.git
$ cd leveldb_dart
$ DART_SDK=/path/to/dart-sdk make
```

Now add leveldb_dart as a path dependency in your pubspec.yaml file.
```yaml
name: my_project
...
dependencies:
  leveldb:
    path: /my/path/to/leveldb_dart
```

Check out [bin/main.dart](bin/main.dart) for example usage.
