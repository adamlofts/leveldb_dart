// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library sample_synchronous_extension;

import 'dart-ext:sample_extension';

class LevelDB extends NativeDB {

  static LevelDB open(String path) {
    LevelDB kls = new LevelDB();
    _init(kls, path);
    return kls;
  }

  String get(String key) {
    return _get(this, key);
  }

  void put(String key, String value) {
    _put(this, key, value);
  }

  String delete(String key) {
    return _delete(this, key);
  }

  LevelIterator get iterator {
    LevelIterator it = new LevelIterator();
    _newIterator(this, it);
    return it;
  }

  static bool _init(LevelDB kls, String path) native 'DBOpen';
  static String _get(LevelDB kls, String key) native 'DBGet';
  static bool _put(LevelDB kls, String key, String value) native 'DBPut';
  static bool _delete(LevelDB kls, String key) native 'DBDelete';
  static void _newIterator(LevelDB kls, LevelIterator it) native "DBNewIterator";
}

class LevelIterator extends NativeIterator {
  void seek() {
    _seek(this);
  }
  bool get valid => _valid(this);
  void next() => _next(this);

  String get key => _key(this);
  String get value => _value(this);

  static void _seek(LevelIterator it) native 'IteratorSeek';
  static bool _valid(LevelIterator it) native "IteratorValid";
  static void _next(LevelIterator it) native "IteratorNext";

  static String _key(LevelIterator it) native "IteratorKey";
  static String _value(LevelIterator it) native "IteratorValue";
}
