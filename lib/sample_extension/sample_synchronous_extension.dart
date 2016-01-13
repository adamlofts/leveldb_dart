// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library sample_synchronous_extension;

import 'dart-ext:sample_extension';

// The simplest way to call native code: top-level functions.
int systemRand() native "SystemRand";
int noScopeSystemRand() native "NoScopeSystemRand";
bool systemSrand(int seed) native "SystemSrand";

class Class extends NativeFieldsWrapper {

  static Class open() {
    Class kls = new Class();
    _init(kls);
    return kls;
  }

  void put() {
    _put(this);
  }

  String get() {
    print(_get(this));
    return _get(this);
  }

  LevelIterator get iterator {
    LevelIterator it = new LevelIterator();
    _newIterator(this, it);
    return it;
  }

  static bool _init(Class kls) native 'LevelDBOpen';
  static String _get(Class kls) native 'LevelDBGet';
  static bool _put(Class kls) native 'LevelDBPut';
  static void _newIterator(Class kls, LevelIterator it) native "DBNewIterator";
}

class LevelIterator extends NativeIterator {
  void seek() {
    _seek(this);
  }
  bool valid() => _valid(this);
  void next() => _next(this);

  String get key => _key(this);
  String get value => _value(this);

  static void _seek(LevelIterator it) native 'IteratorSeek';
  static bool _valid(LevelIterator it) native "IteratorValid";
  static void _next(LevelIterator it) native "IteratorNext";

  static String _key(LevelIterator it) native "IteratorKey";
  static String _value(LevelIterator it) native "IteratorValue";
}