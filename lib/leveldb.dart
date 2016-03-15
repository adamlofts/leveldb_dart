// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library leveldb;

import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';
import 'dart:convert';

import 'dart-ext:leveldb';

/// Base class for all exceptions thrown by leveldb_dart.
abstract class LevelError implements Exception {
  final String _msg;
  const LevelError._internal(this._msg);
  @override
  String toString() => 'LevelError: $_msg';
}

/// Exception thrown if the database is used after it has been closed.
class LevelClosedError extends LevelError {
  const LevelClosedError._internal() : super._internal("DB already closed");
}

/// Exception thrown if a general IO error is encountered.
class LevelIOError extends LevelError {
  const LevelIOError._internal() : super._internal("IOError");
}

/// Exception thrown if the db is corrupted
class LevelCorruptionError extends LevelError {
  const LevelCorruptionError._internal() : super._internal("Corruption error");
}

/// Exception thrown if invalid argument (e.g. if the database does not exist and createIfMissing is false)
class LevelInvalidArgumentError extends LevelError {
  const LevelInvalidArgumentError._internal() : super._internal("Invalid argument");
}

/// Interface for specifying an encoding. The encoding must encode the object ot a Uint8List and decode
/// from a Uint8List.
abstract class LevelEncoding {
  /// Encode to a Uint8List
  Uint8List encode(dynamic v);
  /// Decode from a Uint8List
  dynamic decode(Uint8List v);

  /// The none encoding does not encoding. You must pass in a Uint8List to all fucntions.
  /// Because it does no transformation it reduces the number of allocations.
  /// Use this encoding for performance.
  static LevelEncoding get none => const _LevelEncodingNone();

  /// Default encoding. Expects to be passed a String and will encode/decode to UTF8 in the db.
  static LevelEncoding get utf8 => const _LevelEncodingUtf8();

  /// Ascii encoding. Potentially faster than UTF8 (untested).
  static LevelEncoding get ascii => const _LevelEncodingAscii();

  static dynamic _encodeValue(dynamic v, LevelEncoding encoding) {
    if (encoding == const _LevelEncodingNone()) {
      return v;
    }
    if (encoding == null) { // Default to utf8
      return const _LevelEncodingUtf8().encode(v);
    }
    return encoding.encode(v);
  }

  static dynamic _decodeValue(dynamic v, LevelEncoding encoding) {
    if (encoding == const _LevelEncodingNone()) {
      return v;
    }
    if (encoding == null) { // Default to utf8
      return const _LevelEncodingUtf8().decode(v);
    }
    return encoding.decode(v);
  }
}

class _LevelEncodingUtf8 implements LevelEncoding {
  /// Default UTF8 encoding.
  const _LevelEncodingUtf8();
  @override
  Uint8List encode(String v) => new Uint8List.fromList(UTF8.encode(v));
  @override
  String decode(Uint8List v) => UTF8.decode(v);
}

class _LevelEncodingAscii implements LevelEncoding {
  // Ascii encoding
  const _LevelEncodingAscii();
  @override
  Uint8List encode(String v) => new Uint8List.fromList(const AsciiCodec().encode(v));
  @override
  String decode(Uint8List v) => const AsciiCodec().decode(v);
}

class _LevelEncodingNone implements LevelEncoding {
  const _LevelEncodingNone();
  @override
  Uint8List encode(Uint8List v) => throw new AssertionError();  // Never called
  @override
  Uint8List decode(Uint8List v) => throw new AssertionError();  // Never called
}

class _LevelIterator extends NativeIterator {

  final LevelDB _db;

  final LevelEncoding _keyEncoding;
  final LevelEncoding _valueEncoding;
  final bool _isNoEncoding;

  StreamController<List<dynamic>> _controller;

  bool _isStreaming;

  _LevelIterator._internal(LevelDB db, LevelEncoding keyEncoding, LevelEncoding valueEncoding) :
       _db = db,
        _keyEncoding = keyEncoding,
        _valueEncoding = valueEncoding,
        _isNoEncoding = keyEncoding == const _LevelEncodingNone() && valueEncoding == const _LevelEncodingNone() {

    _controller = new StreamController<List<dynamic>>(
        onListen: () {
          _isStreaming = true;
          _getRows();
        },
        onPause: () {
          _isStreaming = false;
        },
        onResume: () {
          _isStreaming = true;
          _getRows();
        },
        onCancel: () {
          _isStreaming = false;
        },
        sync: true
    );
  }

  Stream<List<dynamic>> get stream => _controller.stream;

  static _LevelIterator _new(LevelDB db, int limit, bool fillCache, Object gt, bool isGtClosed, Object lt, bool isLtClosed, LevelEncoding keyEncoding, LevelEncoding valueEncoding) {
    _LevelIterator it = new _LevelIterator._internal(db, keyEncoding, valueEncoding);
    Uint8List ltEncoded;
    if (lt != null) {
      ltEncoded = LevelEncoding._encodeValue(lt, keyEncoding);
    }
    Uint8List gtEncoded;
    if (gt != null) {
      gtEncoded = LevelEncoding._encodeValue(gt, keyEncoding);
    }
    int v = it._init(db, limit, fillCache, gtEncoded, isGtClosed, ltEncoded, isLtClosed);
    LevelError e = LevelDB._getError(v);
    if (e != null) {
      throw e;
    }
    return it;
  }

  int _init(LevelDB db, int limit, bool fillCache, Uint8List gt, bool isGtClosed, Uint8List lt, bool isLtClosed) native "Iterator_New";

  void _getRows() {
    RawReceivePort port = new RawReceivePort();
    port.handler = (dynamic result) => _handler(port, result);
    _db._getRows(port.sendPort, this);
  }

  void _handler(RawReceivePort port, dynamic result) {
    LevelError e = LevelDB._getError(result);
    if (e != null) {
      port.close();
      _controller.addError(e);
      _controller.close();
      return;
    }

    if (result == 0) { // Stream finished
      port.close();
      _controller.close();
      return;
    }

    if (result == 1) { // maxRows reached
      port.close();
      if (_isStreaming) {
        _getRows(); // Get some more rows.
      }
      return;
    }

    if (!_isNoEncoding) {
      result[0] = LevelEncoding._decodeValue(result[0], _keyEncoding);
      result[1] = LevelEncoding._decodeValue(result[1], _valueEncoding);
    }
    _controller.add(result);
  }
}

/// A key-value database
class LevelDB extends NativeDB {

  LevelDB._internal();

  void _open(SendPort port, String path, int blockSize, bool createIfMissing, bool errorIfExists) native "DB_Open";
  void _put(SendPort port, Uint8List key, Uint8List value, bool sync) native "DB_Put";
  void _get(SendPort port, Uint8List key) native "DB_Get";
  void _delete(SendPort port, Uint8List key) native "DB_Delete";
  void _getRows(SendPort port, _LevelIterator it) native "DB_GetRows";
  void _close(SendPort port) native "DB_Close";

  static LevelError _getError(dynamic reply) {
    if (reply == -1) {
      return const LevelClosedError._internal();
    }
    if (reply == -2) {
      return const LevelIOError._internal();
    }
    if (reply == -3) {
      return const LevelCorruptionError._internal();
    }
    if (reply == -4) {
      return const LevelInvalidArgumentError._internal();
    }
    return null;
  }

  static bool _completeError(Completer<dynamic> completer, dynamic reply) {
    LevelError e = _getError(reply);
    if (e != null) {
      completer.completeError(e);
      return true;
    }
    return false;
  }

  /// Open a new database at `path`
  static Future<LevelDB> open(String path, {int blockSize: 4096, bool createIfMissing: true, bool errorIfExists: false}) {
    Completer<LevelDB> completer = new Completer<LevelDB>();
    RawReceivePort replyPort = new RawReceivePort();

    LevelDB db = new LevelDB._internal();
    replyPort.handler = (dynamic result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete(db);
    };
    db._open(replyPort.sendPort, path, blockSize, createIfMissing, errorIfExists);
    return completer.future;
  }

  /// Close this database. Completes with `true`
  Future<Null> close() {
    Completer<Null> completer = new Completer<Null>();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (dynamic result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete();
    };
    _close(replyPort.sendPort);
    return completer.future;
  }

  /// Get a key in the database. Returns null if the key is not found.
  Future<dynamic> get(dynamic key, { LevelEncoding keyEncoding, LevelEncoding valueEncoding }) {
    Completer<dynamic> completer = new Completer<dynamic>();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (dynamic result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      if (result == 0) { // key not found
        completer.complete(null);
      } else if (result != null) {
        dynamic value = LevelEncoding._decodeValue(result, valueEncoding);
        completer.complete(value);
      }
    };
    key = LevelEncoding._encodeValue(key, keyEncoding);
    _get(replyPort.sendPort, key);
    return completer.future;
  }

  /// Set a key to a value.
  Future<Null> put(dynamic key, dynamic value, { bool sync: false, LevelEncoding keyEncoding, LevelEncoding valueEncoding }) {
    Completer<Null> completer = new Completer<Null>();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (dynamic result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete();
    };
    key = LevelEncoding._encodeValue(key, keyEncoding);
    value = LevelEncoding._encodeValue(value, valueEncoding);
    _put(replyPort.sendPort, key, value, sync);
    return completer.future;
  }

  /// Remove a key from the database
  Future<Null> delete(dynamic key, { LevelEncoding keyEncoding }) {
    Completer<Null> completer = new Completer<Null>();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (dynamic result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete();
    };
    key = LevelEncoding._encodeValue(key, keyEncoding);
    _delete(replyPort.sendPort, key);
    return completer.future;
  }

  /// Iterate through the db returning [key, value] lists.
  Stream<List<dynamic>> getItems({ dynamic gt, dynamic gte, dynamic lt, dynamic lte, int limit: -1, bool fillCache: true,
      LevelEncoding keyEncoding, LevelEncoding valueEncoding }) {
    _LevelIterator iterator = _LevelIterator._new(
        this,
        limit,
        fillCache,
        gt == null ? gte : gt,
        gt == null,
        lt == null ? lte : lt,
        lt == null,
        keyEncoding,
        valueEncoding
    );
    return iterator._controller.stream;
  }

  /// Iterate through the db returning keys
  Stream<dynamic> getKeys({ dynamic gt, dynamic gte, dynamic lt, dynamic lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding, LevelEncoding valueEncoding}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((List<dynamic> item) => item[0]);

  /// Iterate through the db returning values
  Stream<dynamic> getValues({ dynamic gt, dynamic gte, dynamic lt, dynamic lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding, LevelEncoding valueEncoding}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((List<dynamic> item) => item[1]);
}
