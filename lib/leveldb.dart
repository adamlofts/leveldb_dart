// Copyright (c) 2016 Adam Lofts

library leveldb;

import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';
import 'dart:convert';
import 'dart:nativewrappers';
import 'dart:collection';

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

/// A key-value database
class LevelDB extends NativeFieldWrapperClass2 {

  LevelDB._internal();

  void _open(SendPort port, String path, int blockSize, bool createIfMissing, bool errorIfExists) native "DB_Open";

  Uint8List _syncGet(Uint8List key) native "SyncGet";
  void _syncPut(Uint8List key, Uint8List value, bool sync) native "SyncPut";
  void _syncDelete(Uint8List key) native "SyncDelete";
  void _syncClose() native "SyncClose";

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

  /// Close this database.
  /// Any pending iteration will throw after this call.
  void close() {
    _syncClose();
  }

  /// Get a key in the database. Returns null if the key is not found.
  dynamic get(dynamic key, { LevelEncoding keyEncoding, LevelEncoding valueEncoding }) {
    Uint8List keyEnc = LevelEncoding._encodeValue(key, keyEncoding);
    Uint8List value = _syncGet(keyEnc);
    Object ret;
    if (value != null) {
      ret = LevelEncoding._decodeValue(value, valueEncoding);
    }
    return ret;
  }

  /// Set a key to a value.
  void put(dynamic key, dynamic value, { bool sync: false, LevelEncoding keyEncoding, LevelEncoding valueEncoding }) {
    Uint8List keyEnc = LevelEncoding._encodeValue(key, keyEncoding);
    Uint8List valueEnc = LevelEncoding._encodeValue(value, valueEncoding);
    _syncPut(keyEnc, valueEnc, sync);
  }

  /// Remove a key from the database
  void delete(dynamic key, { LevelEncoding keyEncoding }) {
    Uint8List keyEnc = LevelEncoding._encodeValue(key, keyEncoding);
    _syncDelete(keyEnc);
  }

  /// Iterate through the db returning keys
  Iterable<dynamic> getKeys({ dynamic gt, dynamic gte, dynamic lt, dynamic lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding, LevelEncoding valueEncoding}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((LevelItem item) => item.key);

  /// Iterate through the db returning values
  Iterable<dynamic> getValues({ dynamic gt, dynamic gte, dynamic lt, dynamic lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding, LevelEncoding valueEncoding}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((LevelItem item) => item.value);

  /// Return an iterable which will iterate through the db in key order returning key-value items. This iterable
  /// is synchronous so will block when moving.
  Iterable<LevelItem> getItems({ dynamic gt, dynamic gte, dynamic lt, dynamic lte, int limit: -1, bool fillCache: true,
      LevelEncoding keyEncoding, LevelEncoding valueEncoding }) {
    return new _SyncIterable._internal(this,
        limit,
        fillCache,
        gt == null ? gte : gt,
        gt == null,
        lt == null ? lte : lt,
        lt == null,
        keyEncoding,
        valueEncoding);
  }
}

/// A key-value pair returned by the iterator
class LevelItem {
  /// The key. Type is determined by the keyEncoding specified
  final dynamic key;
  /// The value. Type is determinied by the valueEncoding specified
  final dynamic value;
  LevelItem._internal(this.key, this.value);
}

/// An iterator
class LevelIterator extends NativeFieldWrapperClass2 implements Iterator<LevelItem> {
  final _SyncIterable _iterable;

  LevelIterator._internal(_SyncIterable it) :
      _iterable = it;

  int _init(LevelDB db, int limit, bool fillCache, Uint8List gt, bool isGtClosed, Uint8List lt, bool isLtClosed) native "SyncIterator_New";
  List<dynamic> _next() native "SyncIterator_Next";
  Uint8List _current;

  /// The key of the current LevelItem
  dynamic get currentKey => LevelEncoding._decodeValue(new Uint8List.view(_current.buffer, 4, (_current[1] << 8) + _current[0]), _iterable._keyEncoding);
  /// The value of the current LevelItem
  dynamic get currentValue => LevelEncoding._decodeValue(new Uint8List.view(_current.buffer, 4 + (_current[3] << 8) + _current[2]), _iterable._valueEncoding);

  @override
  LevelItem get current {
    return _current == null ? null : new LevelItem._internal(currentKey, currentValue);
  }

  @override
  bool moveNext() {
    _current = _next();
    return _current != null;
  }
}

class _SyncIterable extends IterableBase<LevelItem> {
  final LevelDB _db;

  final int _limit;
  final bool _fillCache;

  final Object _gt;
  final bool _isGtClosed;

  final Object _lt;
  final bool _isLtClosed;

  final LevelEncoding _keyEncoding;
  final LevelEncoding _valueEncoding;

  _SyncIterable._internal(LevelDB db, int limit, bool fillCache, Object gt, bool isGtClosed, Object lt, bool isLtClosed, LevelEncoding keyEncoding, LevelEncoding valueEncoding) :
      _db = db,
      _limit = limit,
      _fillCache = fillCache,
      _gt = gt,
      _isGtClosed = isGtClosed,
      _lt = lt,
      _isLtClosed = isLtClosed,
      _keyEncoding = keyEncoding,
      _valueEncoding = valueEncoding;

  @override
  Iterator<LevelItem> get iterator {
    LevelIterator ret = new LevelIterator._internal(this);
    Uint8List ltEncoded;
    if (_lt != null) {
      ltEncoded = LevelEncoding._encodeValue(_lt, _keyEncoding);
    }
    Uint8List gtEncoded;
    if (_gt != null) {
      gtEncoded = LevelEncoding._encodeValue(_gt, _keyEncoding);
    }

    ret._init(_db, _limit, _fillCache, gtEncoded, _isGtClosed, ltEncoded, _isLtClosed);
    return ret;
  }
}