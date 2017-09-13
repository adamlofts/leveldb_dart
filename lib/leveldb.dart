// Copyright (c) 2016 Adam Lofts

library leveldb;

import 'dart:async' show Future, Completer;
import 'dart:isolate' show RawReceivePort, SendPort;
import 'dart:typed_data' show Uint8List;
import 'dart:convert' show UTF8, AsciiCodec;
import 'dart:nativewrappers' show NativeFieldWrapperClass2;
import 'dart:collection' show IterableBase;

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

/// Interface for specifying an encoding. The encoding must encode the object to a Uint8List and decode
/// from a Uint8List.
abstract class LevelEncoding<T> {
  /// Encode to a Uint8List
  Uint8List encode(T v);
  /// Decode from a Uint8List
  T decode(Uint8List v);

  /// The none encoding does no encoding. You must pass in a Uint8List to all fucntions.
  /// Because it does no transformation it reduces the number of allocations.
  /// Use this encoding for performance.
  static LevelEncoding<Uint8List> get none => const _LevelEncodingNone();

  /// Default encoding. Expects to be passed a String and will encode/decode to UTF8 in the db.
  static LevelEncoding<String> get utf8 => const _LevelEncodingUtf8();

  /// Ascii encoding. Potentially faster than UTF8 for ascii-only text (untested).
  static LevelEncoding<String> get ascii => const _LevelEncodingAscii();
}

class _LevelEncodingUtf8 implements LevelEncoding<String> {
  /// Default UTF8 encoding.
  const _LevelEncodingUtf8();
  @override
  Uint8List encode(String v) => new Uint8List.fromList(UTF8.encode(v));
  @override
  String decode(Uint8List v) => UTF8.decode(v);
}

class _LevelEncodingAscii implements LevelEncoding<String> {
  // Ascii encoding
  const _LevelEncodingAscii();
  @override
  Uint8List encode(String v) => new Uint8List.fromList(const AsciiCodec().encode(v));
  @override
  String decode(Uint8List v) => const AsciiCodec().decode(v);
}

class _LevelEncodingNone implements LevelEncoding<Uint8List> {
  const _LevelEncodingNone();
  @override
  Uint8List encode(Uint8List v) => v;
  @override
  Uint8List decode(Uint8List v) => v;
}

/// A key-value database
class LevelDB<K, V> extends NativeFieldWrapperClass2 {

  final LevelEncoding<K> _keyEncoding;
  final LevelEncoding<V> _valueEncoding;

  LevelDB._internal(this._keyEncoding, this._valueEncoding);

  void _open(bool shared, SendPort port, String path, int blockSize, bool createIfMissing, bool errorIfExists) native "DB_Open";

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

  /// Open a database at [path] using [String] keys and values which will be encoded to utf8
  /// in the database.
  ///
  /// See [open] for information on optional parameters.
  static Future<LevelDB<String, String>> openUtf8(String path,
      {bool shared: false, int blockSize: 4096, bool createIfMissing: true, bool errorIfExists: false}) =>
    open<String, String>(
        path,
        shared: shared,
        blockSize: blockSize,
        createIfMissing: createIfMissing,
        errorIfExists: errorIfExists,
        keyEncoding: LevelEncoding.utf8,
        valueEncoding: LevelEncoding.utf8,
    );

  /// Open a database at [path] using raw [Uint8List] keys and values.
  ///
  /// See [open] for information on optional parameters.
  static Future<LevelDB<Uint8List, Uint8List>> openUint8List(String path,
      {bool shared: false, int blockSize: 4096, bool createIfMissing: true, bool errorIfExists: false}) =>
    open<Uint8List, Uint8List>(path,
        keyEncoding: LevelEncoding.none, valueEncoding: LevelEncoding.none,
        shared: shared, blockSize: blockSize, createIfMissing: createIfMissing, errorIfExists: errorIfExists);

  /// Open a database at [path]
  ///
  /// If [shared] is true the database will be shared to other isolates in the dart vm. The [LevelDB] returned
  /// in another isolate calling [open] with the same [path] will share the underlying database and data changes
  /// will be visible to both.
  ///
  /// [keyEncoding] or [valueEncoding] must be specified. The given encoding will
  /// be used to encoding and decode keys or values respectively. The encodings must match the generic
  /// type of the database.
  static Future<LevelDB<K, V>> open<K, V>(String path,
      {bool shared: false, int blockSize: 4096, bool createIfMissing: true, bool errorIfExists: false,
      LevelEncoding<K> keyEncoding, LevelEncoding<V> valueEncoding}) {
    assert(keyEncoding != null);
    assert(valueEncoding != null);
    Completer<LevelDB<K, V>> completer = new Completer<LevelDB<K, V>>();
    RawReceivePort replyPort = new RawReceivePort();
    LevelDB<K, V> db = new LevelDB<K, V>._internal(keyEncoding, valueEncoding);
    replyPort.handler = (dynamic result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete(db);
    };
    db._open(shared, replyPort.sendPort, path, blockSize, createIfMissing, errorIfExists);
    return completer.future;
  }

  /// Close this database.
  /// Any pending iteration will throw after this call.
  void close() {
    _syncClose();
  }

  /// Get a key in the database. Returns null if the key is not found.
  V get(K key) {
    Uint8List keyEnc = _keyEncoding.encode(key);
    Uint8List value = _syncGet(keyEnc);
    V ret;
    if (value != null) {
      ret = _valueEncoding.decode(value);
    }
    return ret;
  }

  /// Set a key to a value.
  void put(K key, V value, { bool sync: false }) {
    Uint8List keyEnc = _keyEncoding.encode(key);
    Uint8List valueEnc = _valueEncoding.encode(value);
    _syncPut(keyEnc, valueEnc, sync);
  }

  /// Remove a key from the database
  void delete(K key) {
    Uint8List keyEnc = _keyEncoding.encode(key);
    _syncDelete(keyEnc);
  }

  /// Return an iterable which will iterate through the db in key order returning key-value items. This iterable
  /// is synchronous so will block when moving.
  LevelIterable<K, V> getItems({ K gt, K gte, K lt, K lte, int limit: -1, bool fillCache: true }) {
    return new LevelIterable<K, V>._internal(this,
        limit,
        fillCache,
        gt == null ? gte : gt,
        gt == null,
        lt == null ? lte : lt,
        lt == null
    );
  }
}

/// A key-value pair returned by the iterator
class LevelItem<K, V> {
  /// The key. Type is determined by the keyEncoding specified
  final K key;
  /// The value. Type is determined by the valueEncoding specified
  final V value;
  LevelItem._internal(this.key, this.value);
}

/// An iterator
class LevelIterator<K, V> extends NativeFieldWrapperClass2 implements Iterator<LevelItem<K, V>> {
  final LevelEncoding<K> _keyEncoding;
  final LevelEncoding<V> _valueEncoding;

  LevelIterator._internal(LevelIterable<K, V> it) :
      _keyEncoding = it._db._keyEncoding,
      _valueEncoding = it._db._valueEncoding;

  int _init(LevelDB<K, V> db, int limit, bool fillCache, Uint8List gt, bool isGtClosed, Uint8List lt, bool isLtClosed) native "SyncIterator_New";
  Uint8List _next() native "SyncIterator_Next";
  Uint8List _current;

  /// The key of the current LevelItem
  K get currentKey =>
      _current == null ? null : _keyEncoding.decode(new Uint8List.view(_current.buffer, 4, (_current[1] << 8) + _current[0]));

  /// The value of the current LevelItem
  V get currentValue =>
      _current == null ? null : _valueEncoding.decode(new Uint8List.view(_current.buffer, 4 + (_current[3] << 8) + _current[2]));

  @override
  LevelItem<K, V> get current {
    return _current == null ? null : new LevelItem<K, V>._internal(currentKey, currentValue);
  }

  @override
  bool moveNext() {
    _current = _next();
    return _current != null;
  }
}

/// An iterable for the db which creates LevelIterator objects.
class LevelIterable<K, V> extends IterableBase<LevelItem<K, V>> {
  final LevelDB<K, V> _db;

  final int _limit;
  final bool _fillCache;

  final K _gt;
  final bool _isGtClosed;

  final K _lt;
  final bool _isLtClosed;

  LevelIterable._internal(LevelDB<K, V> db, int limit, bool fillCache, K gt, bool isGtClosed, K lt, bool isLtClosed) :
      _db = db,
      _limit = limit,
      _fillCache = fillCache,
      _gt = gt,
      _isGtClosed = isGtClosed,
      _lt = lt,
      _isLtClosed = isLtClosed;

  @override
  LevelIterator<K, V> get iterator {
    LevelIterator<K, V> ret = new LevelIterator<K, V>._internal(this);
    Uint8List ltEncoded;
    if (_lt != null) {
      ltEncoded = _db._keyEncoding.encode(_lt);
    }
    Uint8List gtEncoded;
    if (_gt != null) {
      gtEncoded = _db._keyEncoding.encode(_gt);
    }

    ret._init(_db, _limit, _fillCache, gtEncoded, _isGtClosed, ltEncoded, _isLtClosed);
    return ret;
  }

  /// Returns an iterable of the keys in the db
  Iterable<K> get keys sync* {
    LevelIterator<K, V> it = iterator;
    while (it.moveNext()) {
      yield it.currentKey;
    }
  }

  /// Returns an iterable of the values in the db
  Iterable<V> get values sync* {
    LevelIterator<K, V> it = iterator;
    while (it.moveNext()) {
      yield it.currentValue;
    }
  }
}