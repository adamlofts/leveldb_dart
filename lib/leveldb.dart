// Copyright (c) 2016 Adam Lofts

library leveldb;

import 'dart:convert' as convert;
import 'dart:async' show Future, Completer;
import 'dart:isolate' show RawReceivePort, SendPort;
import 'dart:typed_data' show Uint8List;
import 'dart:nativewrappers' show NativeFieldWrapperClass2;
import 'dart:collection' show IterableBase;

import 'dart-ext:leveldb';

import 'package:meta/meta.dart' show required;

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
  const LevelInvalidArgumentError._internal()
      : super._internal("Invalid argument");
}

class _Uint8ListEncoder extends convert.Converter<List<int>, Uint8List> {
  const _Uint8ListEncoder();
  @override
  Uint8List convert(List<int> input) => new Uint8List.fromList(input);
}

class _Uint8ListDecoder extends convert.Converter<Uint8List, List<int>> {
  const _Uint8ListDecoder();
  @override
  List<int> convert(Uint8List input) => input;
}

/// This codec will encode a [List<int>] to the [Uint8List] required by LevelDB dart.
class Uint8ListCodec extends convert.Codec<List<int>, Uint8List> {
  /// Default constructor
  const Uint8ListCodec();
  @override
  convert.Converter<List<int>, Uint8List> get encoder => const _Uint8ListEncoder();
  @override
  convert.Converter<Uint8List, List<int>> get decoder => const _Uint8ListDecoder();
}

class _IdentityConverter extends convert.Converter<Uint8List, Uint8List> {
  const _IdentityConverter();
  @override
  Uint8List convert(Uint8List input) => input;
}

class _IdentityCodec extends convert.Codec<Uint8List, Uint8List> {
  const _IdentityCodec();
  @override
  convert.Converter<Uint8List, Uint8List> get encoder => const _IdentityConverter();
  @override
  convert.Converter<Uint8List, Uint8List> get decoder => const _IdentityConverter();
}

/// A key-value database
class LevelDB<K, V> extends NativeFieldWrapperClass2 {
  final convert.Codec<K, Uint8List> _keyEncoding;
  final convert.Codec<V, Uint8List> _valueEncoding;

  LevelDB._internal(this._keyEncoding, this._valueEncoding);

  void _open(bool shared, SendPort port, String path, int blockSize,
      bool createIfMissing, bool errorIfExists) native "DB_Open";

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

  /// Default encoding. Expects to be passed a String and will encode/decode to UTF8 in the db.
  static convert.Codec<String, Uint8List> get utf8 => const convert.Utf8Codec().fuse(const Uint8ListCodec());

  /// Ascii encoding. Potentially faster than UTF8 for ascii-only text (untested).
  static convert.Codec<String, Uint8List> get ascii =>
      const convert.AsciiCodec().fuse(const Uint8ListCodec());

  /// The identity encoding does no encoding. You must pass in a Uint8List to all functions.
  /// Because it does no transformation it reduces the number of allocations.
  /// Use this encoding for performance.
  static convert.Codec<Uint8List, Uint8List> get identity => const _IdentityCodec();

  /// Open a database at [path] using [String] keys and values which will be encoded to utf8
  /// in the database.
  ///
  /// See [open] for information on optional parameters.
  static Future<LevelDB<String, String>> openUtf8(String path,
          {bool shared: false,
          int blockSize: 4096,
          bool createIfMissing: true,
          bool errorIfExists: false}) =>
      open<String, String>(
        path,
        shared: shared,
        blockSize: blockSize,
        createIfMissing: createIfMissing,
        errorIfExists: errorIfExists,
        keyEncoding: utf8,
        valueEncoding: utf8,
      );

  /// Open a database at [path] using raw [Uint8List] keys and values.
  ///
  /// See [open] for information on optional parameters.
  static Future<LevelDB<Uint8List, Uint8List>> openUint8List(String path,
          {bool shared: false,
          int blockSize: 4096,
          bool createIfMissing: true,
          bool errorIfExists: false}) =>
      open<Uint8List, Uint8List>(path,
          keyEncoding: identity,
          valueEncoding: identity,
          shared: shared,
          blockSize: blockSize,
          createIfMissing: createIfMissing,
          errorIfExists: errorIfExists);

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
      {bool shared: false,
      int blockSize: 4096,
      bool createIfMissing: true,
      bool errorIfExists: false,
      @required convert.Codec<K, Uint8List> keyEncoding,
      @required convert.Codec<V, Uint8List> valueEncoding}) {
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
    db._open(shared, replyPort.sendPort, path, blockSize, createIfMissing,
        errorIfExists);
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
  void put(K key, V value, {bool sync: false}) {
    Uint8List keyEnc = _keyEncoding.encode(key);
    Uint8List valueEnc = _valueEncoding.encode(value);
    _syncPut(keyEnc, valueEnc, sync);
  }

  /// Remove a key from the database
  void delete(K key) {
    Uint8List keyEnc = _keyEncoding.encode(key);
    _syncDelete(keyEnc);
  }

  /// Return an [Iterable] which will iterate through the db in key byte-collated order.
  ///
  /// To start iteration from a particular point use [gt] or [gte] and the iterator will start at the first key
  /// `>` or `>=` the passed value respectively. To stop iteration before the end use [lt] or [lte] to end at the
  /// key `<` or `<=` the passed value respectively.
  ///
  /// The [limit] parameter limits the total number of items iterated.
  ///
  /// For example, say a database contains the keys `a`, `b`, `c` and `d`. To iterate over all items from key `b`
  /// and before `d` in the collation order you can write:
  ///
  ///     getItems(gte: 'b', lt: 'd')
  ///
  LevelIterable<K, V> getItems(
      {K gt, K gte, K lt, K lte, int limit: -1, bool fillCache: true}) {
    return new LevelIterable<K, V>._internal(this, limit, fillCache,
        gt == null ? gte : gt, gt == null, lt == null ? lte : lt, lt == null);
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
class LevelIterator<K, V> extends NativeFieldWrapperClass2
    implements Iterator<LevelItem<K, V>> {
  final convert.Codec<K, Uint8List> _keyEncoding;
  final convert.Codec<V, Uint8List> _valueEncoding;

  LevelIterator._internal(LevelIterable<K, V> it)
      : _keyEncoding = it._db._keyEncoding,
        _valueEncoding = it._db._valueEncoding;

  int _init(LevelDB<K, V> db, int limit, bool fillCache, Uint8List gt,
      bool isGtClosed, Uint8List lt, bool isLtClosed) native "SyncIterator_New";
  Uint8List _next() native "SyncIterator_Next";
  Uint8List _current;

  /// The key of the current LevelItem
  K get currentKey => _current == null
      ? null
      : _keyEncoding.decode(new Uint8List.view(
          _current.buffer, 4, (_current[1] << 8) + _current[0]));

  /// The value of the current LevelItem
  V get currentValue => _current == null
      ? null
      : _valueEncoding.decode(new Uint8List.view(
          _current.buffer, 4 + (_current[3] << 8) + _current[2]));

  @override
  LevelItem<K, V> get current {
    return _current == null
        ? null
        : new LevelItem<K, V>._internal(currentKey, currentValue);
  }

  @override
  bool moveNext() {
    _current = _next();
    return _current != null;
  }
}

/// An [Iterable<LevelItem>] for iterating over key-value pairs.
///
/// Iteration is sorted by key in byte collation order.
///
/// You can use the [keys] and [values] getters to get an [Iterable] over just the keys or just the values
/// in the database.
class LevelIterable<K, V> extends IterableBase<LevelItem<K, V>> {
  final LevelDB<K, V> _db;

  final int _limit;
  final bool _fillCache;

  final K _gt;
  final bool _isGtClosed;

  final K _lt;
  final bool _isLtClosed;

  LevelIterable._internal(LevelDB<K, V> db, int limit, bool fillCache, K gt,
      bool isGtClosed, K lt, bool isLtClosed)
      : _db = db,
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

    ret._init(_db, _limit, _fillCache, gtEncoded, _isGtClosed, ltEncoded,
        _isLtClosed);
    return ret;
  }

  /// Returns an [Iterable] of the keys in the db
  Iterable<K> get keys sync* {
    LevelIterator<K, V> it = iterator;
    while (it.moveNext()) {
      yield it.currentKey;
    }
  }

  /// Returns an [Iterable] of the values in the db
  Iterable<V> get values sync* {
    LevelIterator<K, V> it = iterator;
    while (it.moveNext()) {
      yield it.currentValue;
    }
  }
}
