// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library leveldb;

import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';
import 'dart:convert';

import 'dart-ext:leveldb';

abstract class LevelDBError implements Exception {
  final String msg;
  const LevelDBError(this.msg);
  String toString() => 'LevelDBError: $msg';
}

class LevelDBClosedError extends LevelDBError {
  const LevelDBClosedError() : super("DB already closed");
}

class LevelDBIOError extends LevelDBError {
  const LevelDBIOError() : super("IOError");
}

abstract class LevelEncoding {
  Uint8List _encode(v);
  _decode(Uint8List v);
}

/**
 * This encoding expects a string.
 */
class LevelEncodingUtf8 implements LevelEncoding {
  const LevelEncodingUtf8();
  Uint8List _encode(String v) => new Uint8List.fromList(UTF8.encode(v));
  String _decode(Uint8List v) => UTF8.decode(v);
}

class LevelEncodingAscii implements LevelEncoding {
  const LevelEncodingAscii();
  Uint8List _encode(String v) => new Uint8List.fromList(const AsciiCodec().encode(v));
  String _decode(Uint8List v) => const AsciiCodec().decode(v);
}

/**
 * This encoding does no transformation. You must pass a Uint8List.
 */
class LevelEncodingNone implements LevelEncoding {
  const LevelEncodingNone();
  Uint8List _encode(Uint8List v) => v;
  Uint8List _decode(Uint8List v) => v;
}

class LevelIterator extends NativeIterator {

  static const int MAX_ROWS = 15000;

  final LevelDB _db;

  final LevelEncoding keyEncoding;
  final LevelEncoding valueEncoding;
  final bool isNoEncoding;

  StreamController<List> _controller;

  bool isStreaming;

  LevelIterator(LevelDB db, LevelEncoding keyEncoding, LevelEncoding valueEncoding) :
       _db = db,
        keyEncoding = keyEncoding,
        valueEncoding = valueEncoding,
        isNoEncoding = keyEncoding == LevelEncodingNone &&  valueEncoding == LevelEncodingNone {

    _controller = new StreamController<List>(
        onListen: () {
          isStreaming = true;
          _getRows(MAX_ROWS);
        },
        onPause: () {
          isStreaming = false;
        },
        onResume: () {
          isStreaming = true;
          _getRows(MAX_ROWS);
        },
        onCancel: () {
          isStreaming = false;
        },
        sync: true
    );
  }

  Stream<List> get stream => _controller.stream;

  static LevelIterator _new(LevelDB db, int limit, bool fillCache, Object gt, bool isGtClosed, Object lt, bool isLtClosed, LevelEncoding keyEncoding, LevelEncoding valueEncoding) {
    LevelIterator it = new LevelIterator(db, keyEncoding, valueEncoding);
    Uint8List ltEncoded;
    if (lt != null) {
      ltEncoded = keyEncoding._encode(lt);
    }
    Uint8List gtEncoded;
    if (gt != null) {
      gtEncoded = keyEncoding._encode(gt);
    }
    int v = it._init(db, limit, fillCache, gtEncoded, isGtClosed, ltEncoded, isLtClosed);
    LevelDBError e = LevelDB._getError(v);
    if (e != null) {
      throw e;
    }
    return it;
  }

  int _init(LevelDB db, int limit, bool fillCache, Uint8List gt, bool isGtClosed, Uint8List lt, bool isLtClosed) native "Iterator_New";

  void _getRows(int maxRows) {
    RawReceivePort port = new RawReceivePort();
    port.handler = (result) => _handler(port, result);
    _db._getRows(port.sendPort, this);
  }

  void _handler(RawReceivePort port, result) {
    LevelDBError e = LevelDB._getError(result);
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
      if (isStreaming) {
        _getRows(MAX_ROWS); // Get some more rows.
      }
      return;
    }

    if (!isNoEncoding) {
      result[0] = keyEncoding._decode(result[0]);
      result[1] = valueEncoding._decode(result[1]);
    }
    _controller.add(result);
  }
}

const _ENCODING = const LevelEncodingUtf8();

class LevelDB extends NativeDB {

  void _open(SendPort port, String path) native "DB_Open";
  void _put(SendPort port, Uint8List key, Uint8List value, bool sync) native "DB_Put";
  void _get(SendPort port, Uint8List key) native "DB_Get";
  void _delete(SendPort port, Uint8List key) native "DB_Delete";
  void _getRows(SendPort port, LevelIterator it) native "DB_GetRows";
  void _close(SendPort port) native "DB_Close";

  /**
   * Internal constructor. Use LevelDB::open().
   */
  LevelDB();

  static LevelDBError _getError(var reply) {
    if (reply == -1) {
      return const LevelDBClosedError();
    }
    if (reply == -2) {
      return const LevelDBIOError();
    }
    return null;
  }

  static bool _completeError(Completer completer, var reply) {
    LevelDBError e = _getError(reply);
    if (e != null) {
      completer.completeError(e);
      return true;
    }
    return false;
  }

  static Future<LevelDB> open(String path) {
    var completer = new Completer();
    var replyPort = new RawReceivePort();

    LevelDB db = new LevelDB();
    replyPort.handler = (var result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete(db);
    };
    db._open(replyPort.sendPort, path);
    return completer.future;
  }

  Future close() {
    Completer completer = new Completer();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete(result);
    };
    _close(replyPort.sendPort);
    return completer.future;
  }

  get(key, { LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING }) {
    Completer completer = new Completer();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      if (result == 0) { // key not found
        completer.complete(null);
      } else if (result != null) {
        completer.complete(valueEncoding._decode(result));
      }
    };
    _get(replyPort.sendPort, keyEncoding._encode(key));
    return completer.future;
  }

  Future put(key, value, { bool sync: false, LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING }) {
    Completer completer = new Completer();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete();
    };
    _put(replyPort.sendPort, keyEncoding._encode(key), valueEncoding._encode(value), sync);
    return completer.future;
  }

  Future delete(key, { LevelEncoding keyEncoding: _ENCODING }) {
    var completer = new Completer();
    RawReceivePort replyPort = new RawReceivePort();
    replyPort.handler = (result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete();
    };
    _delete(replyPort.sendPort, keyEncoding._encode(key));
    return completer.future;
  }

  /**
   * Iterate through the db returning (key, value) tuples.
   */
  Stream<List> getItems({ gt, gte, lt, lte, int limit: -1, bool fillCache: true,
      LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING }) {

    LevelIterator iterator = LevelIterator._new(
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

  /**
   * Some pretty API.
   */
  Stream getKeys({ gt, gte, lt, lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((List item) => item[0]);
  Stream getValues({ gt, gte, lt, lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((List item) => item[1]);
}
