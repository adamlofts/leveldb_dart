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
  Uint8List _encode(var v);
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
 * This encoding expects to be passed a Uint8List
 */
class LevelEncodingNone implements LevelEncoding {
  const LevelEncodingNone();
  Uint8List _encode(Uint8List v) => v;
  Uint8List _decode(Uint8List v) => v;
}

class _Iterator extends NativeIterator {

  static _Iterator _new(int ptr, SendPort port, int limit, bool fillCache, String gt, bool isGtClosed, String lt, bool isLtClosed) {
    _Iterator it = new _Iterator();
    int v = it._init(ptr, port, limit, fillCache, gt, isGtClosed, lt, isLtClosed);
    LevelDBError e = LevelDB._getError(v);
    if (e != null) {
      throw e;
    }
    return it;
  }

  int _init(int ptr, SendPort port, int limit, bool fillCache, String gt, bool isGtClosed, String lt, bool isLtClosed) native "Iterator_New";

  void pause() native "Iterator_Pause";
  void resume() native "Iterator_Resume";
  void cancel() native "Iterator_Cancel";
}

class LevelDB extends NativeDB {

  SendPort _servicePort;
  int _ptr;

  static SendPort _newServicePort() native "DB_ServicePort";
  void _init(int ptr) native "DB_Init";

  static const _ENCODING = const LevelEncodingUtf8();

  /**
   * Internal constructor. Use LevelDB::open().
   */
  LevelDB(SendPort servicePort, int ptr) :
    _servicePort = servicePort,
    _ptr = ptr;

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
    var args = new List(3);
    args[0] = replyPort.sendPort;
    args[1] = 1;
    args[2] = path;

    SendPort servicePort = _newServicePort();
    replyPort.handler = (var result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      LevelDB db = new LevelDB(servicePort, result);
      db._init(result);
      completer.complete(db);
    };
    servicePort.send(args);
    return completer.future;
  }

  Future close() {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(3);
    args[0] = replyPort.sendPort;
    args[1] = 2;
    args[2] = _ptr;

    replyPort.handler = (result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete(result);
    };
    _servicePort.send(args);
    return completer.future;
  }

  get(var key, { LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING }) {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(4);
    args[0] = replyPort.sendPort;
    args[1] = 3;
    args[2] = _ptr;
    args[3] = keyEncoding._encode(key);

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
    _servicePort.send(args);
    return completer.future;
  }


  Future put(key, value, { bool sync: false, LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING }) {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(6);
    args[0] = replyPort.sendPort;
    args[1] = 4;
    args[2] = _ptr;
    args[3] = keyEncoding._encode(key);
    args[4] = valueEncoding._encode(value);
    args[5] = sync;

    replyPort.handler = (result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete();
    };
    _servicePort.send(args);
    return completer.future;
  }

  Future delete(key, { LevelEncoding keyEncoding: _ENCODING }) {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(4);
    args[0] = replyPort.sendPort;
    args[1] = 5;
    args[2] = _ptr;
    args[3] = keyEncoding._encode(key);

    replyPort.handler = (result) {
      replyPort.close();
      if (_completeError(completer, result)) {
        return;
      }
      completer.complete();
    };
    _servicePort.send(args);
    return completer.future;
  }

  /**
   * Iterate through the db returning (key, value) tuples.
   *
   * FIXME: For now the parameters have to be dart strings.
   */
  Stream<List> getItems({ String gt, String gte, String lt, String lte, int limit: -1, bool fillCache: true,
      LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING }) {
    RawReceivePort replyPort = new RawReceivePort();
    _Iterator iterator = _Iterator._new(
        _ptr,
        replyPort.sendPort,
        limit,
        fillCache,
        gt == null ? gte : gt,
        gt == null,
        lt == null ? lte : lt,
        lt == null
    );

    StreamController<List> controller = new StreamController<List>(
      onListen: () => iterator.resume(),
      onPause: () => iterator.pause(),
      onResume: () => iterator.resume(),
      onCancel: () => iterator.cancel()
    );

    replyPort.handler = (result) {
      LevelDBError e = _getError(result);
      if (e != null) {
        replyPort.close();
        controller.addError(e);
        controller.close();
        return;
      }

      if (result == 0) { // Stream finished
        replyPort.close();
        controller.close();
        return;
      }

      // FIXME: Would be good to avoid allocation if no decoding required.
      List ret = new List(2);
      ret[0] = keyEncoding._decode(result[0]);
      ret[1] = valueEncoding._decode(result[1]);
      controller.add(ret);
    };

    return controller.stream;
  }

  /**
   * Some pretty API.
   */
  Stream getKeys({ String gt, String gte, String lt, String lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((List item) => item[0]);
  Stream getValues({ String gt, String gte, String lt, String lte, int limit: -1, bool fillCache: true,
    LevelEncoding keyEncoding: _ENCODING, LevelEncoding valueEncoding: _ENCODING}) =>
      getItems(gt: gt, gte: gte, lt: lt, lte: lte, limit: limit, fillCache: fillCache,
          keyEncoding: keyEncoding, valueEncoding: valueEncoding).map((List item) => item[1]);
}
