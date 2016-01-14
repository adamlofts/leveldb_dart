// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library leveldb;

import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';

import 'dart-ext:leveldb';

class LevelDB {

  final String _path;

  SendPort _servicePort;
  int _ptr;

  SendPort _newServicePort() native "DB_ServicePort";

  LevelDB(String path) :
    _path = path {
    _servicePort = _newServicePort();
  }

  Future open() {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(3);
    args[0] = replyPort.sendPort;
    args[1] = 1;
    args[2] = _path;

    replyPort.handler = (int result) {
      replyPort.close();
      if (result != null) {
        _ptr = result;
        completer.complete(result);
      } else {
        completer.completeError(new Exception("Random array creation failed"));
      }
    };
    _servicePort.send(args);
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
      if (result != null) {
        completer.complete(result);
      } else {
        completer.completeError(new Exception("Random array creation failed"));
      }
    };
    _servicePort.send(args);
    return completer.future;
  }

  Future<Uint8List> get(Uint8List key) {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(4);
    args[0] = replyPort.sendPort;
    args[1] = 3;
    args[2] = _ptr;
    args[3] = key;

    replyPort.handler = (result) {
      replyPort.close();
      if (result == 0) { // key not found
        completer.complete(null);
      } else if (result != null) {
        completer.complete(result);
      } else {
        completer.completeError(new Exception("Random array creation failed"));
      }
    };
    _servicePort.send(args);
    return completer.future;
  }

  Future put(Uint8List key, Uint8List value) {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(5);
    args[0] = replyPort.sendPort;
    args[1] = 4;
    args[2] = _ptr;
    args[3] = key;
    args[4] = value;

    replyPort.handler = (result) {
      replyPort.close();
      if (result != null) {
        completer.complete(result);
      } else {
        completer.completeError(new Exception("Random array creation failed"));
      }
    };
    _servicePort.send(args);
    return completer.future;
  }

  Future delete(Uint8List key) {
    var completer = new Completer();
    var replyPort = new RawReceivePort();
    var args = new List(4);
    args[0] = replyPort.sendPort;
    args[1] = 5;
    args[2] = _ptr;
    args[3] = key;

    replyPort.handler = (result) {
      replyPort.close();
      if (result != null) {
        completer.complete();
      } else {
        completer.completeError(new Exception("Random array creation failed"));
      }
    };
    _servicePort.send(args);
    return completer.future;
  }

  /**
   * Iterate through the db returning (key, value) tuples.
   */
  Stream<List<Uint8List>> getItems() {
    // FIXME: Pause() implementation
    StreamController<List<Uint8List>> controller = new StreamController<List<Uint8List>>();
    RawReceivePort replyPort = new RawReceivePort();
    List args = new List(3);
    args[0] = replyPort.sendPort;
    args[1] = 6;
    args[2] = _ptr;

    replyPort.handler = (result) {
      if (result == null) {
        replyPort.close();
        controller.addError(new Exception("Strem error"));
        controller.close();
        return;
      }

      if (result == 0) { // Stream finished
        replyPort.close();
        controller.close();
        return;
      }

      controller.add(result);
    };
    _servicePort.send(args);

    return controller.stream;
  }

  /**
   * Some pretty API below. Not stable.
   */
  Stream<Uint8List> getKeys() => getItems().map((List<Uint8List> item) => item[0]);
  Stream<Uint8List> getValues() => getItems().map((List<Uint8List> item) => item[1]);
}
