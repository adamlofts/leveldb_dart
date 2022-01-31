import 'dart:io';
import 'dart:async';
import 'dart:convert';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:leveldb/leveldb.dart';

Future<LevelDB<String, String>> _openTestDB(
    {int index: 0, bool shared: false, bool clean: true}) async {
  Directory d = new Directory('/tmp/test-level-db-dart-$index');
  if (clean && d.existsSync()) {
    await d.delete(recursive: true);
  }
  return LevelDB.openUtf8('/tmp/test-level-db-dart-$index', shared: shared);
}

Future<LevelDB<K, V>> _openTestDBEnc<K, V>(
    Codec<K, Uint8List> keyEncoding, Codec<V, Uint8List> valueEncoding,
    {int index: 0, bool shared: false, bool clean: true}) async {
  Directory d = new Directory('/tmp/test-level-db-dart-$index');
  if (clean && d.existsSync()) {
    await d.delete(recursive: true);
  }
  return LevelDB.open('/tmp/test-level-db-dart-$index',
      shared: shared, keyEncoding: keyEncoding, valueEncoding: valueEncoding);
}

const Matcher _isClosedError = const _ClosedMatcher();

class _ClosedMatcher extends TypeMatcher<LevelClosedError> {
  const _ClosedMatcher();
}

const Matcher _isIteratorError = const _IteratorMatcher();

class _IteratorMatcher extends TypeMatcher<LevelInvalidIterator> {
  const _IteratorMatcher();
}

const Matcher _isInvalidArgumentError = const _InvalidArgumentMatcher();

class _InvalidArgumentMatcher extends TypeMatcher<LevelInvalidArgumentError> {
  const _InvalidArgumentMatcher();
}

/// tests
void main() {
  test('LevelDB basics', () async {
    LevelDB<String, String> db = await _openTestDB();

    db.put("k1", "v");
    db.put("k2", "v");

    expect(db.get("k1"), equals("v"));
    List<dynamic> keys = db.getItems().keys.toList();
    expect(keys.first, equals("k1"));

    String? v = db.get("DOESNOTEXIST");
    expect(v, equals(null));

    // All keys
    keys = db.getItems().keys.toList();
    expect(keys.length, equals(2));
    keys = db.getItems(gte: "k1").keys.toList();
    expect(keys.length, equals(2));
    keys = db.getItems(gt: "k1").keys.toList();
    expect(keys.length, equals(1));

    keys = db.getItems(gt: "k0").keys.toList();
    expect(keys.length, equals(2));

    keys = db.getItems(gt: "k5").keys.toList();
    expect(keys.length, equals(0));
    keys = db.getItems(gte: "k5").keys.toList();
    expect(keys.length, equals(0));

    keys = db.getItems(limit: 1).keys.toList();
    expect(keys.length, equals(1));

    keys = db.getItems(lte: "k2").keys.toList();
    expect(keys.length, equals(2));
    keys = db.getItems(lt: "k2").keys.toList();
    expect(keys.length, equals(1));

    keys = db.getItems(gt: "k1", lt: "k2").keys.toList();
    expect(keys.length, equals(0));

    keys = db.getItems(gte: "k1", lt: "k2").keys.toList();
    expect(keys.length, equals(1));

    keys = db.getItems(gt: "k1", lte: "k2").keys.toList();
    expect(keys.length, equals(1));

    keys = db.getItems(gte: "k1", lte: "k2").keys.toList();
    expect(keys.length, equals(2));

    db.close();

    LevelDB<Uint8List, Uint8List> db2 =
        await _openTestDBEnc(LevelDB.identity, LevelDB.identity, clean: false);

    // Test with LevelEncodingNone
    Uint8List key = new Uint8List(2);
    key[0] = "k".codeUnitAt(0);
    key[1] = "1".codeUnitAt(0);
    keys = db2.getItems(gt: key).keys.toList();
    expect(keys.length, equals(1));

    keys = db2.getItems(gte: key).keys.toList();
    expect(keys.length, equals(2));

    key[1] = "2".codeUnitAt(0);
    keys = db2.getItems(gt: key).keys.toList();
    expect(keys.length, equals(0));

    keys = db2.getItems(gte: key).keys.toList();
    expect(keys.length, equals(1));

    keys = db2.getItems(lt: key).keys.toList();
    expect(keys.length, equals(1));

    keys = db2.getItems(lt: key).values.toList();
    expect(keys.length, equals(1));

    db2.close();
  });

  test('LevelDB delete', () async {
    LevelDB<String, String> db = await _openTestDB();
    try {
      db.put("k1", "v");
      db.put("k2", "v");

      db.delete("k1");

      expect(db.get("k1"), equals(null));
      expect(db.getItems().length, 1);
    } finally {
      db.close();
    }
  });

  test('TWO DBS', () async {
    LevelDB<String, String> db1 = await _openTestDB();
    LevelDB<String, String> db2 = await _openTestDB(index: 1);

    db1.put("a", "1");

    String? v = db2.get("a");
    expect(v, equals(null));

    db1.close();
    db2.close();
  });

  test('Usage after close()', () async {
    LevelDB<String, String> db1 = await _openTestDB();
    db1.close();

    expect(() => db1.get("SOME KEY"), throwsA(_isClosedError));
    expect(() => db1.delete("SOME KEY"), throwsA(_isClosedError));
    expect(() => db1.put("SOME KEY", "SOME KEY"), throwsA(_isClosedError));
    expect(() => db1.close(), throwsA(_isClosedError));

    try {
      for (LevelItem<String, String> _ in db1.getItems()) {
        expect(true, equals(false)); // Should not happen.
      }
    } on LevelClosedError {
      expect(true, equals(true)); // Should happen.
    }
  });

  test('DB locking throws IOError', () async {
    LevelDB<String, String> db1 = await _openTestDB();
    try {
      await _openTestDB();
      expect(true, equals(false)); // Should not happen. The db is locked.
    } on LevelIOError {
      expect(true, equals(true)); // Should happen.
    } finally {
      db1.close();
    }
  });

  test('Exception inside iteration', () async {
    LevelDB<String, String> db1 = await _openTestDB();
    db1.put("a", "1");
    db1.put("b", "1");
    db1.put("c", "1");

    try {
      for (LevelItem<String, String> _ in db1.getItems()) {
        throw new Exception("OH NO");
      }
    } catch (e) {
      // Pass
    } finally {
      db1.close();
    }
  });

  test('Test with None encoding', () async {
    LevelDB<Uint8List, Uint8List> dbNone =
        await _openTestDBEnc(LevelDB.identity, LevelDB.identity, shared: true);
    LevelDB<String, String> dbAscii = await _openTestDBEnc(
        LevelDB.ascii, LevelDB.ascii,
        shared: true, clean: false);
    LevelDB<String, String> dbUtf8 = await _openTestDBEnc(
        LevelDB.utf8, LevelDB.utf8,
        shared: true, clean: false);
    Uint8List v = new Uint8List.fromList(utf8.encode("key1"));
    dbNone.put(v, v);

    String? s = dbUtf8.get("key1");
    expect(s, equals("key1"));

    String? s2 = dbAscii.get("key1");
    expect(s2, equals("key1"));

    Uint8List? v2 = dbNone.get(v);
    expect(v2, equals(v));

    dbNone.delete(v);
    expect(dbNone.get(v), null);
    dbNone.close();

    expect(dbAscii.get("key1"), null);
    dbAscii.close();

    expect(dbUtf8.get("key1"), null);
    dbUtf8.close();
  });

  test('Close inside iteration', () async {
    LevelDB<String, String> db1 = await _openTestDB();
    db1.put("a", "1");
    db1.put("b", "1");

    bool isClosedSeen = false;

    try {
      for (LevelItem<String, String> _ in db1.getItems()) {
        db1.close();
      }
    } on LevelClosedError catch (_) {
      isClosedSeen = true;
    }

    expect(isClosedSeen, equals(true));
  });

  test('Test no create if missing', () async {
    expect(
        LevelDB.openUtf8('/tmp/test-level-db-dart-DOES-NOT-EXIST',
            createIfMissing: false),
        throwsA(_isInvalidArgumentError));
  });

  test('Test error if exists', () async {
    LevelDB<String, String> db =
        await LevelDB.openUtf8('/tmp/test-level-db-dart-exists');
    db.close();
    expect(
        LevelDB.openUtf8('/tmp/test-level-db-dart-exists', errorIfExists: true),
        throwsA(_isInvalidArgumentError));
  });

  test('LevelDB sync iterator', () async {
    LevelDB<String, String> db = await _openTestDB();

    db.put("k1", "v");
    db.put("k2", "v");

    // All keys
    List<LevelItem<String, String>> items1 = db.getItems().toList();
    expect(items1.length, equals(2));
    expect(items1.map((LevelItem<String, String> i) => i.key).toList(),
        equals(<String>["k1", "k2"]));
    expect(items1.map((LevelItem<String, String> i) => i.value).toList(),
        equals(<String>["v", "v"]));

    List<LevelItem<String, String>> items = db.getItems(gte: "k1").toList();
    expect(items.length, equals(2));
    items = db.getItems(gt: "k1").toList();
    expect(items.length, equals(1));

    items = db.getItems(gt: "k0").toList();
    expect(items.length, equals(2));

    items = db.getItems(gt: "k5").toList();
    expect(items.length, equals(0));
    items = db.getItems(gte: "k5").toList();
    expect(items.length, equals(0));

    items = db.getItems(limit: 1).toList();
    expect(items.length, equals(1));

    items = db.getItems(lte: "k2").toList();
    expect(items.length, equals(2));
    items = db.getItems(lt: "k2").toList();
    expect(items.length, equals(1));

    items = db.getItems(gt: "k1", lt: "k2").toList();
    expect(items.length, equals(0));

    items = db.getItems(gte: "k1", lt: "k2").toList();
    expect(items.length, equals(1));

    items = db.getItems(gt: "k1", lte: "k2").toList();
    expect(items.length, equals(1));

    items = db.getItems(gte: "k1", lte: "k2").toList();
    expect(items.length, equals(2));

    String val =
        "bv-12345678901234567890123456789012345678901234567890123456789012345678901234567890";
    db.put("a", val);
    LevelItem<String, String> item = db.getItems(lte: "a").first;
    expect(item.value.length, val.length);

    String longKey = "";
    for (int _ in new Iterable<int>.generate(10)) {
      longKey += val;
    }
    db.put(longKey, longKey);
    item = db.getItems(gt: "a", lte: "c").first;
    expect(item.value.length, longKey.length);

    db.close();
  });

  test('LevelDB sync iterator use after close', () async {
    LevelDB<String, String> db = await _openTestDB();

    db.put("k1", "v");
    db.put("k2", "v");

    // All keys
    Iterator<LevelItem<String, String>> it = db.getItems().iterator;
    it.moveNext();

    db.close();

    expect(() => it.moveNext(), throwsA(_isClosedError));
  });

  test('LevelDB sync iterator current == null', () async {
    LevelDB<String, String> db = await _openTestDB();

    db.put("k1", "v");
    LevelIterator<String, String> it = db.getItems().iterator;
    expect(() => it.current, throwsA(_isIteratorError));
    expect(() => it.currentKey, throwsA(_isIteratorError));
    expect(() => it.currentValue, throwsA(_isIteratorError));

    it.moveNext();
    expect(it.current.key, "k1");
    expect(it.currentKey, "k1");
    expect(it.currentValue, "v");
    expect(it.moveNext(), false);
    expect(() => it.current, throwsA(_isIteratorError));
    for (int _ in new Iterable<int>.generate(10)) {
      expect(it.moveNext(),
          false); // Dart requires that it is safe to call moveNext after the end.
      expect(() => it.current, throwsA(_isIteratorError));
      expect(() => it.currentKey, throwsA(_isIteratorError));
      expect(() => it.currentValue, throwsA(_isIteratorError));
    }
    db.close();
  });

  test('Shared db in same isolate', () async {
    LevelDB<String, String> db = await _openTestDB(shared: true);
    LevelDB<String, String> db1 = await _openTestDB(shared: true);

    db.put("k1", "v");
    expect(db1.get("k1"), "v");

    // Close the 1st reference. It cannot be used now.
    db.close();
    expect(() => db.get("SOME KEY"), throwsA(_isClosedError));

    // db1 Should still work.
    db1.put("k1", "v2");
    expect(db1.get("k1"), "v2");

    // close the 2nd reference. It cannot be used.
    db1.close();
    expect(() => db1.get("SOME KEY"), throwsA(_isClosedError));
  });

  test('Shared db removed from map', () async {
    // Test that a shared db is correctly removed from the shared map when closed.
    LevelDB<String, String> db = await _openTestDB(shared: true);
    db.close();

    // Since the db is closed above it will be remove from the shared map and therefore
    // this will open a new db and we are allowed to read/write keys.
    LevelDB<String, String> db1 = await _openTestDB(shared: true);
    db1.put("k1", "v");
    expect(db1.get("k1"), "v");
  });

  test('Shared db isolates test', () async {
    // Spawn 2 isolates of which open and close the same shared db a lot in an attempt to find race conditions
    // in opening and closing the db.
    Future<Null> run(int index) {
      Completer<Null> completer = new Completer<Null>();
      RawReceivePort exitPort = new RawReceivePort((dynamic _) {
        if (!completer.isCompleted) {
          completer.complete();
        }
      });
      RawReceivePort errorPort =
          new RawReceivePort((Object v) => completer.completeError(v));
      Isolate.spawn(_isolateTest, index,
          onExit: exitPort.sendPort, onError: errorPort.sendPort);
      return completer.future;
    }

    await Future.wait(new Iterable<int>.generate(2).map(run), eagerError: true);
  });
}

// Must be a top-level because this function runs in another isolate.
Future<Null> _isolateTest(int v) async {
  for (int _ in new Iterable<int>.generate(1000)) {
    LevelDB<String, String> db = await _openTestDB(shared: true, clean: false);
    // Allocate an iterator.
    for (LevelItem<String, String> _ in db.getItems(limit: 2)) {
      // pass
    }
    db.close();

    await new Future<Null>.delayed(new Duration(milliseconds: 2));
  }
}
