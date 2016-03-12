
import 'dart:io';
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:leveldb/leveldb.dart';

Future<LevelDB> _openTestDB({int index: 0}) async {
  Directory d = new Directory('/tmp/test-level-db-dart-$index');
  if (d.existsSync()) {
    await d.delete(recursive: true);
  }
  return (await LevelDB.open('/tmp/test-level-db-dart-$index'));
}

const Matcher _isClosedError = const _ConcurrentModificationError._ClosedMatcher();

class _ConcurrentModificationError extends TypeMatcher {
  const _ConcurrentModificationError._ClosedMatcher() : super("LevelDBClosedError");
  @override
  bool matches(dynamic item, Map<dynamic, dynamic> matchState) => item is LevelDBClosedError;
}

/// tests
void main() {
  test('LevelDB', () async {
    LevelDB db = await _openTestDB();

    await db.put("k1", "v");
    await db.put("k2", "v");

    expect(await db.get("k1"), equals("v"));
    List<String> keys = await db.getKeys().toList();
    expect(keys.first, equals("k1"));

    String v = await db.get("DOESNOTEXIST");
    expect(v, equals(null));

    // All keys
    keys = await db.getKeys().toList();
    expect(keys.length, equals(2));
    keys = await db.getKeys(gte: "k1").toList();
    expect(keys.length, equals(2));
    keys = await db.getKeys(gt: "k1").toList();
    expect(keys.length, equals(1));

    keys = await db.getKeys(gt: "k0").toList();
    expect(keys.length, equals(2));

    keys = await db.getKeys(gt: "k5").toList();
    expect(keys.length, equals(0));
    keys = await db.getKeys(gte: "k5").toList();
    expect(keys.length, equals(0));

    keys = await db.getKeys(limit: 1).toList();
    expect(keys.length, equals(1));

    keys = await db.getKeys(lte: "k2").toList();
    expect(keys.length, equals(2));
    keys = await db.getKeys(lt: "k2").toList();
    expect(keys.length, equals(1));

    keys = await db.getKeys(gt: "k1", lt: "k2").toList();
    expect(keys.length, equals(0));

    keys = await db.getKeys(gte: "k1", lt: "k2").toList();
    expect(keys.length, equals(1));

    keys = await db.getKeys(gt: "k1", lte: "k2").toList();
    expect(keys.length, equals(1));

    keys = await db.getKeys(gte: "k1", lte: "k2").toList();
    expect(keys.length, equals(2));

    // Test with LevelEncodingNone
    Uint8List key = new Uint8List(2);
    key[0] = "k".codeUnitAt(0);
    key[1] = "1".codeUnitAt(0);
    keys = await db.getKeys(gt: key, keyEncoding: LevelEncoding.none).toList();
    expect(keys.length, equals(1));

    keys = await db.getKeys(gte: key, keyEncoding: LevelEncoding.none).toList();
    expect(keys.length, equals(2));

    key[1] = "2".codeUnitAt(0);
    keys = await db.getKeys(gt: key, keyEncoding: LevelEncoding.none).toList();
    expect(keys.length, equals(0));

    keys = await db.getKeys(gte: key, keyEncoding: LevelEncoding.none).toList();
    expect(keys.length, equals(1));

    keys = await db.getKeys(lt: key, keyEncoding: LevelEncoding.none).toList();
    expect(keys.length, equals(1));

    keys = await db.getValues(lt: key, keyEncoding: LevelEncoding.none).toList();
    expect(keys.length, equals(1));

    await db.close();
  });

  test('LevelDB delete', () async {
    LevelDB db = await _openTestDB();
    try {
      await db.put("k1", "v");
      await db.put("k2", "v");

      await db.delete("k1");

      expect(await db.get("k1"), equals(null));
      expect((await db.getItems().toList()).length, 1);
    } finally {
      await db.close();
    }
  });

  test('TWO DBS', () async {
    LevelDB db1 = await _openTestDB();
    LevelDB db2 = await _openTestDB(index: 1);

    await db1.put("a", "1");

    String v = await db2.get("a");
    expect(v, equals(null));

    await db1.close();
    await db2.close();
  });

  test('Usage after close()', () async {
    LevelDB db1 = await _openTestDB();
    await db1.close();

    expect(db1.get("SOME KEY"), throwsA(_isClosedError));
    expect(db1.delete("SOME KEY"), throwsA(_isClosedError));
    expect(db1.put("SOME KEY", "SOME KEY"), throwsA(_isClosedError));
    expect(db1.close(), throwsA(_isClosedError));

    try {
      await for (List<String> _ in db1.getItems()) {
        expect(true, equals(false)); // Should not happen.
      }
    } on LevelDBClosedError {
      expect(true, equals(true)); // Should happen.
    }
  });

  test('DB locking throws IOError', () async {
    LevelDB db1 = await _openTestDB();
    try {
      await _openTestDB();
      expect(true, equals(false)); // Should not happen. The db is locked.
    } on LevelDBIOError {
      expect(true, equals(true)); // Should happen.
    } finally {
      await db1.close();
    }
  });

  test('Exception inside iteration', () async {
    LevelDB db1 = await _openTestDB();
    await db1.put("a", "1");
    await db1.put("b", "1");
    await db1.put("c", "1");

    try {
      await for (List<String> _ in db1.getItems()) {
        throw new Exception("OH NO");
      }
    } catch (e) {
      // Pass
    } finally {
      await db1.close();
    }
  });

  test('Test with None encoding', () async {
    LevelDB db1 = await _openTestDB();
    Uint8List v = new Uint8List.fromList(UTF8.encode("key1"));

    await db1.put(v, v, keyEncoding: LevelEncoding.none, valueEncoding: LevelEncoding.none);

    String s = await db1.get("key1");
    expect(s, equals("key1"));

    String s2 = await db1.get("key1", keyEncoding: LevelEncoding.ascii);
    expect(s2, equals("key1"));

    Uint8List v2 = await db1.get(v, keyEncoding: LevelEncoding.none, valueEncoding: LevelEncoding.none);
    expect(v2, equals(v));

    await db1.delete(v, keyEncoding: LevelEncoding.none);

    await db1.close();
  });

  test('Close inside iteration', () async {
    LevelDB db1 = await _openTestDB();
    await db1.put("a", "1");
    await db1.put("b", "1");

    bool isClosedSeen = false;

    try {
      await for (List<String> _ in db1.getItems()) {
        await db1.close();
      }
    } on LevelDBClosedError catch (_) {
      isClosedSeen = true;
    }

    expect(isClosedSeen, equals(true));
  });

}