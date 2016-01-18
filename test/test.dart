
import 'dart:io';
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:leveldb/leveldb.dart';

Future<LevelDB> openTestDB({int index: 0}) async {
  Directory d = new Directory('/tmp/test-level-db-dart-$index');
  if (d.existsSync()) {
    await d.delete(recursive: true);
  }
  return (await LevelDB.open('/tmp/test-level-db-dart-$index'));
}

Uint8List fromString(String v) {
  return new Uint8List.fromList(UTF8.encode(v));
}

void main() {
  test('LevelDB', () async {
    LevelDB db = await openTestDB();

    await db.put(fromString("k1"), fromString("v"));
    await db.put(fromString("k2"), fromString("v"));

    expect(await db.get(fromString("k1")), equals(fromString("v")));
    List keys = await db.getKeys().toList();
    Uint8List key = keys.first;
    String keyString = UTF8.decode(key);
    expect(keyString, equals("k1"));

    var v = await db.get(fromString("DOESNOTEXIST"));
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

    await db.close();
  });

  test('TWO DBS', () async {
    LevelDB db1 = await openTestDB();
    LevelDB db2 = await openTestDB(index: 1);

    await db1.put(fromString("a"), fromString("1"));

    var v = await db2.get(fromString("a"));
    expect(v, equals(null));

    await db1.close();
    await db2.close();
  });

  test('Usage after close()', () async {
    LevelDB db1 = await openTestDB();
    await db1.close();
    expect(db1.get(fromString("SOME KEY")), throwsA(const LevelDBClosedError()));
    expect(db1.delete(fromString("SOME KEY")), throwsA(const LevelDBClosedError()));
    expect(db1.put(fromString("SOME KEY"), fromString("SOME KEY")), throwsA(const LevelDBClosedError()));
    expect(db1.close(), throwsA(const LevelDBClosedError()));

    try {
      await for (var _ in db1.getItems()) {
        expect(true, equals(false)); // Should not happen.
      }
    } on LevelDBClosedError {
      expect(true, equals(true)); // Should happen.
    }
  });

  test('DB locking throws IOError', () async {
    LevelDB db1 = await openTestDB();
    try {
      await openTestDB();
      expect(true, equals(false)); // Should not happen. The db is locked.
    } on LevelDBIOError {
      expect(true, equals(true)); // Should happen.
    } finally {
      await db1.close();
    }
  });

  test('Exception inside iteration', () async {
    LevelDB db1 = await openTestDB();
    await db1.put(fromString("a"), fromString("1"));
    await db1.put(fromString("b"), fromString("1"));
    await db1.put(fromString("c"), fromString("1"));

    try {
      await for (var row in db1.getItems()) {
        throw new Exception("OH NO");
      }
    } catch (e) {
      // Pass
    }
  });

}