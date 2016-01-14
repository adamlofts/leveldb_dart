
import 'dart:io';
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:leveldb/leveldb.dart';

Future<LevelDB> openTestDB() async {
  Directory d = new Directory('/tmp/test-level-db-dart');
  if (d.existsSync()) {
    d.delete(recursive: true);
  }
  LevelDB db = new LevelDB('/tmp/test-level-db-dart');
  await db.open();
  return db;
}

Uint8List fromString(String v) {
  return new Uint8List.fromList(UTF8.encode(v));
}

void main() {
  test('LevelDB', () async {
    LevelDB db = await openTestDB();

    await db.put(fromString("k"), fromString("v"));

    expect(await db.get(fromString("k")), equals(fromString("v")));
    List keys = await db.getKeys().toList();
    Uint8List key = keys.first;
    String keyString = UTF8.decode(key);
    expect(keyString, equals("k"));

    var v = await db.get(fromString("DOESNOTEXIST"));
    expect(v, equals(null));
  });
}