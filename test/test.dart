
import 'dart:io';
import 'dart:async';

import 'package:test/test.dart';
import 'package:leveldb_dart/sample_extension/sample_synchronous_extension.dart';

Future<LevelDB> openTestDB() async {
  Directory d = new Directory('/tmp/test-level-db-dart');
  if (d.existsSync()) {
    d.delete(recursive: true);
  }
  LevelDB db = new LevelDB('/tmp/test-level-db-dart');
  await db.open();
  return db;
}

void main() {
  test('LevelDB', () async {
    LevelDB db = await openTestDB();
    await db.put("k", "v");
    expect(await db.get("k"), equals("v"));

    await for (var v in db.getKeys()) {
      expect(v, equals("k"));
    }
  });
}