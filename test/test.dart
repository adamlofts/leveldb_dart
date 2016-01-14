
import 'dart:io';

import 'package:test/test.dart';
import 'package:leveldb_dart/sample_extension/sample_synchronous_extension.dart';

LevelDB openTestDB() {
  Directory d = new Directory('/tmp/test-level-db-dart');
  if (d.existsSync()) {
    d.delete(recursive: true);
  }
  return LevelDB.open('/tmp/test-level-db-dart');
}

void main() {
  test('LevelDB', () {
    LevelDB db = openTestDB();
    db.put("k", "v");
    expect(db.get("k"), equals("v"));
    LevelIterator it = db.iterator;
    it.seek();
    expect(it.valid, equals(true));
    expect(it.key, equals("k"));
    expect(it.value, equals("v"));
    it.next();
//    expect(it.valid, equals(false));
  });
}