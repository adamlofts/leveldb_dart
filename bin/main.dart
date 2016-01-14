
import 'dart:async';

import 'package:leveldb/leveldb.dart';

Future main() async {
  LevelDB db = new LevelDB("/tmp/testdb");

  var v = await db.open();
  v = await db.put("v1", "a");
  v = await db.put("v2", "ab");
  v = await db.put("v3", "ac");
  v = await db.get("v1");
//  await db.delete("v1");

  Stream s = db.getItems();
  await for (var v in s) {
    print("IT: ${v.runtimeType} ${v.toString()}");
  }
}
