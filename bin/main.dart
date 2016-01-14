
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:leveldb/leveldb.dart';

Future main() async {
  LevelDB db = new LevelDB("/tmp/testdb");

  Uint8List key = new Uint8List(2);
  key[0] = 118;
  key[1] = 49;

  var v = await db.open();
  v = await db.put(key, key);

  v = await db.get(key);
  print(v);

  await db.delete(key);

  v = await db.get(key);
  print(v);



  Stream s = db.getItems();
  await for (var v in s) {
    print("IT: ${v.runtimeType} ${v.toString()}");
  }
}
