
import 'dart:async';

import 'package:leveldb_dart/sample_extension/sample_synchronous_extension.dart';

Future main() async {
  DB c = DB.open("/tmp/testdb");
  c.put("A", "B1");
  print(c.get("A"));
  c.delete("A");
  LevelIterator it = c.iterator;
  it.seek();
  while (it.valid) {
    print("${it.key} ${it.value}");
    it.next();
  }
}
