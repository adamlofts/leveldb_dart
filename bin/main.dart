
import 'dart:async';

import 'package:leveldb_dart/sample_extension/sample_synchronous_extension.dart';

Future main() async {
  LevelDB c = LevelDB.open("/tmp/testdb");

  print("Putting keys...");
  DateTime start = new DateTime.now();

  int numKeys = 1000000;
  for (int i = 0; i < numKeys; i += 1) {
    String key = "key-$i";
    String value = i.toString();
    c.put(key, value);
  }
  Duration duration = new DateTime.now().difference(start);
  print("Done in $duration (${duration.inMilliseconds / numKeys} millisec per key)");

  print("Iterating keys");
  start = new DateTime.now();
  LevelIterator it = c.iterator;
  it.seek();
  int count = 0;
  while (it.valid) {
    String key = it.key;
    it.next();
    count += 1;
  }
  print("Found $count keys");
  duration = new DateTime.now().difference(start);
  print("Done in $duration (${duration.inMilliseconds / numKeys} millisec per key)");
}
