
import 'dart:async';

import 'package:leveldb/leveldb.dart';

/// main example.
Future<dynamic> main() async {

  // Open a database. It is created if it does not already exist. Only one process can
  // open a database at a time.
  LevelDB db = await LevelDB.open("/tmp/testdb");

  // All database operations are asynchronous. We use the dart await functionality to wait for
  // the operation to complete before continuing.
  // By default keys and values are dart strings.
  db.put("abc", "def");

  // Now get the key
  String value = db.get("abc");
  print("value is $value"); // value2 is def

  // Delete the key
  db.delete("abc");

  // If a key does not exist we get null back
  String value3 = db.get("abc");
  print("value3 is $value3"); // value3 is null

  // Now lets add a few key-value pairs
  for (int i in new Iterable<int>.generate(5)) {
    db.put("key-$i", "value-$i");
  }

  // Iterate through the key-value pairs in key order.
  for (LevelItem v in db.getItems()) {
    print("Row: ${v.key} ${v.value}");  // prints Row: key-0 value-0, Row: key-1 value-1, ...
  }

  // Iterate keys between key-1 and key-3
  for (LevelItem v in db.getItems(gte: "key-1", lte: "key-3")) {
    print("Row: ${v.key} ${v.value}");  // prints Row: key-1 value-1, Row: key-2 value-2, Row: key-3 value-3
  }

  LevelIterator it = db.getItems(limit: 1).iterator;
  while (it.moveNext()) {
    print("${it.currentKey} ${it.currentValue}");
  }

  // Just key iteration
  for (dynamic key in db.getKeys()) {
    print("Key $key"); // Prints Key key-0, Key key-1, ...
  }

  // Value iteration
  for (dynamic value in db.getValues()) {
    print("Value $value"); // Prints Key value-0, Key value-1, ...
  }

  // Close the db. This free's all resources associated with the db.
  db.close();
}
