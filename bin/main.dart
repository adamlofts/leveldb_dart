
import 'dart:async';

import 'package:leveldb_dart/sample_extension/sample_synchronous_extension.dart';

Future main() async {
  DB c = DB.open();
  c.put();
  print(c.get());
  LevelIterator it = c.iterator;
  it.seek();

  print("VALID ${it.valid} ${it.key} ${it.value}");
  it.next();
  print("VALID ${it.valid}");


  print("DONE");
}
