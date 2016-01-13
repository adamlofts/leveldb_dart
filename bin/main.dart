

import 'package:leveldb_dart/sample_extension/sample_asynchronous_extension.dart';

void main() async {
	List<int> ret = await (new RandomArray().randomArray(1, 2));
	print("HOLA $ret");
}
