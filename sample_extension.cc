
#include <stdlib.h>
#include <stdio.h>

#include <string>
using std::string;     // (or using namespace std if you want to use more of std.)

#include "include/dart_api.h"
#include "include/dart_native_api.h"

#include "leveldb/db.h"


Dart_NativeFunction ResolveName(Dart_Handle name,
                                int argc,
                                bool* auto_setup_scope);


DART_EXPORT Dart_Handle sample_extension_Init(Dart_Handle parent_library) {
  if (Dart_IsError(parent_library)) {
    return parent_library;
  }

  Dart_Handle result_code =
      Dart_SetNativeResolver(parent_library, ResolveName, NULL);
  if (Dart_IsError(result_code)) {
    return result_code;
  }

  result_code = Dart_CreateNativeWrapperClass(
      parent_library, Dart_NewStringFromCString("NativeDB"), 1);
  if (Dart_IsError(result_code)) {
    return result_code;
  }

  result_code = Dart_CreateNativeWrapperClass(
      parent_library, Dart_NewStringFromCString("NativeIterator"), 1);
  if (Dart_IsError(result_code)) {
    return result_code;
  }

  return Dart_Null();
}


Dart_Handle HandleError(Dart_Handle handle) {
  if (Dart_IsError(handle)) {
    Dart_PropagateError(handle);
  }
  return handle;
}


void DBOpen(Dart_NativeArguments arguments) {
  Dart_EnterScope();

  Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);

  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  Dart_Handle result =  Dart_SetNativeInstanceField(arg, 0, (intptr_t) db);
  printf("Address of x1 is %p\n", (void *)db);

  Dart_SetReturnValue(arguments, Dart_NewBoolean(true));
  Dart_ExitScope();
}


void DBPut(Dart_NativeArguments arguments) {
    Dart_EnterScope();

    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result = Dart_GetNativeInstanceField(arg, 0, &ptr);

    printf("Address of x is %p\n", (void *)ptr);
    leveldb::Status s;
    leveldb::DB* db = (leveldb::DB*) ptr;
    s = db->Put(leveldb::WriteOptions(), "v1", "v2");

    Dart_SetReturnValue(arguments, Dart_NewBoolean(true));
    Dart_ExitScope();
}


void DBGet(Dart_NativeArguments arguments) {
    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result =  Dart_GetNativeInstanceField(arg, 0, &ptr);

    leveldb::Status s;
    leveldb::DB* db = (leveldb::DB*) ptr;
    std:string value;
    s = db->Get(leveldb::ReadOptions(), "v1", &value);

    Dart_SetReturnValue(arguments, Dart_NewStringFromCString(value.c_str()));
}


void DBNewIterator(Dart_NativeArguments arguments) {
    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result = Dart_GetNativeInstanceField(arg, 0, &ptr);

    leveldb::Status s;
    leveldb::DB* db = (leveldb::DB*) ptr;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

    Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
    result =  Dart_SetNativeInstanceField(arg1, 0, (intptr_t) it);

    printf("Address of iterator is %p\n", (void *)it);

    Dart_SetReturnValue(arguments, Dart_Null());
}


void IteratorSeek(Dart_NativeArguments arguments) {
    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result = Dart_GetNativeInstanceField(arg, 0, &ptr);

    printf("Address of iterator2 is %p\n", (void *)ptr);

    leveldb::Status s;
    leveldb::Iterator* it = (leveldb::Iterator*) ptr;
    it->Seek("0");

    Dart_SetReturnValue(arguments, Dart_Null());
}


void IteratorValid(Dart_NativeArguments arguments) {
    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result = Dart_GetNativeInstanceField(arg, 0, &ptr);

    leveldb::Iterator* it = (leveldb::Iterator*) ptr;
    bool ret = it->Valid();

    Dart_SetReturnValue(arguments, Dart_NewBoolean(ret));
}


void IteratorNext(Dart_NativeArguments arguments) {
    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result = Dart_GetNativeInstanceField(arg, 0, &ptr);
    leveldb::Iterator* it = (leveldb::Iterator*) ptr;
    it->Next();
    Dart_SetReturnValue(arguments, Dart_Null());
}


void IteratorKey(Dart_NativeArguments arguments) {
    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result = Dart_GetNativeInstanceField(arg, 0, &ptr);
    leveldb::Iterator* it = (leveldb::Iterator*) ptr;
    leveldb::Slice key = it->key();
    Dart_SetReturnValue(arguments, Dart_NewStringFromCString(key.ToString().c_str()));
}


void IteratorValue(Dart_NativeArguments arguments) {
    Dart_Handle arg = Dart_GetNativeArgument(arguments, 0);
    intptr_t ptr;
    Dart_Handle result = Dart_GetNativeInstanceField(arg, 0, &ptr);
    leveldb::Iterator* it = (leveldb::Iterator*) ptr;
    leveldb::Slice value = it->value();
    Dart_SetReturnValue(arguments, Dart_NewStringFromCString(value.ToString().c_str()));
}


struct FunctionLookup {
  const char* name;
  Dart_NativeFunction function;
};


FunctionLookup function_list[] = {
    {"DBOpen", DBOpen},
    {"DBGet", DBGet},
    {"DBPut", DBPut},
    {"DBNewIterator", DBNewIterator},

    {"IteratorSeek", IteratorSeek},
    {"IteratorValid", IteratorValid},
    {"IteratorNext", IteratorNext},
    {"IteratorKey", IteratorKey},
    {"IteratorValue", IteratorValue},

    {NULL, NULL}};


FunctionLookup no_scope_function_list[] = {
  {NULL, NULL}
};

Dart_NativeFunction ResolveName(Dart_Handle name,
                                int argc,
                                bool* auto_setup_scope) {
  if (!Dart_IsString(name)) {
    return NULL;
  }
  Dart_NativeFunction result = NULL;
  if (auto_setup_scope == NULL) {
    return NULL;
  }

  Dart_EnterScope();
  const char* cname;
  HandleError(Dart_StringToCString(name, &cname));

  for (int i=0; function_list[i].name != NULL; ++i) {
    if (strcmp(function_list[i].name, cname) == 0) {
      *auto_setup_scope = true;
      result = function_list[i].function;
      break;
    }
  }

  if (result != NULL) {
    Dart_ExitScope();
    return result;
  }

  for (int i=0; no_scope_function_list[i].name != NULL; ++i) {
    if (strcmp(no_scope_function_list[i].name, cname) == 0) {
      *auto_setup_scope = false;
      result = no_scope_function_list[i].function;
      break;
    }
  }

  Dart_ExitScope();
  return result;
}

