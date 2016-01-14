
#include <stdlib.h>
#include <stdio.h>
#include <memory>

#include <string>
using namespace std;     // (or using namespace std if you want to use more of std.)

#include "include/dart_api.h"
#include "include/dart_native_api.h"

#include "leveldb/db.h"


Dart_NativeFunction ResolveName(Dart_Handle name,
                                int argc,
                                bool* auto_setup_scope);


DART_EXPORT Dart_Handle leveldb_Init(Dart_Handle parent_library) {
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
      parent_library, Dart_NewStringFromCString("NativeIterator"), 2);
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

// FIXME:
leveldb::DB* global_db;
bool is_closed = false;  // FIXME: Synchronized

Dart_CObject* AllocateDartCObjectArray(intptr_t length) {
  // Allocate a Dart_CObject structure followed by an array of
  // pointers to Dart_CObject structures. The pointer to the array
  // content is set up to this area.
  Dart_CObject* value =
      reinterpret_cast<Dart_CObject*>(
          malloc(sizeof(Dart_CObject) + length * sizeof(value)));
  assert(value != NULL);
  value->type = Dart_CObject_kArray;
  value->value.as_array.length = length;
  if (length > 0) {
    value->value.as_array.values = reinterpret_cast<Dart_CObject**>(value + 1);
  } else {
    value->value.as_array.values = NULL;
  }
  return value;
}

void levelServiceHandler(Dart_Port dest_port_id, Dart_CObject* message) {
  Dart_Port reply_port_id = ILLEGAL_PORT;

  // First arg should always be the reply port.
  if (message->type == Dart_CObject_kArray &&
    message->value.as_array.length > 0) {
    Dart_CObject* param0 = message->value.as_array.values[0];
    if (param0->type == Dart_CObject_kSendPort) {
      reply_port_id = param0->value.as_send_port.id;
    }
  }

  // Second arg should be message type
  if (message->type == Dart_CObject_kArray &&
      message->value.as_array.length > 1) {

    Dart_CObject* param1 = message->value.as_array.values[1];
    int msg = param1->value.as_int32;

    if (msg == 1 &&
        message->value.as_array.length > 2) { // open(path)
      Dart_CObject* param2 = message->value.as_array.values[2];

      if (param2->type == Dart_CObject_kString) {
        const char* path = param2->value.as_string;

        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status status = leveldb::DB::Open(options, path, &global_db);
        assert(status.ok());

        Dart_CObject result;
        result.type = Dart_CObject_kInt32;
        result.value.as_int32 = 0;
        Dart_PostCObject(reply_port_id, &result);
        //        // It is OK that result is destroyed when function exits.
        //        // Dart_PostCObject has copied its data.
        return;
      }
    }

    if (msg == 2 &&
        message->value.as_array.length == 2) { // close()

      delete global_db;
      is_closed = true;

      Dart_CObject result;
      result.type = Dart_CObject_kInt32;
      result.value.as_int32 = 0;
      Dart_PostCObject(reply_port_id, &result);
      return;
    }

    if (msg == 3 &&
        message->value.as_array.length == 3) { // get(key)
      Dart_CObject* param2 = message->value.as_array.values[2];

      if (param2->type == Dart_CObject_kString) {
        const char* key = param2->value.as_string;

        leveldb::Status s;
        std:string value;
        s = global_db->Get(leveldb::ReadOptions(), key, &value);

        Dart_CObject result;
        result.type = Dart_CObject_kString;
        result.value.as_string = (char*) value.c_str();  // FIXME: Is thi sallowed?
        Dart_PostCObject(reply_port_id, &result);
        return;
      }
    }

    if (msg == 3 &&
        message->value.as_array.length == 4) { // put(key, value)
      Dart_CObject* param2 = message->value.as_array.values[2];
      Dart_CObject* param3 = message->value.as_array.values[3];

      if (param2->type == Dart_CObject_kString) {
        const char* key = param2->value.as_string;
        const char* value = param3->value.as_string;

        leveldb::Status s;
        s = global_db->Put(leveldb::WriteOptions(), key, value);

        Dart_CObject result;
        result.type = Dart_CObject_kInt32;
        result.value.as_int32 = 0;
        Dart_PostCObject(reply_port_id, &result);
        return;
      }
    }

    if (msg == 4 &&
            message->value.as_array.length == 3) { // delete(key)
      Dart_CObject* param2 = message->value.as_array.values[2];

      if (param2->type == Dart_CObject_kString) {
        const char* key = param2->value.as_string;

        leveldb::Status s;
        s = global_db->Delete(leveldb::WriteOptions(), key);

        Dart_CObject result;
        result.type = Dart_CObject_kInt32;
        result.value.as_int32 = 0;
        Dart_PostCObject(reply_port_id, &result);
        return;
      }
    }

    if (msg == 5 &&
            message->value.as_array.length == 2) { // stream()

      leveldb::Iterator* it = global_db->NewIterator(leveldb::ReadOptions());
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        leveldb::Slice value = it->value();

        Dart_CObject* values[2];

        Dart_CObject r1;
        r1.type = Dart_CObject_kTypedData;
        r1.value.as_typed_data.type = Dart_TypedData_kUint8;
        // It is OK not to copy the slice data because Dart_PostCObject has copied its data.
        r1.value.as_typed_data.values = (uint8_t*) key.data();
        r1.value.as_typed_data.length = key.size();
        values[0] = &r1;

        Dart_CObject r2;
        r2.type = Dart_CObject_kTypedData;
        r2.value.as_typed_data.type = Dart_TypedData_kUint8;
        // It is OK not to copy the slice data because Dart_PostCObject has copied its data.
        r2.value.as_typed_data.values = (uint8_t*) value.data();
        r2.value.as_typed_data.length = value.size();
        values[1] = &r2;

        Dart_CObject result;
        result.type = Dart_CObject_kArray;
        result.value.as_array.length = 2;
        result.value.as_array.values = values;

        // It is OK that result is destroyed when function exits.
        // Dart_PostCObject has copied its data.
        Dart_PostCObject(reply_port_id, &result);
      }

      assert(it->status().ok());  // Check for any errors found during the scan
      delete it;

      Dart_CObject result;
      result.type = Dart_CObject_kInt32;
      result.value.as_int32 = 0;
      Dart_PostCObject(reply_port_id, &result);
      return;

    }
  }

  Dart_CObject result;
  result.type = Dart_CObject_kNull;
  Dart_PostCObject(reply_port_id, &result);
}

/**
 Creates a port representing a level db thread
*/
void dbServicePort(Dart_NativeArguments arguments) {
  Dart_EnterScope();
  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_Port service_port =
      Dart_NewNativePort("LevelService", levelServiceHandler, true /* handle concurrently */);
  if (service_port != ILLEGAL_PORT) {
    Dart_Handle send_port = HandleError(Dart_NewSendPort(service_port));
    Dart_SetReturnValue(arguments, send_port);
  }
  Dart_ExitScope();
}

struct FunctionLookup {
  const char* name;
  Dart_NativeFunction function;
};

FunctionLookup function_list[] = {
    {"DB_ServicePort", dbServicePort},

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

