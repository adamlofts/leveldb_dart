
#include <stdlib.h>
#include <stdio.h>
#include <memory>

#include <string>
using namespace std;     // (or using namespace std if you want to use more of std.)

#include "include/dart_api.h"
#include "include/dart_native_api.h"

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"

const int BLOOM_BITS_PER_KEY = 10;

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

  return Dart_Null();
}


Dart_Handle HandleError(Dart_Handle handle) {
  if (Dart_IsError(handle)) {
    Dart_PropagateError(handle);
  }
  return handle;
}


void levelServiceHandler(Dart_Port dest_port_id, Dart_CObject* message) {

  // First arg should always be the reply port.
  Dart_Port reply_port_id = ILLEGAL_PORT;
  if (message->type == Dart_CObject_kArray &&
    message->value.as_array.length > 0) {
    Dart_CObject* param0 = message->value.as_array.values[0];
    if (param0->type == Dart_CObject_kSendPort) {
      reply_port_id = param0->value.as_send_port.id;
    }
  }

  // Second arg is always message type
  int msg = 0;
  if (message->type == Dart_CObject_kArray &&
      message->value.as_array.length > 1) {

    Dart_CObject* param1 = message->value.as_array.values[1];
    msg = param1->value.as_int32;
  }

  if (msg == 1) { // open(path)
    Dart_CObject* param2 = message->value.as_array.values[2];

    if (param2->type == Dart_CObject_kString) {
      const char* path = param2->value.as_string;

      leveldb::Options options;
      options.create_if_missing = true;
      options.filter_policy = leveldb::NewBloomFilterPolicy(BLOOM_BITS_PER_KEY);

      leveldb::DB* new_db;
      leveldb::Status status = leveldb::DB::Open(options, path, &new_db);
      assert(status.ok());

      Dart_CObject result;
      result.type = Dart_CObject_kInt64;
      result.value.as_int64 = (int64_t) new_db;
      Dart_PostCObject(reply_port_id, &result);
      //        // It is OK that result is destroyed when function exits.
      //        // Dart_PostCObject has copied its data.
      return;
    }
  }

  leveldb::DB* db = NULL;
  if (msg > 1) {
    // All messages below have param2 as the pointer.
    Dart_CObject* param2 = message->value.as_array.values[2];
    if (param2->type == Dart_CObject_kInt64) {
      db = (leveldb::DB*) param2->value.as_int64;
    }
  }

  if (db != NULL) {
    if (msg == 2 &&
        message->value.as_array.length == 3) { // close()

      delete db;

      Dart_CObject result;
      result.type = Dart_CObject_kInt32;
      result.value.as_int32 = 0;
      Dart_PostCObject(reply_port_id, &result);
      return;
    }

    if (msg == 3 &&
        message->value.as_array.length == 4) { // get(key)
      Dart_CObject* param3 = message->value.as_array.values[3];

      if (param3->type == Dart_CObject_kTypedData) {
        leveldb::Slice key = leveldb::Slice((const char*)param3->value.as_typed_data.values, param3->value.as_typed_data.length);
        leveldb::Status s;
        std:string value;
        s = db->Get(leveldb::ReadOptions(), key, &value);

        if (s.IsNotFound()) {
          Dart_CObject result;
          result.type = Dart_CObject_kInt32;
          result.value.as_int32 = 0;
          Dart_PostCObject(reply_port_id, &result);
          return;
        }

        // FIXME: s.ok() <- raise exeception

        Dart_CObject result;
        result.type = Dart_CObject_kTypedData;
        result.value.as_typed_data.type = Dart_TypedData_kUint8;
        // It is OK not to copy the slice data because Dart_PostCObject has copied its data.
        result.value.as_typed_data.values = (uint8_t*) value.data();
        result.value.as_typed_data.length = value.size();
        Dart_PostCObject(reply_port_id, &result);
        return;
      }
    }

    if (msg == 4 &&
        message->value.as_array.length == 6) { // put(key, value, sync)
      Dart_CObject* param3 = message->value.as_array.values[3];
      Dart_CObject* param4 = message->value.as_array.values[4];
      Dart_CObject* param5 = message->value.as_array.values[5];

      if (param3->type == Dart_CObject_kTypedData &&
          param4->type == Dart_CObject_kTypedData &&
          param5->type == Dart_CObject_kBool) {

        leveldb::Slice key = leveldb::Slice((const char*)param3->value.as_typed_data.values, param3->value.as_typed_data.length);
        leveldb::Slice value = leveldb::Slice((const char*)param4->value.as_typed_data.values, param4->value.as_typed_data.length);

        leveldb::Status s;
        leveldb::WriteOptions options;
        options.sync = param5->value.as_bool;
        s = db->Put(options, key, value);

        Dart_CObject result;
        result.type = Dart_CObject_kInt32;
        result.value.as_int32 = 0;
        Dart_PostCObject(reply_port_id, &result);
        return;
      }
    }

    if (msg == 5 &&
            message->value.as_array.length == 4) { // delete(key)
      Dart_CObject* param3 = message->value.as_array.values[3];

      if (param3->type == Dart_CObject_kTypedData) {
        leveldb::Slice key = leveldb::Slice((const char*)param3->value.as_typed_data.values, param3->value.as_typed_data.length);

        leveldb::Status s;
        s = db->Delete(leveldb::WriteOptions(), key);

        Dart_CObject result;
        result.type = Dart_CObject_kInt32;
        result.value.as_int32 = 0;
        Dart_PostCObject(reply_port_id, &result);
        return;
      }
    }

    if (msg == 6 &&
            message->value.as_array.length == 9) { // stream()

      Dart_CObject* param_limit = message->value.as_array.values[3];
      int64_t limit = -1;
      if (param_limit->type == Dart_CObject_kInt32) {
        limit = param_limit->value.as_int32;
      } else if (param_limit->type == Dart_CObject_kInt64) {
        limit = param_limit->value.as_int64;
      }

      Dart_CObject* param_fill_cache = message->value.as_array.values[4];
      bool fill_cache = param_fill_cache->value.as_bool;

      leveldb::ReadOptions options;
      options.fill_cache = fill_cache;

      leveldb::Iterator* it = db->NewIterator(options);

      Dart_CObject* param_start = message->value.as_array.values[5];
      if (param_start->type == Dart_CObject_kTypedData) {
        leveldb::Slice start_slice = leveldb::Slice((const char*)param_start->value.as_typed_data.values, param_start->value.as_typed_data.length);
        it->Seek(start_slice);

        Dart_CObject* param_start_inclusive = message->value.as_array.values[6];
        bool is_start_inclusive = param_start_inclusive->value.as_bool;
        if (!is_start_inclusive && it->Valid()) {
          // If we are pointing at start_slice and not inclusive then we need to advance by 1
          leveldb::Slice key = it->key();
          if (key.compare(start_slice) == 0) {
            it->Next();
          }
        }
      } else {
        it->SeekToFirst();
      }

      Dart_CObject* param_end = message->value.as_array.values[7];
      leveldb::Slice end_slice;
      bool is_end_inclusive = false;

      if (param_end->type == Dart_CObject_kTypedData) {
         end_slice = leveldb::Slice((const char*)param_end->value.as_typed_data.values, param_end->value.as_typed_data.length);
         Dart_CObject* param_end_inclusive = message->value.as_array.values[8];
         is_end_inclusive = param_end_inclusive->value.as_bool;
      }

      int64_t count = 0;
      while (it->Valid()) {
        if (limit >= 0 && count >= limit) {
          break;
        }

        leveldb::Slice key = it->key();
        leveldb::Slice value = it->value();

        // Check if key is equal to end slice
        if (!end_slice.empty()) {

          int cmp = key.compare(end_slice);
          if (cmp == 0 && !is_end_inclusive) {  // key == end_slice and not inclusive
            break;
          }
          if (cmp > 0) { // key > end_slice
            break;
          }
        }

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

        count += 1;
        it->Next();
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

