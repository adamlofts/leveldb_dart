
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <memory>
#include <mutex>

#include <queue>
#include <list>

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

  result_code = Dart_CreateNativeWrapperClass(parent_library, Dart_NewStringFromCString("NativeDB"), 1);
  if (Dart_IsError(result_code)) {
    return result_code;
  }
  result_code = Dart_CreateNativeWrapperClass(parent_library, Dart_NewStringFromCString("NativeIterator"), 1);
  if (Dart_IsError(result_code)) {
    return result_code;
  }

  return Dart_Null();
}


struct IteratorRef;


struct DBRef {
  leveldb::DB *db;

  std::mutex mtx;
  std::list<IteratorRef*> *iterators;  // Guarded
  bool is_finalized;  // Guarded
};


struct RowRequest {
  int64_t max_size;
  Dart_Port reply_port_id;
};


struct IteratorRef {
  DBRef *db_ref;
  leveldb::Iterator *iterator;

  std::mutex mtx;
  std::queue<RowRequest*> *request_queue; // Guarded

  pthread_t thread; // Guarded
  bool is_paused; // Guarded
  bool is_finalized; // Guarded

  // Iterator params
  int64_t limit;
  bool is_gt_closed;
  bool is_lt_closed;
  uint8_t* gt;
  int64_t gt_len;
  uint8_t* lt;
  int64_t lt_len;

  // Iterator state
  bool is_seek_done;
  int64_t count;
};

/**
 * If the worker is running then ask it to stop and wait for it to do so.
 */
static void iteratorPauseAndJoin(IteratorRef *it_ref) {
  bool is_pausing = false;
  pthread_t thread;
  it_ref->mtx.lock();
  if (!it_ref->is_paused) {
    is_pausing = true;
    it_ref->is_paused = true;
    thread = it_ref->thread;
    it_ref->thread = 0;
  }
  it_ref->mtx.unlock();

  if (is_pausing) {
    int rc = pthread_join(thread, NULL);
  }
}


/**
 * Finalize the iterator. Joins up with the iterator thread and removes the iterator from the db list. Thread safe
 */
static void iteratorFinalize(IteratorRef *it_ref) {
  bool is_finalized = false;
  it_ref->mtx.lock();
  is_finalized = it_ref->is_finalized;
  it_ref->is_finalized = true;
  it_ref->mtx.unlock();

  if (is_finalized) {
    return;
  }

  iteratorPauseAndJoin(it_ref);

  delete it_ref->iterator;
  it_ref->iterator = NULL;

  // Remove the iterator from the dbs list
  it_ref->db_ref->iterators->remove(it_ref);

  // Send an error to every socket in the request queue
  while (!it_ref->request_queue->empty()) {
    RowRequest* request = it_ref->request_queue->front();

    Dart_CObject result;
    result.type = Dart_CObject_kInt32;
    result.value.as_int32 = -1;
    Dart_PostCObject(request->reply_port_id, &result);

    delete it_ref->request_queue->front();
    it_ref->request_queue->pop();
  }
}

/**
 * Stop all iterators, close the db.
 * Thread safe.
 */
static void finalizeDB(DBRef *db_ref) {
  bool is_finalized;
  db_ref->mtx.lock();
  is_finalized = db_ref->is_finalized;
  db_ref->is_finalized = true;
  db_ref->mtx.unlock();

  if (is_finalized) {
    return;
  }

  // Finalize every iterator. This will join with all iterator threads then the iterator will
  // remove itself from the iterators array.
  while (!db_ref->iterators->empty()) {
    iteratorFinalize(db_ref->iterators->front());
  }
  delete db_ref->iterators;

  // Close the db
  delete db_ref->db;
}


/**
 * Finalizer called when the dart LevelDB instance is not reachable.
 * */
static void NativeDBFinalizer(void* isolate_callback_data, Dart_WeakPersistentHandle handle, void* peer) {
  DBRef* db_ref = (DBRef*) peer;

  // It is possible that db is still open if the user forgot to close()
  finalizeDB(db_ref);
}


Dart_Handle HandleError(Dart_Handle handle) {
  if (Dart_IsError(handle)) {
    Dart_PropagateError(handle);
  }
  return handle;
}


/**
 * Finalizer called when the dart LevelDB instance is not reachable.
 *
 * */
static void NativeIteratorFinalizer(void* isolate_callback_data, Dart_WeakPersistentHandle handle, void* peer) {
  IteratorRef* it_ref = (IteratorRef*) peer;

  iteratorFinalize(it_ref);

  // Thread is stopped. Free all memory.
  assert(it_ref->iterator == NULL);
  delete it_ref->gt;
  delete it_ref->lt;

  assert(it_ref->request_queue->empty());
  delete it_ref->request_queue;

  delete it_ref;
}


int32_t levelDBServiceHandler(Dart_Port reply_port_id, DBRef *db_ref, int msg, Dart_CObject* message) {

  if (msg == 2 &&
      message->value.as_array.length == 3) { // close()

    finalizeDB(db_ref);

    Dart_CObject result;
    result.type = Dart_CObject_kInt32;
    result.value.as_int32 = 0;
    Dart_PostCObject(reply_port_id, &result);
    return 0;
  }

  leveldb::DB* db = db_ref->db;

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
        return 0;
      }

      // FIXME: s.ok() <- raise exeception

      Dart_CObject result;
      result.type = Dart_CObject_kTypedData;
      result.value.as_typed_data.type = Dart_TypedData_kUint8;
      // It is OK not to copy the slice data because Dart_PostCObject has copied its data.
      result.value.as_typed_data.values = (uint8_t*) value.data();
      result.value.as_typed_data.length = value.size();
      Dart_PostCObject(reply_port_id, &result);
      return 0;
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
      return 0;
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
      return 0;
    }
  }

  return -2;
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

      if (status.IsIOError()) {
        Dart_CObject result;
        result.type = Dart_CObject_kInt32;
        result.value.as_int32 = -2;
        Dart_PostCObject(reply_port_id, &result);
        return;
      }
      assert(status.ok());

      DBRef* db_ref = new DBRef();
      db_ref->iterators = new std::list<IteratorRef*>();
      db_ref->is_finalized = false;
      db_ref->db = new_db;

      Dart_CObject result;
      result.type = Dart_CObject_kInt64;
      result.value.as_int64 = (int64_t) db_ref;
      Dart_PostCObject(reply_port_id, &result);
      //        // It is OK that result is destroyed when function exits.
      //        // Dart_PostCObject has copied its data.
      return;
    }
  }

  DBRef *db_ref = NULL;
  if (msg > 1) {
    // All messages below have param2 as the pointer.
    Dart_CObject* param2 = message->value.as_array.values[2];
    if (param2->type == Dart_CObject_kInt64) {
      db_ref = (DBRef*) param2->value.as_int64;
    }
  }

  int32_t error = 0;
  if (db_ref != NULL) {
    if (db_ref->is_finalized) {
      error = -1;
    } else {
      error = levelDBServiceHandler(reply_port_id, db_ref, msg, message);
    }
  }
  if (error < 0) {
    Dart_CObject result;
    result.type = Dart_CObject_kInt32;
    result.value.as_int32 = error;
    Dart_PostCObject(reply_port_id, &result);
  }
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


/**
 * Add a finalizer to the NativeDB class so we call close() if the user has not already done so.
 */
void dbInit(Dart_NativeArguments arguments) {
  Dart_EnterScope();

  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  int64_t value;
  Dart_GetNativeIntegerArgument(arguments, 1, &value);

  Dart_NewWeakPersistentHandle(arg0, (void*) value, 0 /* external_allocation_size */, NativeDBFinalizer);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void* IteratorWork(void *data) {
  IteratorRef* it_ref = (IteratorRef*) data;
  leveldb::Iterator* it = it_ref->iterator;

  // The first time around we do an initial seek.
  if (!it_ref->is_seek_done) {
    it_ref->is_seek_done = true;
    if (it_ref->gt_len > 0) {
      leveldb::Slice start_slice = leveldb::Slice((char*)it_ref->gt, it_ref->gt_len);
      it->Seek(start_slice);

      if (!it_ref->is_gt_closed && it->Valid()) {
        // If we are pointing at start_slice and not inclusive then we need to advance by 1
        leveldb::Slice key = it->key();
        if (key.compare(start_slice) == 0) {
          it->Next();
        }
      }
    } else {
      it->SeekToFirst();
    }
  }

  leveldb::Slice end_slice = leveldb::Slice((char*)it_ref->lt, it_ref->lt_len);

  // When the thread leaves this loop it_ref->is_paused MUST be true and it_ref->thread MUST be 0. These changes are
  // either made by this thread holding the lock or by another thread holding the lock.
  RowRequest *row_request = NULL;
  while (true) {

    // If we have emitted max_size rows then send the done message and destroy the pop the request from the queue.
    if (row_request != NULL && row_request->max_size == 0) {
      Dart_CObject eos;
      eos.type = Dart_CObject_kInt32;
      eos.value.as_int32 = 1;
      Dart_PostCObject(row_request->reply_port_id, &eos);

      it_ref->mtx.lock();
      it_ref->request_queue->pop(); // Removes the 1st item.
      it_ref->mtx.unlock();

      delete row_request;
      row_request = NULL;
    }

    it_ref->mtx.lock();

    // We have been paused by another thread. Leave the loop.
    bool is_paused = it_ref->is_paused;

    // Peek at the next row request if NULL. If there is no next request then send the empty signal and pause the thread.
    if (row_request == NULL) {
      if (it_ref->request_queue->empty()) {
        is_paused = true;

        it_ref->thread = 0;
        it_ref->is_paused = true;


      } else {
        row_request = it_ref->request_queue->front();
      }
    }
    it_ref->mtx.unlock();

    if (is_paused) {
      break;
    }

    bool is_valid = it->Valid();
    bool is_limit_reached = it_ref->limit >= 0 && it_ref->count >= it_ref->limit;
    bool is_query_limit_reached = false;
    leveldb::Slice key;
    leveldb::Slice value;

    if (is_valid) {
      key = it->key();
      value = it->value();

      // Check if key is equal to end slice
      if (it_ref->lt_len > 0) {
        int cmp = key.compare(end_slice);
        if (cmp == 0 && !it_ref->is_lt_closed) {  // key == end_slice and not closed
          is_query_limit_reached = true;
        }
        if (cmp > 0) { // key > end_slice
          is_query_limit_reached = true;
        }
      }
    }

    // If the iterator has moved past the end or the limit reached  or the query parameters reached
    // then send the finished message and shut down.
    if (!is_valid || is_limit_reached || is_query_limit_reached) {

      // Send the close message.
      Dart_CObject eos;
      eos.type = Dart_CObject_kInt32;
      eos.value.as_int32 = 0;
      Dart_PostCObject(row_request->reply_port_id, &eos);

      it_ref->mtx.lock();
      if (!it_ref->is_paused) {
        it_ref->thread = 0;
        it_ref->is_paused = true;
      }
      it_ref->mtx.unlock();
      break;
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
    Dart_PostCObject(row_request->reply_port_id, &result);
    row_request->max_size -= 1;

    it_ref->count += 1;
    it->Next();
  }
}


/**
 * Starts the worker thread up (if it has not already started)
 */
static void iteratorStartWork(IteratorRef *it_ref) {
  it_ref->mtx.lock();
  if (it_ref->is_paused && !it_ref->is_finalized) {
    it_ref->is_paused = false;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    int rc = pthread_create(&it_ref->thread, &attr, IteratorWork, (void*)it_ref);
    assert(rc == 0);

    pthread_attr_destroy(&attr);
  }
  it_ref->mtx.unlock();
}


void iteratorNew(Dart_NativeArguments arguments) {  // (this, db, limit, fillCache, gt, is_gt_closed, lt, is_lt_closed)
  Dart_EnterScope();

  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  int64_t value;
  Dart_GetNativeIntegerArgument(arguments, 1, &value);
  DBRef* db_ref = (DBRef *) value;

  db_ref->mtx.lock();
  if (db_ref->is_finalized) {
    Dart_SetReturnValue(arguments, Dart_NewInteger(-1));
    Dart_ExitScope();
    db_ref->mtx.unlock();
    return;
  }

  IteratorRef* it_ref = new IteratorRef();
  it_ref->db_ref = db_ref;
  it_ref->is_paused = true;
  it_ref->thread = 0;
  it_ref->is_seek_done = false;
  it_ref->count = 0;
  it_ref->is_finalized = false;
  it_ref->request_queue = new std::queue<RowRequest*>();

  leveldb::ReadOptions options;
  Dart_GetNativeBooleanArgument(arguments, 3, &options.fill_cache);
  it_ref->iterator = db_ref->db->NewIterator(options);

  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_SetNativeInstanceField(arg0, 0, (intptr_t) it_ref);

  Dart_GetNativeIntegerArgument(arguments, 2, &it_ref->limit);

  Dart_Handle arg5 = Dart_GetNativeArgument(arguments, 4);
  if (Dart_IsNull(arg5)) {
    it_ref->gt = NULL;
    it_ref->gt_len = 0;
  } else if (Dart_IsString(arg5)) {
    uint8_t* s;
    Dart_StringToUTF8(arg5, &s, &it_ref->gt_len);
    it_ref->gt = (uint8_t*) malloc(it_ref->gt_len);
    memcpy(it_ref->gt, s, it_ref->gt_len);
  } else {
    assert(false); // Not reached
  }

  Dart_Handle arg7 = Dart_GetNativeArgument(arguments, 6);
  if (Dart_IsNull(arg7)) {
    it_ref->lt = NULL;
    it_ref->lt_len = 0;
  } else if (Dart_IsString(arg7)) {
    uint8_t* s;
    Dart_StringToUTF8(arg7, &s, &it_ref->lt_len);
    it_ref->lt = (uint8_t*) malloc(it_ref->lt_len);
    memcpy(it_ref->lt, s, it_ref->lt_len);
  } else {
    assert(false); // Not reached
  }

  Dart_GetNativeBooleanArgument(arguments, 5, &it_ref->is_gt_closed);
  Dart_GetNativeBooleanArgument(arguments, 7, &it_ref->is_lt_closed);

  Dart_NewWeakPersistentHandle(arg0, (void*) it_ref, 0 /* external_allocation_size */, NativeIteratorFinalizer);

  // Add to iterators list
  db_ref->iterators->push_back(it_ref);

  db_ref->mtx.unlock();

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void iteratorGetRows(Dart_NativeArguments arguments) {
  Dart_EnterScope();

  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  IteratorRef* it_ref;
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &it_ref);

  Dart_Port reply_port_id;
  Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 1);
  Dart_SendPortGetId(arg2, &reply_port_id);

  it_ref->mtx.lock();
  if (!it_ref->is_finalized) {
    RowRequest *row_request = new RowRequest();
    row_request->reply_port_id = reply_port_id;
    Dart_GetNativeIntegerArgument(arguments, 2, &row_request->max_size);

    it_ref->request_queue->push(row_request);
  } else {
    // If we are finalized then respond that the db is closed
    Dart_CObject result;
    result.type = Dart_CObject_kInt32;
    result.value.as_int32 = -1;
    Dart_PostCObject(reply_port_id, &result);
  }
  it_ref->mtx.unlock();

  iteratorStartWork(it_ref);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


struct FunctionLookup {
  const char* name;
  Dart_NativeFunction function;
};


FunctionLookup function_list[] = {
    {"DB_Init", dbInit},
    {"DB_ServicePort", dbServicePort},

    {"Iterator_New", iteratorNew},
    {"Iterator_GetRows", iteratorGetRows},

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
