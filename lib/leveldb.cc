
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#include <queue>
#include <list>
#include <string>

#include "include/dart_api.h"
#include "include/dart_native_api.h"

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"

const int BLOOM_BITS_PER_KEY = 10;

// The max number of rows an iterator will send in response to a GetRows() message.
const int MAX_ROWS_PER_MESSAGE = 50000;

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


struct NativeIterator;


struct Message {
  Dart_Port port_id;
  int cmd;

  int key_len;
  char* key;

  int value_len;
  char* value;

  bool sync;
  NativeIterator *iterator;
};


struct NativeDB {
  leveldb::DB *db;

  char* path;
  int64_t block_size;
  bool create_if_missing;
  bool error_if_exists;

  pthread_mutex_t mtx;
  pthread_t thread;
  pthread_cond_t cond;
  std::queue<Message*> *messages;

  bool is_closed;
  bool is_finalized;

  std::list<NativeIterator*> *iterators;
};


struct NativeIterator {
  NativeDB *native_db;

  leveldb::Iterator *iterator;
  bool is_finalized;

  // Iterator params
  int64_t limit;
  bool is_gt_closed;
  bool is_lt_closed;
  uint8_t* gt;
  int64_t gt_len;
  uint8_t* lt;
  int64_t lt_len;
  bool is_fill_cache;

  // Iterator state
  int64_t count;
};


/**
 * Finalize the iterator.
 */
static void iteratorFinalize(NativeIterator *it_ref) {
  if (it_ref->is_finalized) {
    return; // FIXME: LOCKING?
  }
  it_ref->is_finalized = true;

  // This iterator will only be in the db list if the level db iterator has been created (i.e. the stream has
  // started).
  if (it_ref->iterator != NULL) {
    // Remove the iterator from the db list
    it_ref->native_db->iterators->remove(it_ref);
    delete it_ref->iterator;
    it_ref->iterator = NULL;
  }

  delete it_ref->gt;
  delete it_ref->lt;
}


/**
 * Stop all iterators, close the db.
 * Thread safe.
 */
static void finalizeDB(NativeDB *native_db) {
  if (native_db->is_finalized) {  // FIXME: locking?
    return;
  }
  native_db->is_finalized = true;

  delete native_db->path;

  // Finalize every iterator. The iterators remove themselves from the array.
  while (!native_db->iterators->empty()) {
    iteratorFinalize(native_db->iterators->front());
  }
  delete native_db->iterators;

  // Close the db
  delete native_db->db;
}


/**
 * Finalizer called when the dart LevelDB instance is not reachable.
 * */
static void NativeDBFinalizer(void* isolate_callback_data, Dart_WeakPersistentHandle handle, void* peer) {
  NativeDB* native_db = (NativeDB*) peer;
  finalizeDB(native_db);
  delete native_db;
}


Dart_Handle HandleError(Dart_Handle handle) {
  if (Dart_IsError(handle)) {
    Dart_PropagateError(handle);
  }
  return handle;
}


/**
 * Finalizer called when the dart instance is not reachable.
 * */
static void NativeIteratorFinalizer(void* isolate_callback_data, Dart_WeakPersistentHandle handle, void* peer) {
  NativeIterator* it_ref = (NativeIterator*) peer;
  iteratorFinalize(it_ref);
  delete it_ref;
}


void iteratorNew(Dart_NativeArguments arguments) {  // (this, db, limit, fillCache, gt, is_gt_closed, lt, is_lt_closed)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_GetNativeInstanceField(arg1, 0, (intptr_t*) &native_db);

  NativeIterator* it_ref = new NativeIterator();
  it_ref->native_db = native_db;
  it_ref->is_finalized = false;
  it_ref->iterator = NULL;
  it_ref->count = 0;

  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_SetNativeInstanceField(arg0, 0, (intptr_t) it_ref);

  Dart_GetNativeIntegerArgument(arguments, 2, &it_ref->limit);
  Dart_GetNativeBooleanArgument(arguments, 3, &it_ref->is_fill_cache);

  Dart_Handle arg5 = Dart_GetNativeArgument(arguments, 4);
  if (Dart_IsNull(arg5)) {
    it_ref->gt = NULL;
    it_ref->gt_len = 0;
  } else {
    Dart_TypedData_Type typed_data_type = Dart_GetTypeOfTypedData(arg5);
    assert(typed_data_type == Dart_TypedData_kUint8);

    char *data;
    intptr_t len;
    Dart_TypedDataAcquireData(arg5, &typed_data_type, (void**)&data, &len);
    it_ref->gt_len = len;
    it_ref->gt = (uint8_t*) malloc(len);
    memcpy(it_ref->gt, data, len);
    Dart_TypedDataReleaseData(arg5);
  }

  Dart_Handle arg6 = Dart_GetNativeArgument(arguments, 6);
  if (Dart_IsNull(arg6)) {
    it_ref->lt = NULL;
    it_ref->lt_len = 0;
  } else {
    Dart_TypedData_Type typed_data_type = Dart_GetTypeOfTypedData(arg6);
    assert(typed_data_type != Dart_TypedData_kInvalid);

    char *data;
    intptr_t len;
    Dart_TypedDataAcquireData(arg6, &typed_data_type, (void**)&data, &len);
    it_ref->lt_len = len;
    it_ref->lt = (uint8_t*) malloc(len);
    memcpy(it_ref->lt, data, len);
    Dart_TypedDataReleaseData(arg6);
  }

  Dart_GetNativeBooleanArgument(arguments, 5, &it_ref->is_gt_closed);
  Dart_GetNativeBooleanArgument(arguments, 7, &it_ref->is_lt_closed);

  // We just pass the directly allocated size of the iterator here. The iterator holds a lot of other data in
  // memory when it mmaps the files but I'm not sure how to account for it.
  // Because the GC is not seeing all of the allocated memory it is important to manually call finalize() on the
  // iterator when we are done with it (for example when the iterator reaches the end of its range).
  Dart_NewWeakPersistentHandle(arg0, (void*) it_ref, /* external_allocation_size */ sizeof(NativeIterator), NativeIteratorFinalizer);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


const int MESSAGE_OPEN = 0;
const int MESSAGE_PUT = 1;
const int MESSAGE_GET = 2;
const int MESSAGE_DELETE = 3;
const int MESSAGE_GET_ROWS = 4;
const int MESSAGE_CLOSE = 5;


void freeMessage(Message*m) {
  delete m->key;
  delete m->value;

  delete m;
}


bool maybeSendError(Dart_Port port_id, leveldb::Status status) {
  if (status.IsNotFound()) {
    Dart_PostInteger(port_id, -5);
    return true;
  }
  if (status.IsIOError()) {
    Dart_PostInteger(port_id, -2);
    return true;
  }
  if (status.IsCorruption()) {
    Dart_PostInteger(port_id, -3);
    return true;
  }
  // LevelDB does not provide Status::IsInvalidArgument so we just assume all other errors are invalid argument.
  if (!status.ok()) {
    Dart_PostInteger(port_id, -4);
    return true;
  }
  return false;
}


void processMessageOpen(NativeDB *native_db, Message *m) {
  leveldb::Options options;
  options.create_if_missing = native_db->create_if_missing;
  options.error_if_exists = native_db->error_if_exists;
  options.block_size = native_db->block_size;
  options.filter_policy = leveldb::NewBloomFilterPolicy(BLOOM_BITS_PER_KEY);

  leveldb::Status status = leveldb::DB::Open(options, native_db->path, &native_db->db);
  if (maybeSendError(m->port_id, status)) {
    return;
  }
  Dart_PostInteger(m->port_id, 0);
}


void processMessagePut(NativeDB *native_db, Message *m) {
  leveldb::WriteOptions options;
  options.sync = m->sync;

  leveldb::Slice key = leveldb::Slice(m->key, m->key_len);
  leveldb::Slice value = leveldb::Slice(m->value, m->value_len);
  leveldb::Status status = native_db->db->Put(options, key, value);
  if (maybeSendError(m->port_id, status)) {
    return;
  }
  Dart_PostInteger(m->port_id, 0);
}


void processMessageGet(NativeDB *native_db, Message *m) {
  leveldb::Slice key = leveldb::Slice(m->key, m->key_len);
  std::string value;
  leveldb::Status status = native_db->db->Get(leveldb::ReadOptions(), key, &value);
  if (maybeSendError(m->port_id, status)) {
    return;
  }

  Dart_CObject result;
  result.type = Dart_CObject_kTypedData;
  result.value.as_typed_data.type = Dart_TypedData_kUint8;
  // It is OK not to copy the slice data because Dart_PostCObject has copied its data.
  result.value.as_typed_data.values = (uint8_t*) value.data();
  result.value.as_typed_data.length = value.size();
  Dart_PostCObject(m->port_id, &result);
}


void processMessageDelete(NativeDB *native_db, Message *m) {
  leveldb::Slice key = leveldb::Slice(m->key, m->key_len);
  leveldb::Status status = native_db->db->Delete(leveldb::WriteOptions(), key);
  if (maybeSendError(m->port_id, status)) {
    return;
  }
  Dart_PostInteger(m->port_id, 0);
}


void processMessageGetRows(NativeDB *native_db, Message *m) {
  NativeIterator *native_iterator = m->iterator;

  leveldb::Iterator* it = native_iterator->iterator;

  // If it is NULL we need to create the iterator and perform the initial seek.
  if (it == NULL) {
    leveldb::ReadOptions options;
    options.fill_cache = native_iterator->is_fill_cache;
    it = native_db->db->NewIterator(options);

    native_iterator->iterator = it;
    // Add the iterator to the db list. This is so we know to finalize it before finalizing the db.
    native_db->iterators->push_back(native_iterator);

    if (native_iterator->gt_len > 0) {
      leveldb::Slice start_slice = leveldb::Slice((char*)native_iterator->gt, native_iterator->gt_len);
      it->Seek(start_slice);

      if (!native_iterator->is_gt_closed && it->Valid()) {
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

  leveldb::Slice end_slice = leveldb::Slice((char*)native_iterator->lt, native_iterator->lt_len);

  int count = 0;
  bool is_finished = false;
  while (!native_iterator->is_finalized && count < MAX_ROWS_PER_MESSAGE) {
    bool is_valid = it->Valid();
    bool is_limit_reached = native_iterator->limit >= 0 && native_iterator->count >= native_iterator->limit;
    bool is_query_limit_reached = false;
    leveldb::Slice key;
    leveldb::Slice value;

      if (is_valid) {
        key = it->key();
        value = it->value();

        // Check if key is equal to end slice
        if (native_iterator->lt_len > 0) {
          int cmp = key.compare(end_slice);
          if (cmp == 0 && !native_iterator->is_lt_closed) {  // key == end_slice and not closed
            is_query_limit_reached = true;
          }
          if (cmp > 0) { // key > end_slice
            is_query_limit_reached = true;
          }
        }
      }

      if (!is_valid || is_query_limit_reached || is_limit_reached) {
        is_finished = true;
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
      Dart_PostCObject(m->port_id, &result);

      native_iterator->count += 1;
      it->Next();
      count += 1;
  }

  if (is_finished) {
    Dart_PostInteger(m->port_id, 0);
    // The iterator has reached the end of its rows. We can finalize it now.
    iteratorFinalize(native_iterator);
  } else {
    // Iterator is not finished but we have sent a lot of rows. The dart side will finish processing the rows then
    // ask for some more. This means we set a maximum on the number of rows buffered in the dart stream and we
    // properly pause the iterator if requested.
    Dart_PostInteger(m->port_id, 1);
  }
}


void processMessage(NativeDB *native_db, Message *m) {
  switch (m->cmd) {
    case MESSAGE_OPEN:
      processMessageOpen(native_db, m);
      break;
    case MESSAGE_PUT:
      processMessagePut(native_db, m);
      break;
    case MESSAGE_GET:
      processMessageGet(native_db, m);
      break;
    case MESSAGE_DELETE:
      processMessageDelete(native_db, m);
      break;
    case MESSAGE_GET_ROWS:
      processMessageGetRows(native_db, m);
      break;
    default:
      assert(false);
  }
}


void* processMessages(void* ptr) {
  NativeDB *native_db = (NativeDB*) ptr;
  Message* m;

  while (true) {
    pthread_mutex_lock(&native_db->mtx);
    while (native_db->messages->empty()) {
      pthread_cond_wait(&native_db->cond, &native_db->mtx);
    }
    m = native_db->messages->front();
    native_db->messages->pop();
    pthread_mutex_unlock(&native_db->mtx);

    if (m->cmd == MESSAGE_CLOSE) {
      break;
    }
    processMessage(native_db, m);
    freeMessage(m);
  }

  // Finalize. This will finalize all iterators and then the db.
  finalizeDB(native_db);

  // Respond to the close message
  Dart_PostInteger(m->port_id, 0);
  freeMessage(m);

  return NULL;
}


void dbAddMessage(NativeDB* native_db, Message *m) {
  pthread_mutex_lock(&native_db->mtx);
  native_db->messages->push(m);
  pthread_cond_signal(&native_db->cond);
  pthread_mutex_unlock(&native_db->mtx);
}


void dbOpen(Dart_NativeArguments arguments) {  // (SendPort port, String path, int blockSize, bool create_if_missing, bool error_if_exists)
  Dart_EnterScope();

  NativeDB* native_db = new NativeDB();

  native_db->is_closed = false;
  native_db->is_finalized = false;
  native_db->messages = new std::queue<Message*>();
  native_db->iterators = new std::list<NativeIterator*>();
  native_db->path = NULL;

  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_SetNativeInstanceField(arg0, 0, (intptr_t) native_db);

  Dart_Port port_id;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_SendPortGetId(arg1, &port_id);

  const char* path;
  Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 2);
  Dart_StringToCString(arg2, &path);
  native_db->path = strdup(path);

  Dart_GetNativeIntegerArgument(arguments, 3, &native_db->block_size);
  Dart_GetNativeBooleanArgument(arguments, 4, &native_db->create_if_missing);
  Dart_GetNativeBooleanArgument(arguments, 5, &native_db->error_if_exists);

  // Create the open message
  Message* m = new Message();
  m->port_id = port_id;
  m->cmd = MESSAGE_OPEN;
  m->key = NULL;
  m->value = NULL;
  dbAddMessage(native_db, m);

  // Start the db thread
  pthread_mutex_init(&native_db->mtx, NULL);
  pthread_cond_init(&native_db->cond, NULL);

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  int rc = pthread_create(&native_db->thread, &attr, processMessages, (void*)native_db);
  assert(rc == 0);

  pthread_attr_destroy(&attr);

  Dart_NewWeakPersistentHandle(arg0, (void*) native_db, sizeof(NativeDB) /* external_allocation_size */, NativeDBFinalizer);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void dbPut(Dart_NativeArguments arguments) {  // (SendPort port, Uint8List key, Uint8List value, bool sync)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  Dart_Port port_id;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_SendPortGetId(arg1, &port_id);

  if (native_db->is_closed) {
    Dart_PostInteger(port_id, -1);
    Dart_SetReturnValue(arguments, Dart_Null());
    Dart_ExitScope();
    return;
  }

  // Create the open message
  Message* m = new Message();
  m->port_id = port_id;
  m->cmd = MESSAGE_PUT;

  Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 2);
  Dart_TypedData_Type typed_data_type = Dart_GetTypeOfTypedData(arg2);
  assert(typed_data_type == Dart_TypedData_kUint8);
  char *data;
  intptr_t len;
  Dart_TypedDataAcquireData(arg2, &typed_data_type, (void**)&data, &len);
  m->key_len = len;
  m->key = (char*) malloc(len);
  memcpy(m->key, data, len);
  Dart_TypedDataReleaseData(arg2);

  Dart_Handle arg3 = Dart_GetNativeArgument(arguments, 3);
  typed_data_type = Dart_GetTypeOfTypedData(arg2);
  assert(typed_data_type == Dart_TypedData_kUint8);
  Dart_TypedDataAcquireData(arg3, &typed_data_type, (void**)&data, &len);
  m->value_len = len;
  m->value = (char*) malloc(len);
  memcpy(m->value, data, len);
  Dart_TypedDataReleaseData(arg3);

  Dart_GetNativeBooleanArgument(arguments, 4, &m->sync);
  dbAddMessage(native_db, m);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void dbGet(Dart_NativeArguments arguments) {  // (SendPort port, Uint8List key)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  Dart_Port port_id;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_SendPortGetId(arg1, &port_id);

  if (native_db->is_closed) {
    Dart_PostInteger(port_id, -1);
    Dart_SetReturnValue(arguments, Dart_Null());
    Dart_ExitScope();
    return;
  }

  // Create the open message
  Message* m = new Message();
  m->port_id = port_id;
  m->cmd = MESSAGE_GET;

  Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 2);
  Dart_TypedData_Type typed_data_type = Dart_GetTypeOfTypedData(arg2);
  assert(typed_data_type == Dart_TypedData_kUint8);
  char *data;
  intptr_t len;
  Dart_TypedDataAcquireData(arg2, &typed_data_type, (void**)&data, &len);
  m->key_len = len;
  m->key = (char*) malloc(len);
  memcpy(m->key, data, len);
  Dart_TypedDataReleaseData(arg2);

  m->value = NULL;
  dbAddMessage(native_db, m);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void dbDelete(Dart_NativeArguments arguments) {  // (SendPort port, Uint8List key)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  Dart_Port port_id;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_SendPortGetId(arg1, &port_id);

  if (native_db->is_closed) {
    Dart_PostInteger(port_id, -1);
    Dart_SetReturnValue(arguments, Dart_Null());
    Dart_ExitScope();
    return;
  }

  // Create the open message
  Message* m = new Message();
  m->port_id = port_id;
  m->cmd = MESSAGE_DELETE;

  Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 2);
  Dart_TypedData_Type typed_data_type = Dart_GetTypeOfTypedData(arg2);
  assert(typed_data_type == Dart_TypedData_kUint8);
  char *data;
  intptr_t len;
  Dart_TypedDataAcquireData(arg2, &typed_data_type, (void**)&data, &len);
  m->key_len = len;
  m->key = (char*) malloc(len);
  memcpy(m->key, data, len);
  Dart_TypedDataReleaseData(arg2);

  m->value = NULL;
  dbAddMessage(native_db, m);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void dbGetRows(Dart_NativeArguments arguments) {  // (SendPort port, Leveliterator iterator)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  Dart_Port port_id;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_SendPortGetId(arg1, &port_id);

  if (native_db->is_closed) {
    Dart_PostInteger(port_id, -1);
    Dart_SetReturnValue(arguments, Dart_Null());
    Dart_ExitScope();
    return;
  }

  NativeIterator *native_iterator;
  Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 2);
  Dart_GetNativeInstanceField(arg2, 0, (intptr_t*) &native_iterator);

  Message* m = new Message();
  m->port_id = port_id;
  m->cmd = MESSAGE_GET_ROWS;
  m->key = NULL;
  m->value = NULL;
  m->iterator = native_iterator;
  dbAddMessage(native_db, m);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void dbClose(Dart_NativeArguments arguments) {  // (SendPort port)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  Dart_Port port_id;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_SendPortGetId(arg1, &port_id);

  if (native_db->is_closed) {
    Dart_PostInteger(port_id, -1);
    Dart_SetReturnValue(arguments, Dart_Null());
    Dart_ExitScope();
    return;
  }

  native_db->is_closed = true;

  // Send the close message to the thread and join.
  Message* m = new Message();
  m->port_id = port_id;
  m->cmd = MESSAGE_CLOSE;
  m->key = NULL;
  m->value = NULL;
  dbAddMessage(native_db, m);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


struct FunctionLookup {
  const char* name;
  Dart_NativeFunction function;
};


FunctionLookup function_list[] = {
    {"DB_Open", dbOpen},
    {"DB_Put", dbPut},
    {"DB_Get", dbGet},
    {"DB_Delete", dbDelete},
    {"DB_GetRows", dbGetRows},
    {"DB_Close", dbClose},

    {"Iterator_New", iteratorNew},

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
