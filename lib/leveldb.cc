
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#include <list>
#include <deque>
#include <string>
#include <map>
#include <cstring>

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


int64_t statusToError(leveldb::Status status) {
    if (status.IsNotFound()) {
        return -5;
    }
    if (status.IsIOError()) {
        return -2;
    }
    if (status.IsCorruption()) {
        return -3;
    }
    // LevelDB does not provide Status::IsInvalidArgument so we just assume all other errors are invalid argument.
    if (!status.ok()) {
        return -4;
    }
    return 0;
}


struct DB {
  leveldb::DB *db;
  int64_t refcount;

  bool is_shared;
  char* path;
  int64_t block_size;
  bool create_if_missing;
  bool error_if_exists;

  pthread_t thread;
  std::deque<Dart_Port> notify_list;
  int64_t open_status;
  pthread_mutex_t mutex;
};


struct cmp_str {
   bool operator()(char const *a, char const *b) {
      return std::strcmp(a, b) < 0;
   }
};


typedef std::map<char const*, DB*, cmp_str> DBMap;
pthread_mutex_t shared_mutex = PTHREAD_MUTEX_INITIALIZER;
DBMap sharedDBs;


void* runOpen(void* ptr) {
    // This function may not take the shared mutex because we take it when joining to this thread.
    DB *native_db = (DB*) ptr;
    leveldb::Options options;
    options.create_if_missing = native_db->create_if_missing;
    options.error_if_exists = native_db->error_if_exists;
    options.block_size = native_db->block_size;
    options.filter_policy = leveldb::NewBloomFilterPolicy(BLOOM_BITS_PER_KEY);

    leveldb::Status status = leveldb::DB::Open(options, native_db->path, &native_db->db);

    // Notify all ports the new status.
    pthread_mutex_lock(&native_db->mutex);
    native_db->open_status = statusToError(status);

    while (!native_db->notify_list.empty()) {
        Dart_Port port = native_db->notify_list.front();
        native_db->notify_list.pop_front();
        Dart_PostInteger(port, native_db->open_status);
    }
    pthread_mutex_unlock(&native_db->mutex);
    return NULL;
}

/// Open a db and take a reference to it.
/// open_port_id will be notified when the db is ready or an error occurs.
DB* referenceDB(const char *path, bool is_shared, Dart_Port open_port_id, bool create_if_missing, bool error_if_exists, int64_t block_size) {
    DB* db = NULL;
    bool is_new = false;

    pthread_mutex_lock(&shared_mutex);

    // Look for the db by path
    if (is_shared) {
        DBMap::iterator it = sharedDBs.find(path);
        if (it != sharedDBs.end()) {
            db = it->second;
            assert(db->refcount > 0);
        }
    }

    // Create db if not found
    if (db == NULL) {
        is_new = true;
        db = new DB();
        db->is_shared = is_shared;
        db->path = strdup(path);
        db->refcount = 0;
        db->open_status = 1;
        db->create_if_missing = create_if_missing;
        db->error_if_exists = error_if_exists;
        db->block_size = block_size;
        pthread_mutex_init(&db->mutex, NULL);
    }

    // If the db is shared add it to the map
    if (is_shared) {
        sharedDBs[db->path] = db;
    }

    // If the db is open then just post a reply now. Otherwise add the port to the notify list.
    pthread_mutex_lock(&db->mutex);
    db->refcount += 1;
    if (db->open_status <= 0) {
        // The open thread has finished.
        Dart_PostInteger(open_port_id, db->open_status);
    } else {
        db->notify_list.push_back(open_port_id);
    }
    pthread_mutex_unlock(&db->mutex);
    pthread_mutex_unlock(&shared_mutex);

    // Spawn a thread to open the DB
    if (is_new) {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        int rc = pthread_create(&db->thread, &attr, runOpen, (void*)db);
        assert(rc == 0);
        pthread_attr_destroy(&attr);
    }
    return db;
}


/// Drop a reference to a db.
/// May result in the db being closed.
void unreferenceDB(DB* db) {
    bool is_finished;
    // Take the shared mutex and the db mutex. This is so that if the refcount drops to 0 we can safely remove it
    // from the shared map.
    pthread_mutex_lock(&shared_mutex);
    pthread_mutex_lock(&db->mutex);
    db->refcount -= 1;
    is_finished = db->refcount == 0;

    // If the db is shared then remove it from the map.
    if (is_finished && db->is_shared) {
        sharedDBs.erase(db->path);
    }

    pthread_mutex_unlock(&db->mutex);

    if (is_finished) {
        // It is possible that unreferenceDB is called before db->thread is initialized if a 2nd thread quickly takes a reference to a
        // shared db and then drops it. However the initializing thread still has a reference so it is safe to call pthread_join()
        // if the refcount was 0
        pthread_join(db->thread, NULL);

        // The actual closing of the db and its file descriptors must be run whilst
        // the shared lock is taken so that any threads attempting to open the same file will
        // succeed.
        delete db->path;
        delete db->db;
        delete db;
    }

    pthread_mutex_unlock(&shared_mutex);
}


struct NativeIterator;


struct NativeDB {
    // Reference to the DB. NULL if closed.
    DB* db;
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
    return;
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
 * Finalizer called when the dart LevelDB instance is not reachable.
 * */
static void NativeDBFinalizer(void* isolate_callback_data, Dart_WeakPersistentHandle handle, void* peer) {
    NativeDB* native_db = (NativeDB*) peer;

    // If the db reference is not NULL then the user did not call close on the db before it went out of scope.
    // We unreference it now.
    if (native_db->db != NULL) {
        unreferenceDB(native_db->db);
        native_db->db = NULL;
    }

    // Finalize every iterator. The iterators remove themselves from the array.
    while (!native_db->iterators->empty()) {
        iteratorFinalize(native_db->iterators->front());
    }
    delete native_db->iterators;

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


void dbOpen(Dart_NativeArguments arguments) {  // (bool shared, SendPort port, String path, int blockSize, bool create_if_missing, bool error_if_exists)
    Dart_EnterScope();

    NativeDB* native_db = new NativeDB();

    const char* path;
    Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 3);
    Dart_StringToCString(arg2, &path);

    Dart_Port port_id;
    Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 2);
    Dart_SendPortGetId(arg1, &port_id);

    bool is_shared;
    bool create_if_missing;
    bool error_if_exists;
    int64_t block_size;

    Dart_GetNativeBooleanArgument(arguments, 1, &is_shared);
    Dart_GetNativeIntegerArgument(arguments, 4, &block_size);
    Dart_GetNativeBooleanArgument(arguments, 5, &create_if_missing);
    Dart_GetNativeBooleanArgument(arguments, 6, &error_if_exists);

    native_db->db = referenceDB(path, is_shared, port_id, create_if_missing, error_if_exists, 1024);
    native_db->iterators = new std::list<NativeIterator*>();

    Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
    Dart_SetNativeInstanceField(arg0, 0, (intptr_t) native_db);

    Dart_NewWeakPersistentHandle(arg0, (void*) native_db, sizeof(NativeDB) /* external_allocation_size */, NativeDBFinalizer);

    Dart_SetReturnValue(arguments, Dart_Null());
    Dart_ExitScope();
}


// SYNC API


// Throw a LevelClosedError. This function does not return.
void throwClosedException() {
  Dart_Handle klass = Dart_GetType(Dart_LookupLibrary(Dart_NewStringFromCString("package:leveldb/leveldb.dart")), Dart_NewStringFromCString("LevelClosedError"), 0, NULL);
  Dart_Handle exception = Dart_New(klass, Dart_NewStringFromCString("_internal"), 0, NULL);
  Dart_ThrowException(exception);
}


// If status is not ok then throw an error. This function does not return.
void maybeThrowStatus(leveldb::Status status) {
  if (status.ok()) {
    return;
  }
  Dart_Handle library = Dart_LookupLibrary(Dart_NewStringFromCString("package:leveldb/leveldb.dart"));
  Dart_Handle klass;
  if (status.IsCorruption()) {
    klass = Dart_GetType(library, Dart_NewStringFromCString("LevelCorruptionError"), 0, NULL);
  } else {
    klass = Dart_GetType(library, Dart_NewStringFromCString("LevelIOError"), 0, NULL);
  }
  Dart_Handle exception = Dart_New(klass, Dart_NewStringFromCString("_internal"), 0, NULL);
  Dart_ThrowException(exception);
}


void syncNew(Dart_NativeArguments arguments) {  // (this, db, limit, fillCache, gt, is_gt_closed, lt, is_lt_closed)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_GetNativeInstanceField(arg1, 0, (intptr_t*) &native_db);

  if (native_db->db == NULL) {
    throwClosedException();
    assert(false); // Not reached
  }

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


// http://stackoverflow.com/questions/2022179/c-quick-calculation-of-next-multiple-of-4
uint32_t increaseToMultipleOf4(uint32_t v) {
  return (v + 3) & ~0x03;
}


void syncNext(Dart_NativeArguments arguments) {  // (this)
  Dart_EnterScope();

  NativeIterator *native_iterator;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_iterator);

  NativeDB *native_db = native_iterator->native_db;
  leveldb::Iterator* it = native_iterator->iterator;

  if (native_db->db == NULL) {
    throwClosedException();
    assert(false); // Not reached
  }

  // If it is NULL we need to create the iterator and perform the initial seek.
  if (!native_iterator->is_finalized && it == NULL) {
    leveldb::ReadOptions options;
    options.fill_cache = native_iterator->is_fill_cache;
    it = native_db->db->db->NewIterator(options);

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
  bool is_valid = false;
  bool is_limit_reached = native_iterator->limit >= 0 && native_iterator->count >= native_iterator->limit;
  bool is_query_limit_reached = false;

  leveldb::Slice key;
  leveldb::Slice value;
  if (!native_iterator->is_finalized) {
    is_valid = it->Valid();
  }

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

  Dart_Handle result = Dart_Null();

  if (!is_valid || is_query_limit_reached || is_limit_reached) {
    // Iteration is finished. Any subsequent calls to syncNext() will return null so we can finalize the iterator
    // here.
    iteratorFinalize(native_iterator);
  } else {
    // Copy key and value into same buffer.
    // Align the value array to a multiple of 4 bytes so the offset of the view in dart is a multiple of 4.
    uint32_t key_size_mult_4 = increaseToMultipleOf4(key.size());
    result = Dart_NewTypedData(Dart_TypedData_kUint8, key_size_mult_4 + value.size() + 4);
    uint8_t *data;
    intptr_t len;
    Dart_TypedData_Type t;
    Dart_TypedDataAcquireData(result, &t, (void**)&data, &len);
    data[0] = key.size() & 0xFF;
    data[1] = (key.size() >> 8) & 0xFF;
    data[2] = key_size_mult_4 & 0xFF;
    data[3] = (key_size_mult_4 >> 8) & 0xFF;
    memcpy(data + 4, key.data(), key.size());
    memcpy(data + 4 + key_size_mult_4, value.data(), value.size());
    Dart_TypedDataReleaseData(result);

    native_iterator->count += 1;
    it->Next();
  }

  Dart_SetReturnValue(arguments, result);
  Dart_ExitScope();
}


void syncGet(Dart_NativeArguments arguments) {  // (this, key)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  if (native_db->db == NULL) {
    throwClosedException();
    assert(false); // Not reached
  }

  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_TypedData_Type typed_data_type = Dart_GetTypeOfTypedData(arg1);
  assert(typed_data_type == Dart_TypedData_kUint8);

  char *data;
  intptr_t len;
  Dart_TypedDataAcquireData(arg1, &typed_data_type, (void**)&data, &len);

  leveldb::Slice key = leveldb::Slice(data, len);

  std::string value;
  leveldb::Status status = native_db->db->db->Get(leveldb::ReadOptions(), key, &value);
  Dart_TypedDataReleaseData(arg1);

  Dart_Handle result;
  if (status.IsNotFound()) {
    result = Dart_Null();
  } else if (status.ok()) {
    result = Dart_NewTypedData(Dart_TypedData_kUint8, value.size());
    Dart_TypedData_Type t;
    Dart_TypedDataAcquireData(result, &t, (void**)&data, &len);
    memcpy(data, value.data(), value.size());
    Dart_TypedDataReleaseData(result);
  } else {
    maybeThrowStatus(status);
    assert(false); // Not reached
  }

  Dart_SetReturnValue(arguments, result);
  Dart_ExitScope();
}


void syncPut(Dart_NativeArguments arguments) {  // (this, key, value, sync)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  if (native_db->db == NULL) {
    throwClosedException();
    assert(false); // Not reached
  }

  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_TypedData_Type typed_data_type1;

  Dart_Handle arg2 = Dart_GetNativeArgument(arguments, 2);
  Dart_TypedData_Type typed_data_type2;

  bool is_sync;
  Dart_GetNativeBooleanArgument(arguments, 3, &is_sync);

  char *data1, *data2;
  intptr_t len1, len2;
  Dart_TypedDataAcquireData(arg1, &typed_data_type1, (void**)&data1, &len1);
  Dart_TypedDataAcquireData(arg2, &typed_data_type2, (void**)&data2, &len2);

  assert(typed_data_type1 == Dart_TypedData_kUint8);
  assert(typed_data_type2 == Dart_TypedData_kUint8);

  leveldb::Slice key = leveldb::Slice(data1, len1);
  leveldb::Slice value = leveldb::Slice(data2, len2);

  leveldb::WriteOptions options;
  options.sync = is_sync;

  leveldb::Status status = native_db->db->db->Put(options, key, value);
  
  Dart_TypedDataReleaseData(arg1);
  Dart_TypedDataReleaseData(arg2);
  
  maybeThrowStatus(status);

  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void syncDelete(Dart_NativeArguments arguments) {  // (this, key)
  Dart_EnterScope();

  NativeDB *native_db;
  Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
  Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

  if (native_db->db == NULL) {
    throwClosedException();
    assert(false); // Not reached
  }

  Dart_Handle arg1 = Dart_GetNativeArgument(arguments, 1);
  Dart_TypedData_Type typed_data_type = Dart_GetTypeOfTypedData(arg1);
  assert(typed_data_type == Dart_TypedData_kUint8);

  char *data;
  intptr_t len;
  Dart_TypedDataAcquireData(arg1, &typed_data_type, (void**)&data, &len);

  leveldb::Slice key = leveldb::Slice(data, len);
  leveldb::Status status = native_db->db->db->Delete(leveldb::WriteOptions(), key);
  Dart_TypedDataReleaseData(arg1);

  maybeThrowStatus(status);
  
  Dart_SetReturnValue(arguments, Dart_Null());
  Dart_ExitScope();
}


void syncClose(Dart_NativeArguments arguments) {  // (this)
    Dart_EnterScope();

    NativeDB *native_db;
    Dart_Handle arg0 = Dart_GetNativeArgument(arguments, 0);
    Dart_GetNativeInstanceField(arg0, 0, (intptr_t*) &native_db);

    if (native_db->db == NULL) {
        // DB has already been closed
        throwClosedException();
        assert(false); // Not reached
    }

    // Finalize all iterators
    while (!native_db->iterators->empty()) {
        iteratorFinalize(native_db->iterators->front());
    }

    unreferenceDB(native_db->db);
    native_db->db = NULL;

    Dart_SetReturnValue(arguments, Dart_Null());
    Dart_ExitScope();
}


// Plugin

struct FunctionLookup {
  const char* name;
  Dart_NativeFunction function;
};


FunctionLookup function_list[] = {
    {"DB_Open", dbOpen},

    {"SyncIterator_New", syncNew},
    {"SyncIterator_Next", syncNext},

    {"SyncGet", syncGet},
    {"SyncPut", syncPut},
    {"SyncDelete", syncDelete},
    {"SyncClose", syncClose},

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
