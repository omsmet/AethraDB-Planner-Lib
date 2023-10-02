#ifndef __AETHRADB_PLANNER_LIB_H
#define __AETHRADB_PLANNER_LIB_H

#include <graal_isolate_dynamic.h>


#if defined(__cplusplus)
extern "C" {
#endif

typedef graal_isolatethread_t* (*Java_AethraDB_util_AethraDatabase_createIsolate_fn_t)();

typedef void * (*Java_AethraDB_util_AethraDatabase_plan_fn_t)(struct JNIEnv_*, size_t, graal_isolatethread_t*, void *, void *);

#if defined(__cplusplus)
}
#endif
#endif
