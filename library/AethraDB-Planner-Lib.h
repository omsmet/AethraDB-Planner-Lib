#ifndef __AETHRADB_PLANNER_LIB_H
#define __AETHRADB_PLANNER_LIB_H

#include <graal_isolate.h>


#if defined(__cplusplus)
extern "C" {
#endif

graal_isolatethread_t* Java_AethraDB_AethraDB_createIsolate();

void * Java_AethraDB_AethraDB_plan(struct JNIEnv_*, size_t, graal_isolatethread_t*, void *, void *);

#if defined(__cplusplus)
}
#endif
#endif
