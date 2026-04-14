/* Copyright (c) 2026 Magalu Cloud.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef OVSDB_WORKER_POOL_H
#define OVSDB_WORKER_POOL_H 1

#include <stddef.h>
#include <stdbool.h>
#include "compiler.h"

struct ovsdb_worker_pool;

/* Function signatures for worker jobs.
 *
 * 'ovsdb_worker_fn' runs in a worker thread.  It receives 'arg' and
 * returns a result pointer.
 *
 * 'ovsdb_worker_done_fn' runs on the main thread during
 * ovsdb_worker_pool_run().  It receives the result from the worker
 * function and the auxiliary data provided at submission time. */
typedef void *(*ovsdb_worker_fn)(void *arg);
typedef void (*ovsdb_worker_done_fn)(void *result, void *aux);

/* Lifecycle. */
struct ovsdb_worker_pool *ovsdb_worker_pool_create(
    size_t n_threads, const char *name);
void ovsdb_worker_pool_destroy(struct ovsdb_worker_pool *);

/* Job submission.  'fn' runs in a worker thread.  When it completes,
 * 'done_fn' is called on the main thread (during pool_run).
 * 'done_fn' may be NULL if no completion callback is needed. */
void ovsdb_worker_pool_submit(struct ovsdb_worker_pool *,
                              ovsdb_worker_fn fn, void *arg,
                              ovsdb_worker_done_fn done_fn,
                              void *aux);

/* Main-thread polling integration.
 *
 * ovsdb_worker_pool_run() delivers completed jobs by invoking their
 * done_fn callbacks on the main thread.  It should be called from
 * the main loop.
 *
 * ovsdb_worker_pool_wait() arranges for poll_block() to wake up when
 * new results become available.
 *
 * ovsdb_worker_pool_has_pending() returns true if any jobs are still
 * queued or executing. */
void ovsdb_worker_pool_run(struct ovsdb_worker_pool *);
void ovsdb_worker_pool_wait(struct ovsdb_worker_pool *);
bool ovsdb_worker_pool_has_pending(
    const struct ovsdb_worker_pool *);

#endif /* ovsdb/worker-pool.h */
