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

#include <config.h>

#include "worker-pool.h"

#include <pthread.h>
#include <string.h>

#include "openvswitch/list.h"
#include "openvswitch/vlog.h"
#include "ovs-thread.h"
#include "seq.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_worker_pool);

/* A single unit of work submitted to the pool. */
struct ovsdb_worker_job {
    struct ovs_list list_node;    /* In pending_jobs or done_jobs. */
    ovsdb_worker_fn fn;           /* Function to execute in worker. */
    void *arg;                    /* Argument passed to 'fn'. */
    void *result;                 /* Return value from 'fn'. */
    ovsdb_worker_done_fn done_fn; /* Callback for main thread. */
    void *aux;                    /* Auxiliary data for 'done_fn'. */
};

/* Thread pool with seq-based completion signaling.
 *
 * Workers dequeue jobs from 'pending_jobs', execute them, and place
 * them on 'done_jobs'.  The main thread picks up completed jobs
 * during ovsdb_worker_pool_run() and invokes their callbacks.
 *
 * Completion is signaled via 'done_seq' so that poll_block() in the
 * main loop wakes up when results are ready. */
struct ovsdb_worker_pool {
    /* Worker threads. */
    pthread_t *threads;
    size_t n_threads;
    char *name;                   /* Pool name for diagnostics. */

    /* Job queues (protected by 'mutex'). */
    struct ovs_mutex mutex;
    struct ovs_list pending_jobs; /* Jobs waiting for a worker. */
    struct ovs_list done_jobs;    /* Completed, awaiting callback. */
    bool shutting_down;           /* True when pool is stopping. */
    pthread_cond_t work_available; /* Workers block here. */

    /* Counts of in-flight jobs (protected by 'mutex'). */
    size_t n_pending;             /* Jobs on pending_jobs list. */
    size_t n_running;             /* Jobs currently executing. */
    size_t n_done;                /* Jobs on done_jobs list. */

    /* Seq-based signaling to main thread. */
    struct seq *done_seq;
    uint64_t done_seqno;
};

static void *ovsdb_worker_thread(void *arg);

/* Creates and returns a new worker pool with 'n_threads' worker
 * threads.  'name' is used as a prefix for thread names and
 * diagnostic messages. */
struct ovsdb_worker_pool *
ovsdb_worker_pool_create(size_t n_threads, const char *name)
{
    struct ovsdb_worker_pool *pool;
    size_t i;

    if (!n_threads) {
        VLOG_WARN("%s: pool requested with 0 threads, disabled.",
                  name);
        return NULL;
    }

    pool = xzalloc(sizeof *pool);
    pool->name = xstrdup(name);
    pool->n_threads = n_threads;
    pool->threads = xmalloc(n_threads * sizeof *pool->threads);

    ovs_mutex_init(&pool->mutex);
    ovs_list_init(&pool->pending_jobs);
    ovs_list_init(&pool->done_jobs);
    pool->shutting_down = false;
    xpthread_cond_init(&pool->work_available, NULL);

    pool->n_pending = 0;
    pool->n_running = 0;
    pool->n_done = 0;

    pool->done_seq = seq_create();
    pool->done_seqno = seq_read(pool->done_seq);

    VLOG_INFO("%s: creating worker pool with %"PRIuSIZE" threads.",
              name, n_threads);

    for (i = 0; i < n_threads; i++) {
        char thread_name[16];

        snprintf(thread_name, sizeof thread_name,
                 "%.11s_%02u", name, (unsigned int) i);
        pool->threads[i] = ovs_thread_create(thread_name,
                                             ovsdb_worker_thread,
                                             pool);
    }

    return pool;
}

/* Shuts down and frees 'pool'.
 *
 * The caller must ensure that no new jobs are submitted after calling
 * this function.  Any jobs still on the done queue will have their
 * done_fn callbacks invoked before the pool is freed.  Jobs that are
 * pending or running will be allowed to finish. */
void
ovsdb_worker_pool_destroy(struct ovsdb_worker_pool *pool)
{
    struct ovsdb_worker_job *job;
    size_t i;

    if (!pool) {
        return;
    }

    VLOG_INFO("%s: shutting down worker pool.", pool->name);

    /* Signal all workers to exit. */
    ovs_mutex_lock(&pool->mutex);
    pool->shutting_down = true;
    xpthread_cond_broadcast(&pool->work_available);
    ovs_mutex_unlock(&pool->mutex);

    /* Wait for all worker threads to finish. */
    for (i = 0; i < pool->n_threads; i++) {
        xpthread_join(pool->threads[i], NULL);
    }

    /* Deliver any remaining completed jobs. */
    ovs_mutex_lock(&pool->mutex);
    LIST_FOR_EACH_POP (job, list_node, &pool->done_jobs) {
        ovs_mutex_unlock(&pool->mutex);
        if (job->done_fn) {
            job->done_fn(job->result, job->aux);
        }
        free(job);
        ovs_mutex_lock(&pool->mutex);
    }

    /* Drain any jobs that were never picked up by a worker.
     * This can only happen if the pool is destroyed while jobs
     * are still pending. */
    LIST_FOR_EACH_POP (job, list_node, &pool->pending_jobs) {
        VLOG_WARN("%s: discarding unprocessed job during "
                  "shutdown.", pool->name);
        free(job);
    }
    ovs_mutex_unlock(&pool->mutex);

    xpthread_cond_destroy(&pool->work_available);
    ovs_mutex_destroy(&pool->mutex);
    seq_destroy(pool->done_seq);
    free(pool->threads);
    free(pool->name);
    free(pool);
}

/* Submits a job to 'pool'.
 *
 * 'fn' will be called in a worker thread with 'arg'.  When 'fn'
 * returns, the result is stored and 'done_fn' is called on the main
 * thread during ovsdb_worker_pool_run() with the result and 'aux'.
 *
 * 'done_fn' may be NULL if no completion callback is needed; in that
 * case the result of 'fn' is discarded. */
void
ovsdb_worker_pool_submit(struct ovsdb_worker_pool *pool,
                         ovsdb_worker_fn fn, void *arg,
                         ovsdb_worker_done_fn done_fn,
                         void *aux)
{
    struct ovsdb_worker_job *job;

    ovs_assert(!pool->shutting_down);

    job = xmalloc(sizeof *job);
    job->fn = fn;
    job->arg = arg;
    job->result = NULL;
    job->done_fn = done_fn;
    job->aux = aux;

    ovs_mutex_lock(&pool->mutex);
    ovs_list_push_back(&pool->pending_jobs, &job->list_node);
    pool->n_pending++;
    xpthread_cond_signal(&pool->work_available);
    ovs_mutex_unlock(&pool->mutex);
}

/* Delivers completed jobs to the main thread by calling their
 * done_fn callbacks.
 *
 * This function must be called from the main thread, typically as
 * part of the main loop's run cycle. */
void
ovsdb_worker_pool_run(struct ovsdb_worker_pool *pool)
{
    struct ovs_list ready_jobs;
    struct ovsdb_worker_job *job;

    /* Move all done jobs to a local list to minimize time spent
     * holding the lock. */
    ovs_list_init(&ready_jobs);

    ovs_mutex_lock(&pool->mutex);
    if (!ovs_list_is_empty(&pool->done_jobs)) {
        ovs_list_splice(&ready_jobs, pool->done_jobs.next,
                        &pool->done_jobs);
        pool->n_done = 0;
    }
    ovs_mutex_unlock(&pool->mutex);

    /* Invoke callbacks outside the lock. */
    LIST_FOR_EACH_POP (job, list_node, &ready_jobs) {
        if (job->done_fn) {
            job->done_fn(job->result, job->aux);
        }
        free(job);
    }

    /* Update seqno so that pool_wait sees the latest state. */
    pool->done_seqno = seq_read(pool->done_seq);
}

/* Arranges for the next call to poll_block() to wake up when jobs
 * complete in 'pool'. */
void
ovsdb_worker_pool_wait(struct ovsdb_worker_pool *pool)
{
    seq_wait(pool->done_seq, pool->done_seqno);
}

/* Returns true if 'pool' has any jobs that are queued, running, or
 * completed but not yet delivered. */
bool
ovsdb_worker_pool_has_pending(const struct ovsdb_worker_pool *pool)
{
    bool pending;

    ovs_mutex_lock(&pool->mutex);
    pending = pool->n_pending > 0
              || pool->n_running > 0
              || pool->n_done > 0;
    ovs_mutex_unlock(&pool->mutex);

    return pending;
}

/* Worker thread main loop.
 *
 * Each worker blocks on the condition variable until a job is
 * available (or shutdown is signaled).  It dequeues one job at a
 * time, executes it outside the lock, and places the result on the
 * done queue.  After enqueueing a result it bumps done_seq to wake
 * the main thread's poll_block(). */
static void *
ovsdb_worker_thread(void *arg)
{
    struct ovsdb_worker_pool *pool = arg;

    ovs_mutex_lock(&pool->mutex);
    for (;;) {
        struct ovsdb_worker_job *job;

        /* Wait for work or shutdown. */
        while (ovs_list_is_empty(&pool->pending_jobs)
               && !pool->shutting_down) {
            ovs_mutex_cond_wait(&pool->work_available,
                                &pool->mutex);
        }

        /* If shutting down and no pending work, exit. */
        if (pool->shutting_down
            && ovs_list_is_empty(&pool->pending_jobs)) {
            break;
        }

        /* Dequeue one job. */
        job = CONTAINER_OF(ovs_list_pop_front(&pool->pending_jobs),
                           struct ovsdb_worker_job, list_node);
        pool->n_pending--;
        pool->n_running++;

        /* Execute outside the lock. */
        ovs_mutex_unlock(&pool->mutex);
        job->result = job->fn(job->arg);
        ovs_mutex_lock(&pool->mutex);

        /* Enqueue result. */
        pool->n_running--;
        pool->n_done++;
        ovs_list_push_back(&pool->done_jobs, &job->list_node);

        /* Wake the main thread. */
        ovs_mutex_unlock(&pool->mutex);
        seq_change(pool->done_seq);
        ovs_mutex_lock(&pool->mutex);
    }
    ovs_mutex_unlock(&pool->mutex);

    return NULL;
}
