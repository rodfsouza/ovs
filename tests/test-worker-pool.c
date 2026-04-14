/* Copyright (c) 2024, 2025 Magalu Cloud.
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
#undef NDEBUG
#include <string.h>
#include <stdlib.h>

#include "ovsdb/worker-pool.h"
#include "ovstest.h"
#include "ovs-atomic.h"
#include "ovs-thread.h"
#include "openvswitch/poll-loop.h"
#include "seq.h"
#include "timeval.h"
#include "util.h"

/* Maximum iterations to spin-wait for jobs to complete. */
#define MAX_SPIN_ITERATIONS 5000

/* Sleep duration per spin iteration (1 ms in nanoseconds). */
#define SPIN_SLEEP_NS 1000000

/* ---- Shared state for test_basic ---- */

static void *
increment_worker(void *arg)
{
    struct atomic_count *counter = arg;
    atomic_count_inc(counter);
    return NULL;
}

static int done_count;

static void
count_done(void *result OVS_UNUSED, void *aux OVS_UNUSED)
{
    done_count++;
}

/* Spin-wait until done_count reaches 'expected', calling pool_run()
 * each iteration.  Asserts on timeout. */
static void
spin_until_done(struct ovsdb_worker_pool *pool, int expected)
{
    int i;
    for (i = 0; i < MAX_SPIN_ITERATIONS && done_count < expected; i++) {
        ovsdb_worker_pool_run(pool);
        xnanosleep(SPIN_SLEEP_NS);
    }
    ovs_assert(done_count == expected);
}

/* ---- test_basic ----
 * Create pool with 4 threads.  Submit 100 jobs that each increment an
 * atomic counter.  Verify counter == 100 after all done_fn fire. */
static void
test_basic(void)
{
    struct ovsdb_worker_pool *pool;
    struct atomic_count counter;
    unsigned int counter_val;
    int i;

    printf("  test_basic...\n");

    done_count = 0;
    atomic_count_init(&counter, 0);

    pool = ovsdb_worker_pool_create(4, "test-basic");
    ovs_assert(pool != NULL);

    for (i = 0; i < 100; i++) {
        ovsdb_worker_pool_submit(pool, increment_worker, &counter,
                                 count_done, NULL);
    }

    spin_until_done(pool, 100);

    counter_val = atomic_count_get(&counter);
    ovs_assert(counter_val == 100);

    ovsdb_worker_pool_destroy(pool);
    printf("  test_basic: ok\n");
}

/* ---- test_done_callback ----
 * Submit a job that returns a malloc'd string.  Verify done_fn
 * receives that pointer and free it there. */

static const char done_cb_expected[] = "hello from worker";
static bool done_cb_received;

static void *
string_worker(void *arg OVS_UNUSED)
{
    return xstrdup(done_cb_expected);
}

static void
string_done(void *result, void *aux OVS_UNUSED)
{
    char *s = result;
    ovs_assert(s != NULL);
    ovs_assert(!strcmp(s, done_cb_expected));
    free(s);
    done_cb_received = true;
    done_count++;
}

static void
test_done_callback(void)
{
    struct ovsdb_worker_pool *pool;

    printf("  test_done_callback...\n");

    done_count = 0;
    done_cb_received = false;

    pool = ovsdb_worker_pool_create(2, "test-done-cb");
    ovs_assert(pool != NULL);

    ovsdb_worker_pool_submit(pool, string_worker, NULL,
                             string_done, NULL);

    spin_until_done(pool, 1);
    ovs_assert(done_cb_received);

    ovsdb_worker_pool_destroy(pool);
    printf("  test_done_callback: ok\n");
}

/* ---- test_stress ----
 * 8 threads, 10 000 jobs.  Each job allocates 1 KB, fills with a
 * pattern byte, verifies the pattern, then frees.  This tests
 * ASAN for memory safety under contention. */

#define STRESS_JOBS    10000
#define STRESS_BUF_SZ  1024

struct stress_arg {
    unsigned char pattern;
};

static void *
stress_worker(void *arg)
{
    struct stress_arg *sa = arg;
    unsigned char *buf;
    int i;

    buf = xmalloc(STRESS_BUF_SZ);
    memset(buf, sa->pattern, STRESS_BUF_SZ);

    for (i = 0; i < STRESS_BUF_SZ; i++) {
        ovs_assert(buf[i] == sa->pattern);
    }

    free(buf);
    return NULL;
}

static void
test_stress(void)
{
    struct ovsdb_worker_pool *pool;
    struct stress_arg *args;
    int i;

    printf("  test_stress...\n");

    done_count = 0;
    args = xmalloc(STRESS_JOBS * sizeof *args);

    pool = ovsdb_worker_pool_create(8, "test-stress");
    ovs_assert(pool != NULL);

    for (i = 0; i < STRESS_JOBS; i++) {
        args[i].pattern = (unsigned char)(i & 0xff);
        ovsdb_worker_pool_submit(pool, stress_worker, &args[i],
                                 count_done, NULL);
    }

    spin_until_done(pool, STRESS_JOBS);

    ovsdb_worker_pool_destroy(pool);
    free(args);
    printf("  test_stress: ok\n");
}

/* ---- test_destroy_with_pending ----
 * Submit 100 jobs then immediately destroy.  Verify no crash
 * and no leak. */

static void *
slow_worker(void *arg OVS_UNUSED)
{
    xnanosleep(SPIN_SLEEP_NS);
    return NULL;
}

static void
test_destroy_with_pending(void)
{
    struct ovsdb_worker_pool *pool;
    int i;

    printf("  test_destroy_with_pending...\n");

    pool = ovsdb_worker_pool_create(4, "test-destroy");
    ovs_assert(pool != NULL);

    for (i = 0; i < 100; i++) {
        ovsdb_worker_pool_submit(pool, slow_worker, NULL,
                                 NULL, NULL);
    }

    /* Destroy immediately without draining. */
    ovsdb_worker_pool_destroy(pool);
    printf("  test_destroy_with_pending: ok\n");
}

/* ---- test_no_threads ----
 * Create pool with 0 threads (edge case).  Submit a job, call
 * pool_run().  If the implementation runs the job synchronously
 * or defers it, verify no crash and clean up. */

static int no_thread_done_count;

static void *
no_thread_worker(void *arg OVS_UNUSED)
{
    return NULL;
}

static void
no_thread_done(void *result OVS_UNUSED, void *aux OVS_UNUSED)
{
    no_thread_done_count++;
}

static void
test_no_threads(void)
{
    struct ovsdb_worker_pool *pool;
    int i;

    printf("  test_no_threads...\n");

    no_thread_done_count = 0;

    pool = ovsdb_worker_pool_create(0, "test-zero");

    if (!pool) {
        /* Implementation may reject 0 threads.  That is fine. */
        printf("  test_no_threads: skipped (pool creation "
               "returned NULL)\n");
        return;
    }

    ovsdb_worker_pool_submit(pool, no_thread_worker, NULL,
                             no_thread_done, NULL);

    /* Give the pool a chance to execute synchronously. */
    for (i = 0; i < MAX_SPIN_ITERATIONS; i++) {
        ovsdb_worker_pool_run(pool);
        if (no_thread_done_count > 0) {
            break;
        }
        xnanosleep(SPIN_SLEEP_NS);
    }

    /* Either the job completed or it will be cleaned up on
     * destroy.  Both are acceptable. */
    ovsdb_worker_pool_destroy(pool);
    printf("  test_no_threads: ok (done_count=%d)\n",
           no_thread_done_count);
}

/* ---- main ---- */

static void
test_worker_pool_main(int argc OVS_UNUSED, char *argv[] OVS_UNUSED)
{
    printf("test-worker-pool\n");

    test_basic();
    test_done_callback();
    test_stress();
    test_destroy_with_pending();
    test_no_threads();

    printf("test-worker-pool: ok\n");
}

OVSTEST_REGISTER("test-worker-pool", test_worker_pool_main);
