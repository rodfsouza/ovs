# Plan: Async Disk-Store Compaction via Worker Pool + Appctl

## Context

`ovsdb_disk_store_compact()` is implemented but never called. It needs to be wired into the server with an appctl command for manual triggering. The compaction must run asynchronously — disk I/O on a worker thread, atomic swap on the main thread — to avoid blocking client requests.

OVS already has: a worker pool (`ovsdb/worker-pool.h`), the lazy-load pattern as a blueprint, and an existing `ovsdb-server/compact` appctl command for JSON-file compaction.

## Design: Two-Phase Async Compaction

```
Main thread (appctl handler):
  1. Build compaction request struct
  2. Submit to worker pool
  3. Reply "compaction started" to appctl
  4. Return to event loop

Worker thread (Phase A — all disk I/O):
  5. Snapshot entries under rdlock (deep-copy)
  6. Group by table, sort by UUID
  7. Write temp file with header + rows
  8. Build new_index hmap
  9. Extract name column values for name index
  10. rename(tmp, original)  ← atomic on POSIX
  11. Return result struct to main thread

Main thread (Phase B — completion callback):
  12. Acquire wrlock, swap fd + index
  13. Release wrlock
  14. Clear and rebuild name index (in-memory only)
  15. Rebuild bloom filter (in-memory only)
  16. Destroy old entries
  17. Log completion
```

### Why rename() in the worker

`rename()` is atomic on POSIX. After rename, the filename points to the new file. Old fd still reads the old (unlinked) file until we close it. Doing rename in the worker means the main thread's swap (step 12) is just pointer assignments — microseconds.

### Compaction Request Struct

```c
struct compact_request {
    struct ovsdb *db;
    struct ovsdb_disk_store *store;
    struct ovsdb_table *table;        /* For name/bloom rebuild. */

    /* Populated by worker (result). */
    int new_fd;                       /* fd of new compacted file. */
    struct hmap new_index;            /* New UUID→offset index. */
    struct ovsdb_error *error;        /* NULL on success. */
    bool success;
};
```

### Worker Function

```c
static void *
compact_worker(void *arg)
{
    struct compact_request *req = arg;
    
    /* Reuse the existing compaction logic from
     * ovsdb_disk_store_compact(), but split:
     * - Phase A (here): everything up to and including rename
     * - Phase B (done callback): fd/index swap
     *
     * The current compact() does both phases synchronously.
     * Refactor it to accept a mode flag or split into two
     * functions. */
    
    req->error = ovsdb_disk_store_compact_prepare(req->store,
                                                   &req->new_fd,
                                                   &req->new_index);
    req->success = (req->error == NULL);
    return req;
}
```

### Completion Callback (Main Thread)

```c
static void
compact_done(void *result, void *aux)
{
    struct compact_request *req = result;
    
    if (!req->success) {
        VLOG_WARN("disk-store compaction failed: %s",
                  ovsdb_error_to_string(req->error));
        ovsdb_error_destroy(req->error);
        free(req);
        return;
    }
    
    /* Phase B: atomic swap on main thread. */
    ovsdb_disk_store_compact_commit(req->store,
                                    req->new_fd,
                                    &req->new_index);
    
    /* Rebuild bloom filter (in-memory, fast). */
    if (req->table->bloom) {
        ovsdb_disk_store_rebuild_bloom(req->store,
                                       &req->table->bloom,
                                       req->table->schema->name);
    }
    
    /* Rebuild name index (in-memory, fast). */
    if (req->table->name_index) {
        ovsdb_name_index_clear(req->table->name_index);
        ovsdb_disk_store_build_name_index(req->store,
                                          req->table->name_index);
    }
    
    VLOG_INFO("disk-store compaction completed for table %s",
              req->table->schema->name);
    free(req);
}
```

### Appctl Handler

```c
static void
ovsdb_server_compact_disk_store(struct unixctl_conn *conn,
                                int argc OVS_UNUSED,
                                const char *argv[] OVS_UNUSED,
                                void *dbs_)
{
    struct shash *all_dbs = dbs_;
    struct shash_node *node;
    int submitted = 0;
    
    SHASH_FOR_EACH (node, all_dbs) {
        struct db *db = node->data;
        if (!db->db->disk_store_mode) {
            continue;
        }
        
        struct shash_node *tnode;
        SHASH_FOR_EACH (tnode, &db->db->tables) {
            struct ovsdb_table *table = tnode->data;
            if (!table->disk_store) {
                continue;
            }
            
            struct compact_request *req = xzalloc(sizeof *req);
            req->db = db->db;
            req->store = table->disk_store;
            req->table = table;
            
            ovsdb_worker_pool_submit(lazy_pool,
                                     compact_worker, req,
                                     compact_done, req);
            submitted++;
        }
    }
    
    if (submitted > 0) {
        unixctl_command_reply(conn, "compaction started");
    } else {
        unixctl_command_reply(conn, "no disk-store tables to compact");
    }
}
```

### Refactor `ovsdb_disk_store_compact()` into Two Functions

Split the existing monolithic compact into:

**`ovsdb_disk_store_compact_prepare()`** — runs on worker thread:
- Takes: `store`, output `new_fd`, output `new_index`
- Does: write header, copy rows (grouped/sorted), build new_index, rename
- Returns: error or NULL

**`ovsdb_disk_store_compact_commit()`** — runs on main thread:
- Takes: `store`, `new_fd`, `new_index`
- Does: wrlock, swap fd+index, unlock, destroy old entries
- No return (always succeeds once prepare succeeded)

The existing `ovsdb_disk_store_compact()` becomes:
```c
struct ovsdb_error *
ovsdb_disk_store_compact(struct ovsdb_disk_store *store)
{
    int new_fd;
    struct hmap new_index;
    struct ovsdb_error *error;
    
    error = ovsdb_disk_store_compact_prepare(store, &new_fd, &new_index);
    if (error) {
        return error;
    }
    ovsdb_disk_store_compact_commit(store, new_fd, &new_index);
    return NULL;
}
```

Backwards compatible — the synchronous API still works.

## Files to Modify

| File | Change |
|------|--------|
| `ovsdb/disk-store.c` | Split `compact()` into `compact_prepare()` + `compact_commit()`. Keep `compact()` as wrapper. |
| `ovsdb/disk-store.h` | Expose `compact_prepare()` and `compact_commit()` |
| `ovsdb/ovsdb-server.c` | Register `ovsdb-server/compact-disk-store` appctl command. Add `compact_worker()` and `compact_done()`. |

## Guard Rails

- **Only one compaction at a time**: Add a `bool compacting` flag on `ovsdb_disk_store`. Reject appctl if already in progress.
- **Worker pool availability**: Check `ovsdb_lazy_load_pool_available()` before submitting. Fall back to synchronous if no pool.
- **Shared disk store**: Multiple tables share the same `disk_store`. Compact per-store, not per-table. Track which store was already submitted.

## Tests

### Test 1: Appctl triggers compaction
- Start server with `--disk-store`, insert rows
- Run `ovs-appctl ovsdb-server/compact-disk-store`
- Verify "compaction started" reply
- Verify file size decreases after deleting rows + compact

### Test 2: Compaction during active queries
- Insert rows, start a long-running select
- Trigger compaction via appctl
- Verify select still returns correct results
- Verify compaction completes

### Test 3: Bloom + name index rebuilt after compaction
- Insert 50 rows, delete 25, compact
- Verify bloom filter has 25 entries (not 50)
- Verify name index has 25 entries
- Verify lookups by UUID and name work

### Test 4: Double compaction rejected
- Trigger compaction, immediately trigger again
- Verify second attempt returns "already in progress"

### Test 5: Fallback to synchronous if no worker pool
- Start without worker pool
- Trigger compaction
- Verify it completes synchronously

### Test 6: Regression
```bash
make check TESTSUITEFLAGS="-j4 -k ovsdb"
```

## Verification

```bash
make -j4
make check TESTSUITEFLAGS="-j4 -k 'queries on' -k 'monitor-cond' -k 'delete all'"
```

After implementation: `/simplify` and `/review`.
