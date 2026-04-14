# Plan: Native Binary Format Support + Worker Pool Wiring

## Context

All disk-store, row-cache, and worker-pool infrastructure is implemented
but completely disconnected from the ovsdb-server runtime.  `table->cache`
and `table->disk_store` are always NULL; `io_worker_pool` never receives
work; `waiting_for_data` is never set to true.  ovsdb-server currently
rejects binary databases with an error.

This plan wires everything together so ovsdb-server can natively open and
serve binary databases, and the worker pool performs async row loading.

---

## Scope

### Feature 1: `--disk-store` command-line flag
Add a flag to ovsdb-server that enables binary disk store mode.  When set,
tables get `disk_store` and `cache` attached, and the worker pool is used
for async row loading.

### Feature 2: Binary database open path in storage layer
Extend `ovsdb_storage_open__()` to open binary databases natively instead
of returning an error.  Create a new `ovsdb_storage` variant that wraps
a `struct ovsdb_disk_store`.

### Feature 3: Wire table cache and disk store at database load
When loading a binary database, attach `disk_store` and `cache` to each
table and populate the cache with UNLOADED entries from the disk index.

### Feature 4: Wire worker pool for async row loading
When `ovsdb_table_get_row()` encounters a cache miss with state UNLOADED,
submit an async load job to `io_worker_pool` instead of blocking.

### Feature 5: Wire trigger parking for lazy loading
When a trigger references unloaded rows, set `waiting_for_data = true`
to park it.  When the worker pool loads the row, clear the flag and
re-run triggers.

---

## Files to Modify

| File | Change |
|------|--------|
| `ovsdb/storage.c` | Replace error return with actual binary open path; add `disk_store` field to `struct ovsdb_storage` |
| `ovsdb/storage.h` | Declare accessor for disk_store in storage |
| `ovsdb/ovsdb-server.c` | Add `--disk-store` flag; pass flag to open_db; wire pool to table loading |
| `ovsdb/ovsdb.c` | In `ovsdb_create()`: if binary mode, attach cache+disk_store to tables |
| `ovsdb/ovsdb.h` | Add `bool disk_store_mode` field to `struct ovsdb` |
| `ovsdb/table.c` | Update `ovsdb_table_get_row()` to submit async load jobs via worker pool |
| `ovsdb/table.h` | No changes (fields already exist) |
| `ovsdb/trigger.c` | Set `waiting_for_data = true` when transaction references unloaded rows |
| `ovsdb/row-cache.h` | Add waiter callback registration API |
| `ovsdb/row-cache.c` | Implement waiter notification on row load completion |
| `tests/ovsdb-binary-serve.at` | New: integration tests for serving binary DBs |
| `tests/ovsdb.at` | Register new test module |

---

## Implementation Steps

### Step 1: Add `--disk-store` flag to ovsdb-server

In `ovsdb/ovsdb-server.c`:

1. Add `OPT_DISK_STORE` to the options enum (after `OPT_CONFIG_FILE`)
2. Add `{"disk-store", no_argument, NULL, OPT_DISK_STORE}` to `long_options[]`
3. Add `static bool disk_store_enabled;` global
4. Handle in switch: `case OPT_DISK_STORE: disk_store_enabled = true; break;`
5. Pass the flag through `server_config` to `open_db()`

### Step 2: Extend `struct ovsdb_storage` for binary databases

In `ovsdb/storage.c`, add a `disk_store` field:

```c
struct ovsdb_storage {
    struct ovsdb_log *log;
    struct raft *raft;
    struct ovsdb_disk_store *disk_store;  /* NEW: binary mode. */
    char *unbacked_name;
    /* ... rest unchanged ... */
};
```

In `ovsdb_storage_open__()`, replace the error return:

```c
if (ovsdb_disk_store_is_binary(filename)) {
    struct ovsdb_disk_store *ds;
    ds = ovsdb_disk_store_open(filename, NULL);
    if (!ds) {
        return ovsdb_error(NULL, "%s: failed to open binary "
                           "disk store", filename);
    }
    struct ovsdb_storage *storage = xzalloc(sizeof *storage);
    storage->disk_store = ds;
    schedule_next_snapshot(storage, false);
    *storagep = storage;
    return NULL;
}
```

Add `ovsdb_storage_is_disk_store()` accessor:

```c
bool
ovsdb_storage_is_disk_store(const struct ovsdb_storage *s)
{
    return s->disk_store != NULL;
}

struct ovsdb_disk_store *
ovsdb_storage_get_disk_store(const struct ovsdb_storage *s)
{
    return s->disk_store;
}
```

Update `ovsdb_storage_close()` to close disk_store.

Update `ovsdb_storage_read_schema()` to read from disk_store when
in binary mode (using `ds->schema`).

### Step 3: Create `ovsdb_file_read_binary()` for binary database loading

In `ovsdb/file.c`, add a new function that creates an ovsdb from a
binary disk store:

```c
struct ovsdb *
ovsdb_file_read_binary(const char *filename)
{
    /* 1. Open storage (detects binary, creates ds). */
    /* 2. Read schema from ds->schema. */
    /* 3. Create ovsdb with ovsdb_create(schema, storage). */
    /* 4. Do NOT replay rows — they stay on disk. */
    /* 5. Return ovsdb with empty in-memory tables. */
}
```

This is the key difference from JSON mode: rows are NOT loaded into
memory.  They stay in the disk store and are loaded on demand.

### Step 4: Attach cache + disk_store to tables on binary open

In `ovsdb/ovsdb.c`, after `ovsdb_create()`, if `disk_store_mode`:

```c
void
ovsdb_attach_disk_store(struct ovsdb *db, size_t cache_max_atoms)
{
    struct ovsdb_disk_store *ds;
    ds = ovsdb_storage_get_disk_store(db->storage);
    if (!ds) {
        return;
    }

    struct shash_node *node;
    SHASH_FOR_EACH (node, &db->tables) {
        struct ovsdb_table *table = node->data;

        /* Attach disk store and cache to each table. */
        table->disk_store = ds;  /* Shared, not owned. */
        table->cache = ovsdb_row_cache_create(cache_max_atoms);

        /* Populate cache index with UNLOADED entries from
         * the disk store's UUID index. */
        struct ovsdb_disk_store_cursor *cursor;
        cursor = ovsdb_disk_store_cursor_open(ds, node->name);
        if (cursor) {
            /* Walk the cursor to get UUIDs without loading
             * full row data.  We need a lightweight iterator
             * that yields UUIDs only. */
            /* For now: load count and register as unloaded. */
            ovsdb_disk_store_cursor_close(cursor);
        }
    }
    db->disk_store_mode = true;
}
```

This requires adding a UUID-only iteration function to disk-store
(e.g., `ovsdb_disk_store_list_uuids()`) that walks the in-memory
index without reading row data from disk.

### Step 5: Add UUID listing to disk store

In `ovsdb/disk-store.c`, add:

```c
/* Calls 'cb' for each non-deleted UUID in the given table.
 * Does not read row data from disk. */
void
ovsdb_disk_store_for_each_uuid(
    struct ovsdb_disk_store *ds,
    const char *table_name,
    void (*cb)(const struct uuid *, void *aux),
    void *aux);
```

This iterates the in-memory index (no disk I/O) and calls the
callback for each UUID.  Used to populate the row cache with
UNLOADED entries at startup.

### Step 6: Wire worker pool into table row access

In `ovsdb/table.c`, modify `ovsdb_table_get_row()`:

The current code for disk store fallback does a synchronous read:
```c
if (table->disk_store && table->cache) {
    row = ovsdb_disk_store_read_row(...);
    ...
}
```

Change to async via worker pool when available:

```c
if (table->disk_store && table->cache) {
    enum ovsdb_row_state state;
    state = ovsdb_row_cache_get_state(table->cache, uuid);

    switch (state) {
    case OVSDB_ROW_CACHED:
        /* Already in cache, return it. */
        return ovsdb_row_cache_lookup(table->cache, uuid);

    case OVSDB_ROW_LOADING:
        /* Load in progress, return NULL.  Caller must park. */
        return NULL;

    case OVSDB_ROW_UNLOADED:
        /* Submit async load job to worker pool. */
        ovsdb_row_cache_set_state(table->cache, uuid,
                                  OVSDB_ROW_LOADING);
        /* Submit job (defined in ovsdb-server.c or table.c). */
        submit_row_load_job(table, uuid);
        return NULL;  /* Caller must park. */
    }
}
```

The `submit_row_load_job()` function submits work to `io_worker_pool`:
- Worker function: `pread()` row from disk store, deserialize
- Done function: insert into cache as CACHED, set
  `db->run_triggers = true` to wake parked triggers

### Step 7: Wire trigger parking

In `ovsdb/trigger.c`, inside `ovsdb_trigger_try()`, before executing
the transaction:

```c
/* If the database is in disk-store mode and the transaction
 * references rows that are not yet loaded, park this trigger. */
if (t->db->disk_store_mode && !t->waiting_for_data) {
    /* Check if the transaction can proceed with current data.
     * ovsdb_execute_compose() will call ovsdb_table_get_row()
     * which returns NULL for UNLOADED/LOADING rows.  If the
     * transaction fails due to missing rows, park it. */
}
```

The simplest approach: let the transaction execute.  If it references
a row that returns NULL from `get_row()` (because UNLOADED/LOADING),
the transaction sees "row not found" which is an error.  Catch this
specific error and park the trigger:

```c
if (error && t->db->disk_store_mode
    && ovsdb_error_is_row_not_loaded(error)) {
    ovsdb_error_destroy(error);
    t->waiting_for_data = true;
    return false;  /* Will retry later. */
}
```

When the worker pool loads a row and calls `db->run_triggers = true`,
the parked trigger retries and succeeds.

### Step 8: Worker pool done callback

Define the row load done callback in `ovsdb-server.c` or a new
`ovsdb/lazy-load.c`:

```c
struct row_load_request {
    struct ovsdb_disk_store *ds;
    struct ovsdb_table *table;
    struct uuid uuid;
};

static void *
row_load_worker(void *arg)
{
    struct row_load_request *req = arg;
    struct ovsdb_row *row;

    row = ovsdb_disk_store_read_row(req->ds, req->table,
                                    &req->uuid);
    return row;
}

static void
row_load_done(void *result, void *aux)
{
    struct ovsdb_row *row = result;
    struct row_load_request *req = aux;

    if (row) {
        ovsdb_row_cache_insert(req->table->cache, row,
                               ovsdb_row_count_atoms(row));
        ovsdb_row_cache_set_state(req->table->cache,
                                  &req->uuid,
                                  OVSDB_ROW_CACHED);
        /* Wake triggers to retry. */
        req->table->/* need access to db */->run_triggers = true;
    }
    free(req);
}
```

### Step 9: Update `open_db()` to use binary path

In `ovsdb/ovsdb-server.c`, modify `open_db()`:

```c
if (disk_store_enabled
    && ovsdb_disk_store_is_binary(filename)) {
    /* Binary path: open storage (which now creates ds),
     * read schema, create ovsdb, attach disk store to tables. */
    ...
    ovsdb_attach_disk_store(db->db, OVSDB_CACHE_MAX_ATOMS);
    /* Skip read_db() — rows loaded on demand. */
} else {
    /* Existing JSON path unchanged. */
    read_db(config, db);
}
```

### Step 10: Tests

New file `tests/ovsdb-binary-serve.at`:

1. **Start server with --disk-store on binary DB**: Create JSON db,
   convert to binary, start ovsdb-server with `--disk-store`, verify
   it opens without error.

2. **Query data from binary DB**: Insert data, convert to binary,
   start server, query via ovsdb-client — verify rows returned.

3. **Transaction on binary DB**: Start server on binary DB, insert
   new rows via ovsdb-client transact, query back.

4. **Concurrent access during lazy load**: Start server on large
   binary DB, immediately send multiple queries, verify all complete
   without timeout.

---

## Verification

1. Compile all modified files with gcc
2. Run existing tests: `ovstest test-disk-store`, `test-row-cache`,
   `test-worker-pool` — must still pass
3. Run new tests: `make check TESTSUITEFLAGS="-k binary-serve"`
4. Manual test:
   ```bash
   ovsdb-tool create conf.db vswitch.ovsschema
   # insert data...
   ovsdb-tool convert-format conf.db binary
   ovsdb-server --disk-store --remote=punix:db.sock conf.db &
   ovsdb-client list-dbs unix:db.sock
   ovs-vsctl show
   ```

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Async load race: trigger executes before row loaded | Trigger parks via `waiting_for_data`, retries when row arrives |
| Double ownership: worker loads row, transaction also modifies | Worker inserts into cache only; hmap rows unchanged. Cache is main-thread only. |
| Disk store shared across tables | `table->disk_store` is a shared pointer, not owned. Only `ovsdb_storage` owns it. Cleanup in `ovsdb_table_destroy()` must not call `ovsdb_disk_store_close()`. |
| Schema not embedded in binary file | Schema stored in `ds->schema` field set at open time. `convert-format` provides schema from JSON source. |
| Server crashes with partial cache | On restart, cache is rebuilt from disk index. Disk store is crash-safe (append-only + index rebuild). |
