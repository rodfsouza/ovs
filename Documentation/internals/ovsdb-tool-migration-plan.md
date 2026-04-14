# Plan: ovsdb-tool Format Migration (JSON ↔ Binary)

## Context

Phase 1 introduced a binary disk store (`ovsdb/disk-store.c`) with `BINARYV1` file
format. All existing OVS deployments use the JSON-based log format (magic `"JSON"`).
We need migration tooling so administrators can upgrade databases to binary format and
downgrade back to JSON for rollback.

The binary format is **local storage only** — Raft replication stays JSON on the wire.
Rolling upgrades in clustered environments need no coordination.

---

## Files to Modify

| File | Change |
|------|--------|
| `ovsdb/ovsdb-tool.c` | Add `convert-format` and `db-format` commands; update `compact_or_convert()` for binary awareness |
| `ovsdb/storage.c` | Extend `ovsdb_storage_open__()` to detect `BINARYV1` magic and open via disk store |
| `ovsdb/log.h` | Add `OVSDB_BINARY_MAGIC` constant |
| `ovsdb/file.c` | Add `ovsdb_file_write_binary()` helper for JSON→binary conversion |
| `ovsdb/disk-store.c` | Add `ovsdb_disk_store_write_schema()` to embed schema in binary file |
| `ovsdb/disk-store.h` | Declare new function |
| `tests/ovsdb-migration.at` | New: 5 migration test cases |
| `tests/ovsdb.at` | Register migration test module |

## Existing Code Entry Points

- **Format detection**: `ovsdb_storage_open__()` at `storage.c:63-97` — opens log with `OVSDB_MAGIC"|"RAFT_MAGIC`, checks magic via `ovsdb_log_get_magic()`
- **Magic validation**: `ovsdb_log_open()` at `log.c:126-291` — `is_magic_ok()` validates against pipe-delimited magic string
- **Schema reading**: `read_standalone_schema()` at `ovsdb-tool.c:254-262` — uses `ovsdb_storage_open_standalone()` → `ovsdb_storage_read_schema()`
- **Compact/convert core**: `compact_or_convert()` at `ovsdb-tool.c:377-437` — reads DB into memory, writes back as single snapshot
- **JSON log replay**: `ovsdb_file_read__()` at `file.c:566-609` — loops reading transactions, replays each into in-memory tables
- **DB write**: `write_standalone_db()` at `ovsdb-tool.c:264-300` — writes schema + data as JSON log records

## Implementation Steps

### Step 1: Add `OVSDB_BINARY_MAGIC` constant

In `ovsdb/log.h`, add alongside existing `OVSDB_MAGIC`:
```c
#define OVSDB_MAGIC        "JSON"
#define OVSDB_BINARY_MAGIC "BINARYV1"
```

### Step 2: Extend `ovsdb_storage_open__()` for binary format detection

In `storage.c`, before the existing `ovsdb_log_open()` call, probe the file header to detect binary format:

```c
/* Probe for binary disk store format first. */
if (ovsdb_disk_store_is_binary(filename)) {
    /* Open via disk store path. */
    struct ovsdb_disk_store *ds;
    struct ovsdb_schema *schema;
    ds = ovsdb_disk_store_open(filename, NULL);
    /* Store reference in storage for later use. */
    ...
    return NULL;
}
/* Fall through to existing JSON/Raft log open. */
```

This requires adding `ovsdb_disk_store_is_binary()` — a function that reads the first 8 bytes and checks for `"BINARYV1"` magic.

### Step 3: Add `ovsdb_disk_store_is_binary()` probe

In `disk-store.c`:
```c
bool
ovsdb_disk_store_is_binary(const char *filename)
{
    /* Read first 8 bytes, compare to DISK_STORE_MAGIC. */
}
```

### Step 4: Add `ovsdb_disk_store_read_schema()` to extract embedded schema

The binary file embeds the schema JSON after the header. Add a function to read it without loading all rows:
```c
struct ovsdb_schema *
ovsdb_disk_store_read_schema(const char *filename)
{
    /* Open file, read header, seek to schema_offset,
     * read schema JSON, parse, return. */
}
```

### Step 5: Add `do_convert_format` command in `ovsdb-tool.c`

New command: `ovsdb-tool convert-format DB --format=json|binary`

```c
static void
do_convert_format(struct ovs_cmdl_context *ctx)
{
    const char *db = ctx->argv[1];
    const char *format = ctx->argv[2];  /* "json" or "binary" */

    if (!strcmp(format, "binary")) {
        /* JSON → Binary:
         * 1. ovsdb_file_read(db) → in-memory ovsdb
         * 2. ovsdb_disk_store_open(tmp_file, schema)
         * 3. Iterate all tables/rows, write each
         * 4. Close disk store
         * 5. Atomic rename tmp → db */
    } else if (!strcmp(format, "json")) {
        /* Binary → JSON:
         * 1. ovsdb_disk_store_open(db, schema)
         * 2. Iterate all rows via cursor → build in-memory ovsdb
         * 3. write_standalone_db(ovsdb, tmp_file)
         * 4. Atomic rename tmp → db */
    }
}
```

### Step 6: Add `do_db_format` command in `ovsdb-tool.c`

New command: `ovsdb-tool db-format DB` → prints `"json"` or `"binaryv1"`

```c
static void
do_db_format(struct ovs_cmdl_context *ctx)
{
    const char *db = ctx->argv[1];
    if (ovsdb_disk_store_is_binary(db)) {
        puts("binaryv1");
    } else {
        puts("json");
    }
}
```

### Step 7: Update existing commands for binary awareness

- **`do_db_version`** / **`do_db_cksum`**: If binary, use `ovsdb_disk_store_read_schema()` instead of `read_standalone_schema()`
- **`do_compact`**: If binary, open disk store, compact via `ovsdb_disk_store_compact()`, done
- **`do_db_is_standalone`**: Also return true for `BINARYV1` (binary is standalone)
- **`do_show_log`**: If binary, iterate rows with cursor and print human-readable output
- **`do_needs_conversion`**: If binary, read schema from binary header for comparison

### Step 8: Register new commands

Add to `all_commands[]` in `ovsdb-tool.c`:
```c
{ "convert-format", "db format", 2, 2, do_convert_format, OVS_RO },
{ "db-format", "[db]", 0, 1, do_db_format, OVS_RO },
```

### Step 9: Write migration tests

New file `tests/ovsdb-migration.at` with 5 test cases:

1. **json to binary upgrade**: Create JSON db with `ovsdb-tool create`, insert data via `ovsdb-tool transact`, convert to binary, verify data readable
2. **binary to json downgrade**: Create binary db, convert back to JSON, verify old tools can read it
3. **schema convert on binary DB**: Create binary db, convert schema, verify columns correct
4. **binary format version forward compat**: Craft a file with `format_version=99`, verify clean error
5. **db-format command**: Create both formats, verify `ovsdb-tool db-format` reports correctly

Register in `tests/ovsdb.at`:
```
m4_include([tests/ovsdb-migration.at])
```

## Verification

1. Compile all modified files with gcc (same flags as current setup)
2. Run `ovstest test-disk-store` — existing tests still pass
3. Run migration .at tests via the test suite
4. Manual test: create a JSON db, convert to binary, convert back, diff — must be identical content

## Risks

| Risk | Mitigation |
|------|-----------|
| Binary file corruption during convert | Atomic rename: write to temp file, rename only on success |
| Schema mismatch between binary header and row data | Validate schema hash on open; reject if mismatch |
| Partial write on crash during conversion | Temp file is cleaned up on next run; original untouched until rename |
| Old OVS version encounters binary file | Clear error message: "cannot identify file type" from existing `is_magic_ok()` |
