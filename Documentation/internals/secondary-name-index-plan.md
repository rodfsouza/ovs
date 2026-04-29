# Plan: Linked Name→UUID Secondary Index

## Context

Queries like `name == "foo"` do full table scans — O(n) deserializing every row from disk. We need a secondary index that makes these O(1) by chaining: `name_index → UUID → bloom → offset → pread()`.

The index entry lives on the existing `disk_store_index_entry` struct (the central node), linking UUID index and name index through the same allocation. Mutations (insert/delete/update) update both indexes atomically.

OVS already provides `hash_string()` (MurmurHash3 or hardware CRC32 depending on CPU) in `lib/hash.h` — excellent distribution, minimal collision chains.

## Design: Linked Index Entries

### Extended `disk_store_index_entry`

```c
struct disk_store_index_entry {
    struct hmap_node hmap_node;    /* UUID index (existing). */
    struct uuid uuid;
    off_t offset;
    uint32_t length;
    char *table_name;
    bool deleted;

    /* Secondary name index linkage (NEW). */
    char *name_value;              /* Indexed column value, or NULL. */
    struct hmap_node name_node;    /* In name_index hmap (by name hash). */
    bool in_name_index;            /* True if name_node is inserted. */
};
```

### Name Index Structure

```c
/* Per-table secondary index on a string column. */
struct ovsdb_name_index {
    struct hmap entries;           /* name_hash → disk_store_index_entry
                                   * (via entry->name_node). */
    char *column_name;            /* Which column is indexed. */
};
```

Stored on `struct ovsdb_table`:
```c
struct ovsdb_name_index *name_index;  /* NULL if no indexed column
                                       * or no disk_store. */
```

### Lookup Chain

```
name == "foo"
  → hash_string("foo", 0)
  → name_index.entries hmap lookup
  → disk_store_index_entry (via name_node)
  → entry->uuid
  → ovsdb_table_get_row(table, &entry->uuid)
    → bloom filter check
    → cache lookup
    → pread(fd, entry->offset, entry->length)
  → row
```

All O(1). No full table scan.

---

## Implementation

### Part 1: Build Both Indexes in Single Pass at Startup

**`ovsdb/disk-store.c`** — Modify `disk_store_rebuild_index()`:

Currently reads only row headers (24 bytes + table_name). Extend to read the FULL row record (we have `total_len`) and scan for the indexed column name:

```c
static void
disk_store_rebuild_index(struct ovsdb_disk_store *store)
{
    /* ... existing header/magic validation ... */
    
    while (pos < file_size) {
        /* Read row header (existing). */
        pread(store->fd, row_header, 24, pos);
        memcpy(&uuid, row_header, sizeof uuid);
        memcpy(&total_len, row_header + 16, sizeof total_len);
        
        /* Read full record to extract name column (NEW). */
        uint8_t *record = xmalloc(total_len);
        pread(store->fd, record, total_len, pos);
        
        /* Parse table_name from record (existing, adjusted). */
        ...
        
        /* Scan for indexed column name in record (NEW). */
        char *name_value = NULL;
        if (store->indexed_column_name) {
            name_value = disk_store_extract_column_string(
                record, total_len, n_columns,
                table_name, table_name_len,
                store->indexed_column_name);
        }
        
        /* Create index entry with name linkage. */
        entry = xzalloc(sizeof *entry);
        entry->uuid = uuid;
        entry->offset = pos;
        entry->length = total_len;
        entry->table_name = xstrdup(table_name_buf);
        entry->deleted = (flags & DISK_STORE_FLAG_DELETED);
        entry->name_value = name_value;  /* NULL if not found */
        entry->in_name_index = false;
        
        hmap_insert(&store->index, &entry->hmap_node,
                    disk_store_uuid_hash(&uuid));
        
        pos += total_len;
        free(record);
    }
}
```

**New helper** `disk_store_extract_column_string()`:

Scans column headers in the record buffer, looking for a column by name. Returns the string value if found, NULL otherwise. Stops as soon as the target column is found (early exit — avoids parsing remaining columns):

```c
static char *
disk_store_extract_column_string(const uint8_t *record,
                                 uint32_t total_len,
                                 uint16_t n_columns,
                                 const char *table_name,
                                 uint16_t table_name_len,
                                 const char *target_column)
{
    size_t col_offset = DISK_STORE_ROW_HEADER_SIZE
                        + sizeof(uint16_t) + table_name_len;
    
    for (uint16_t c = 0; c < n_columns; c++) {
        /* Read column name. */
        uint16_t col_name_len = get_uint16(record + col_offset);
        col_offset += sizeof(uint16_t);
        const char *col_name = (const char *)(record + col_offset);
        col_offset += col_name_len;
        
        /* Read type tags. */
        uint8_t key_type = record[col_offset++];
        uint8_t val_type = record[col_offset++];
        
        /* Check if this is our target column. */
        if (col_name_len == strlen(target_column)
            && !memcmp(col_name, target_column, col_name_len)
            && key_type == OVSDB_TYPE_STRING) {
            /* Extract the string value. */
            uint32_t datum_n = get_uint32(record + col_offset);
            col_offset += sizeof(uint32_t);
            if (datum_n >= 1) {
                uint32_t str_len = get_uint32(record + col_offset);
                col_offset += sizeof(uint32_t);
                return xmemdup0(record + col_offset, str_len);
            }
            return NULL;
        }
        
        /* Skip this column's datum to advance to next column. */
        col_offset += disk_store_skip_datum(record + col_offset,
                                            key_type, val_type);
    }
    return NULL;
}
```

**Also need**: `disk_store_skip_datum()` — advances past a serialized datum without fully parsing it. Reads `datum_n`, then skips `datum_n` key atoms (and value atoms for maps).

### Part 2: Build Name Index hmap After UUID Index

**`ovsdb/disk-store.c`** — New function called after `rebuild_index()`:

```c
void
ovsdb_disk_store_build_name_index(struct ovsdb_disk_store *store,
                                  struct ovsdb_name_index *ni)
{
    struct disk_store_index_entry *e;
    
    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (e->deleted || !e->name_value) {
            continue;
        }
        hmap_insert(&ni->entries, &e->name_node,
                    hash_string(e->name_value, 0));
        e->in_name_index = true;
    }
}
```

**`ovsdb/ovsdb.c`** — In `ovsdb_attach_disk_store()`, after opening the disk store:

```c
/* Determine which column to index (from schema indexes). */
for (size_t i = 0; i < table->schema->n_indexes; i++) {
    struct ovsdb_column_set *idx = &table->schema->indexes[i];
    if (idx->n_columns == 1
        && idx->columns[0]->type.key.type == OVSDB_TYPE_STRING
        && idx->columns[0]->type.n_max == 1) {
        /* Tell disk store which column to extract during rebuild. */
        ovsdb_disk_store_set_indexed_column(ds, idx->columns[0]->name);
        break;  /* One name index per table for now. */
    }
}

/* Open disk store (builds UUID→offset index AND extracts
 * name values in the same pass). */
...

/* Build name index hmap from extracted values. */
if (ovsdb_disk_store_has_name_values(ds)) {
    table->name_index = ovsdb_name_index_create(
        indexed_column_name);
    ovsdb_disk_store_build_name_index(ds, table->name_index);
}
```

### Part 3: Maintain Name Index During Transactions

**`ovsdb/transaction.c`** — In `ovsdb_txn_row_commit()`, after disk store and cache updates:

```c
/* Update name index. */
if (txn_row->table->name_index && txn_row->table->disk_store) {
    struct ovsdb_name_index *ni = txn_row->table->name_index;
    
    if (txn_row->old) {
        /* Remove old name from index. */
        const struct uuid *old_uuid = ovsdb_row_get_uuid(txn_row->old);
        ovsdb_disk_store_name_index_remove(
            txn_row->table->disk_store, ni, old_uuid);
    }
    if (txn_row->new) {
        /* Add new name to index. */
        const struct ovsdb_datum *d =
            &txn_row->new->fields[ni->column_index];
        if (d->n == 1) {
            const char *name = json_string(d->keys[0].s);
            const struct uuid *new_uuid =
                ovsdb_row_get_uuid(txn_row->new);
            ovsdb_disk_store_name_index_add(
                txn_row->table->disk_store, ni,
                new_uuid, name);
        }
    }
}
```

**`ovsdb/disk-store.c`** — Name index mutation functions:

```c
void
ovsdb_disk_store_name_index_add(struct ovsdb_disk_store *store,
                                struct ovsdb_name_index *ni,
                                const struct uuid *uuid,
                                const char *name)
{
    /* Find the disk_store_index_entry by UUID. */
    struct disk_store_index_entry *e = disk_store_find_entry(store, uuid);
    if (!e) return;
    
    /* Update name_value and insert into name hmap. */
    free(e->name_value);
    e->name_value = xstrdup(name);
    if (e->in_name_index) {
        hmap_remove(&ni->entries, &e->name_node);
    }
    hmap_insert(&ni->entries, &e->name_node,
                hash_string(name, 0));
    e->in_name_index = true;
}

void
ovsdb_disk_store_name_index_remove(struct ovsdb_disk_store *store,
                                   struct ovsdb_name_index *ni,
                                   const struct uuid *uuid)
{
    struct disk_store_index_entry *e = disk_store_find_entry(store, uuid);
    if (!e) return;
    
    if (e->in_name_index) {
        hmap_remove(&ni->entries, &e->name_node);
        e->in_name_index = false;
    }
    free(e->name_value);
    e->name_value = NULL;
}
```

### Part 4: Query Path — Name Index Lookup

**`ovsdb/table.c`** — In `ovsdb_table_query()`, new Path 2:

```c
/* Path 2: Name index lookup.
 * Single-clause string equality on the indexed column. */
if (check_cond
    && condition->n_clauses == 1
    && condition->clauses[0].function == OVSDB_F_EQ
    && table->name_index
    && condition->clauses[0].column->index
       == table->name_index->column_index) {
    
    const char *target = json_string(
        condition->clauses[0].arg.keys[0].s);
    const struct uuid *uuid =
        ovsdb_name_index_find(table->name_index, target);
    
    if (uuid) {
        const struct ovsdb_row *row = ovsdb_table_get_row(table, uuid);
        if (row && ovsdb_condition_match_every_clause(row, condition)) {
            cb(row, aux);
        }
    }
    return;
}
```

**`ovsdb/disk-store.c`** — Name index find:

```c
const struct uuid *
ovsdb_name_index_find(const struct ovsdb_name_index *ni,
                      const char *name)
{
    uint32_t h = hash_string(name, 0);
    struct disk_store_index_entry *e;
    
    HMAP_FOR_EACH_WITH_HASH (e, name_node, h, &ni->entries) {
        if (!strcmp(e->name_value, name)) {
            return &e->uuid;
        }
    }
    return NULL;
}
```

### Part 5: CLI Name Condition Push

**`lib/db-ctl-base.c`** — Extend `ctl_set_uuid_condition` → `ctl_set_row_condition`:

When record_id is not a UUID, look up the table's `ctl_row_id.name_column` and build a name condition `["name","==","record_id"]`:

```c
static void
ctl_set_row_condition(struct ctl_context *ctx,
                      const struct ovsdb_idl_table_class *table,
                      int first)
{
    /* Check if all args are UUIDs (existing). */
    bool all_uuids = true;
    for (int i = first; i < ctx->argc; i++) {
        struct uuid uuid;
        if (!uuid_from_string(&uuid, ctx->argv[i])) {
            all_uuids = false;
            break;
        }
    }
    
    if (all_uuids) {
        /* UUID condition (existing code). */
        ...
    } else if (ctx->argc == first + 1) {
        /* Single non-UUID arg — try name condition. */
        const struct ctl_table_class *ctl =
            &ctl_classes[table - idl_classes];
        for (int i = 0; i < ARRAY_SIZE(ctl->row_ids); i++) {
            const struct ctl_row_id *id = &ctl->row_ids[i];
            if (id->name_column && !id->key && !id->uuid_column) {
                struct json *clause = json_array_create_3(
                    json_string_create(id->name_column->name),
                    json_string_create("=="),
                    json_string_create(ctx->argv[first]));
                struct json *cond = json_array_create_1(clause);
                ovsdb_idl_set_condition_json(ctx->idl, table, cond);
                json_destroy(cond);
                return;
            }
        }
    }
}
```

### Part 6: Monitor Condition Widening

**`ovsdb/monitor.c`** — In `ovsdb_monitor_get_initial_conditioned()`:

Widen the single-clause equality check to include any OVSDB_F_EQ (not just UUID). `ovsdb_table_query()` handles both UUID and name-indexed lookups internally:

```c
if (has_cond && !ovsdb_condition_is_true(new_cond)
    && new_cond->n_clauses == 1
    && new_cond->clauses[0].function == OVSDB_F_EQ) {
    /* Single equality — ovsdb_table_query handles UUID
     * fast-path and name index lookup internally. */
    ovsdb_table_query(table, new_cond, cb, &aux);
} else {
    ovsdb_table_for_each_row_from_disk(table, cb, &aux);
}
```

### Part 7: Compaction — Rebuild Name Index

In `ovsdb_disk_store_compact()`, the new index entries created during compaction also get `name_value` populated. After compaction, rebuild the name index hmap from the new entries.

---

## Hash Strategy

Using `hash_string()` from `lib/hash.h` which dispatches to:
- **x86-64 with SSE4.2**: Hardware CRC32 (`_mm_crc32_u32`) — fastest
- **ARM AArch64**: Hardware CRC32 instruction
- **Fallback**: MurmurHash3 (Austin Appleby, public domain)

These provide excellent distribution. OVS `hmap` uses chaining with auto-expansion at load factor 0.5, and logs warnings if any bucket exceeds 6 entries. For our use case (unique names per table), collision chains should be length 1.

---

## Startup Order

```
ovsdb_attach_disk_store():
  1. Determine indexed column from schema (table->schema->indexes)
  2. ovsdb_disk_store_open()
     → disk_store_rebuild_index()
        → Single pass: reads each row record
        → Builds UUID→offset hmap entries
        → Extracts name column value into entry->name_value
  3. Build bloom filter (from UUID index)
  4. Build name index hmap (from entries with name_value)
  5. Build row cache (UNLOADED entries)
```

UUID→offset and name extraction happen in the **same single pass** over the disk file. The name index hmap is built as a second in-memory pass over the already-created entries.

---

## Files to Create/Modify

| File | Change |
|------|--------|
| `ovsdb/disk-store.c` | Extend `disk_store_index_entry` with name fields, extract column during rebuild, add name index mutation functions, add `skip_datum` helper |
| `ovsdb/disk-store.h` | Expose name index types and functions |
| `ovsdb/table.h` | Add `name_index` field to `struct ovsdb_table` |
| `ovsdb/table.c` | Add Path 2 (name index lookup) in `ovsdb_table_query()` |
| `ovsdb/ovsdb.c` | Determine indexed column, build name index at startup |
| `ovsdb/transaction.c` | Maintain name index on insert/update/delete |
| `ovsdb/monitor.c` | Widen single-clause equality optimization |
| `lib/db-ctl-base.c` | Add name condition push in `ctl_set_row_condition()` |
| `ovsdb/automake.mk` | No new files needed — name index lives in disk-store.c |

## Tests

### Test 1: Name index lookup returns correct row
- Table with `"indexes":[["name"]]`, disk-backed
- Insert 100 rows, query `["name","==","row-42"]`
- Verify exactly 1 row returned

### Test 2: Name index maintained on insert/update/delete
- Insert name="foo" → query returns it
- Update to name="bar" → "foo" returns nothing, "bar" returns it
- Delete → "bar" returns nothing

### Test 3: Name index via monitor_cond
- Start `monitor-cond` with `["name","==","target"]`
- Verify only 1 row in initial dump

### Test 4: CLI name condition push
- `list TABLE router-name` → verify condition pushed, only matching row fetched

### Test 5: Code path convergence
- Same `name=="foo"` query via: select, monitor, idl_select, CLI
- All return identical row

### Test 6: Name index rebuilt after compaction
- Insert, delete half, compact
- Verify name index only has surviving rows

### Test 7: Regression
```bash
make check TESTSUITEFLAGS="-j4 -k ovsdb"
```

## Verification

```bash
make -j4
make check TESTSUITEFLAGS="-j4 -k 'queries on' -k 'monitor-cond' -k 'delete all'"
```

After implementation: run /simplify and /review.
