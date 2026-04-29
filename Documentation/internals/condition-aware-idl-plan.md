# Plan: Condition-Aware End-to-End IDL and OVSDB Server

## Context

CLI tools (ovn-nbctl, ovs-vsctl) download **all rows** via `monitor_cond [true]` and filter locally. On the server, multiple code paths bypass the optimized `ovsdb_query()` engine (bloom -> cache -> disk). The goal: make **every row-access path** condition-aware and route through the uniform query infrastructure.

### Server-Side Audit Results

| Path | File:Line | Current | Optimized? |
|------|-----------|---------|------------|
| SELECT | execution.c:462 | `ovsdb_query()` | YES |
| UPDATE | execution.c:549 | `ovsdb_query()` | YES |
| MUTATE | execution.c:619 | `ovsdb_query()` | YES |
| DELETE | execution.c:676 | `ovsdb_query()` | YES |
| WAIT | execution.c:793 | `ovsdb_query()` | YES |
| Monitor initial dump | monitor.c:1614 | `ovsdb_table_for_each_row_from_disk()` -- full scan | **NO** |
| Monitor cond_change | monitor.c:1274 | `ovsdb_table_for_each_row_from_disk()` -- full scan | **NO** |
| RBAC lookup | rbac.c:53 | `HMAP_FOR_EACH` on `table->rows` -- O(n) | **NO** |
| UUID lookup | table.c:370 | `ovsdb_table_get_row()` bloom->cache->disk | YES |

### Server-Side Code Duplication Audit

`ovsdb_table_for_each_row_from_disk()` (table.c:600) and `ovsdb_query()` (query.c:28) implement the **same three-step deduplication logic independently**:

| Step | `for_each_row_from_disk` | `ovsdb_query` |
|------|--------------------------|---------------|
| 1. Yield table->rows first | lines 604-610 | lines 60-69 |
| 2. Skip UUIDs already in table->rows | line 635 `table_rows_contains()` | lines 86-92 `HMAP_FOR_EACH_WITH_HASH` |
| 3. Insert disk rows into cache | lines 662-674 | lines 100-101 |

The only difference: `ovsdb_query` applies condition matching (line 94). This is ~100 lines of duplicated logic that must be unified.

---

## Phase 0: Server Refactor -- Unify Row Iteration into `ovsdb_table_query()`

### Problem
Two functions implement the same disk -> memory -> cache iteration with deduplication:
- `ovsdb_table_for_each_row_from_disk()` (table.c:600) -- unconditional, used by monitor.c
- `ovsdb_query()` (query.c:28) -- conditional, used by execution.c

Callers must choose between "conditioned" and "unconditioned" variants that share 80% of their code. This is fragile and makes it impossible to transparently add condition support to monitor paths.

### Design

Introduce `ovsdb_table_query()` in table.c as the **single entry point** for all conditioned row iteration. The existing functions become thin wrappers or are deprecated.

**Keep separate (different semantics):**
- `ovsdb_table_get_row()` -- UUID-only direct lookup (bloom -> cache -> disk). Not an iteration.
- `ovsdb_table_for_each_loaded_row()` -- stable pointers, cache-only, no disk walk. Different lifetime contract.
- `ovsdb_table_for_each_row()` -- disk-cursor-only iteration (no dedup, no cache insert). Used by background threads (compaction) and ovsdb-tool.

### Changes

**`ovsdb/table.h`** -- New unified function:
```c
/* Iterate rows matching 'condition'.  If condition is NULL or empty,
 * yields all rows.  Routes through the optimized bloom -> cache -> disk
 * path.  Row pointers are TRANSIENT (same contract as
 * ovsdb_table_for_each_row_from_disk). */
void ovsdb_table_query(struct ovsdb_table *table,
                       const struct ovsdb_condition *condition,
                       ovsdb_table_row_cb cb, void *aux);
```

**`ovsdb/table.c`** -- Implementation:
- Move the unified iteration logic from `ovsdb_query()` into `ovsdb_table_query()`
- Two execution paths:
  1. **UUID exact-match**: if condition has `_uuid == <uuid>` clause, call `ovsdb_table_get_row()` directly (bloom -> cache -> disk)
  2. **Unified iteration** (single code path for both disk-backed and in-memory tables):
     - Always yield `table->rows` with condition check
     - If `table->disk_store` exists: walk disk cursor with condition + dedup vs table->rows + selective cache insertion
     - If no disk_store: iteration ends after table->rows (the disk section is simply never entered)
- `NULL` condition or `ovsdb_condition_is_true()` -> skips condition checks, yields all rows (equivalent to current `for_each_row_from_disk`)

**`ovsdb/table.c`** -- Deprecate `ovsdb_table_for_each_row_from_disk()`:
```c
/* Deprecated: use ovsdb_table_query(table, NULL, cb, aux) instead. */
void
ovsdb_table_for_each_row_from_disk(const struct ovsdb_table *table,
                                   ovsdb_table_row_cb cb, void *aux)
{
    ovsdb_table_query(CONST_CAST(struct ovsdb_table *, table),
                      NULL, cb, aux);
}
```

**`ovsdb/query.c`** -- Simplify `ovsdb_query()` to delegate:
```c
void
ovsdb_query(struct ovsdb_table *table, const struct ovsdb_condition *cnd,
            bool (*output_row)(const struct ovsdb_row *, void *aux), void *aux)
{
    ovsdb_table_query(table, cnd, output_row, aux);
}
```
Keep `ovsdb_query()` as the public API for callers in execution.c that already use it. Both names route to the same implementation.

**`ovsdb/query.h`** -- Keep existing API unchanged (callers don't need to change).

### After Refactor: Single Code Path

```
                    ovsdb_table_query(table, condition, cb, aux)
                              |
              +---------------+---------------+
              |                               |
        UUID == match              Unified iteration
              |                               |
       get_row(uuid)              1. yield table->rows
              |                      (with condition check)
        bloom -> cache -> disk    2. if disk_store:
                                     walk cursor + dedup
                                     + condition check
                                     + selective cache insert
                                  3. (no disk_store = done after step 1)
```

No branching between "disk-backed" and "in-memory" — same code path, the disk section is simply never entered when `table->disk_store` is NULL.

All callers (execution.c, monitor.c, rbac.c, future IDL select) go through this single path.

### Tests -- `tests/ovsdb-query.at` (new) and existing tests

**Test 0a: ovsdb_table_query with NULL condition equals full scan**
```
AT_SETUP([table query NULL condition])
AT_KEYWORDS([ovsdb query])
```
- Insert 20 rows, call `ovsdb_table_query(table, NULL, cb, aux)`
- Verify all 20 rows yielded (identical to `for_each_row_from_disk`)

**Test 0b: ovsdb_table_query with condition filters correctly**
```
AT_SETUP([table query with condition])
AT_KEYWORDS([ovsdb query])
```
- Insert 20 rows, query with `[["i","<","5"]]`
- Verify only 5 rows returned

**Test 0c: ovsdb_table_query UUID fast-path**
```
AT_SETUP([table query UUID optimization])
AT_KEYWORDS([ovsdb query])
```
- Insert rows, query with `[["_uuid","==",<uuid>]]`
- Verify exactly 1 row, and it goes through `ovsdb_table_get_row()` (bloom -> cache -> disk)

**Test 0d: Regression -- all existing execution.c tests still pass**
- Run `make check -k "ovsdb"` to verify no regressions in SELECT, UPDATE, MUTATE, DELETE, WAIT

**Test 0e: ovsdb_table_query with disk-store and condition-aware caching**
```
AT_SETUP([table query disk-store condition caching])
AT_KEYWORDS([ovsdb query disk-store])
```
- Start with `--disk-store`, insert 50 rows
- Query with condition matching 3 rows
- Verify only 3 rows cached (non-matching rows not polluting cache)

---

## Phase 1: Server -- Condition-Aware Monitor Initial Dump

### Problem
`ovsdb_monitor_get_initial()` (monitor.c:1589) calls `ovsdb_table_for_each_row_from_disk()` -- a full scan. With Phase 0 done, we can simply call `ovsdb_table_query()` with the session's condition.

### Constraint
`dbmon->init_change_set` is cached and shared across sessions. Different sessions may have different conditions.

### Changes

**`ovsdb/monitor.c`**

1. New function: `ovsdb_monitor_get_initial_conditioned()`
```c
void
ovsdb_monitor_get_initial_conditioned(
    struct ovsdb_monitor *dbmon,
    const struct ovsdb_monitor_session_condition *condition,
    struct ovsdb_monitor_change_set **p_mcs)
```
- Allocates a **per-session** change set (NOT cached on `dbmon->init_change_set`)
- For each monitored table with `(mcst->mt->select & OJMS_INITIAL)`:
  - Extract session's `ovsdb_condition` via `ovsdb_monitor_get_table_conditions()`
  - If condition is trivial (`ovsdb_condition_is_true()`): call `ovsdb_table_query(table, NULL, cb, aux)` (existing full-scan path)
  - If condition is non-trivial: call `ovsdb_table_query(table, cond, cb, aux)` -- same function, just with a condition
- The callback is `monitor_initial_row_cb()` (unchanged) which feeds `ovsdb_monitor_changes_update()`

**Symmetry note**: Both conditioned and unconditioned paths now call `ovsdb_table_query()`. The only difference is the condition parameter.

**`ovsdb/monitor.h`**
- Expose `ovsdb_monitor_get_initial_conditioned()`

**`ovsdb/jsonrpc-server.c`**
- In `ovsdb_jsonrpc_monitor_create()` (~line 1620): when `m->condition && m->condition->conditional`, call `ovsdb_monitor_get_initial_conditioned()` instead of `ovsdb_monitor_get_initial()`
- In deferred completion path `ovsdb_jsonrpc_monitor_complete_deferred()`: same dispatch

### Tests -- `tests/ovsdb-monitor.at`

**Test 1: Conditioned initial dump returns only matching rows**
```
AT_SETUP([monitor-cond initial dump filtered])
AT_KEYWORDS([ovsdb server monitor monitor-cond])
```
- Insert 100 rows with name "row-0" through "row-99"
- Start `monitor-cond` with condition `[["name","==","row-42"]]`
- Verify initial dump contains exactly 1 row (row-42)
- Verify no `insert` notifications for non-matching rows

**Test 2: Conditioned initial dump with UUID equality**
```
AT_SETUP([monitor-cond initial dump UUID filter])
AT_KEYWORDS([ovsdb server monitor monitor-cond])
```
- Insert 3 rows, capture UUID of the second
- Start `monitor-cond` with `[["_uuid","==",["uuid","<captured>"]]]`
- Verify only 1 row in initial dump
- This exercises the UUID fast-path (bloom -> cache -> disk)

**Test 3: Unconditioned monitor still shares cached init_change_set**
```
AT_SETUP([monitor-cond unconditional shares init_change_set])
AT_KEYWORDS([ovsdb server monitor monitor-cond])
```
- Insert rows, start two monitors with condition `[true]`
- Verify both see all rows (shared cache path)
- Verify with `ovs-appctl ovsdb-server/memory` that only one change set is allocated

**Test 4: Conditioned initial dump with disk-store backend**
```
AT_SETUP([monitor-cond initial dump with disk-store])
AT_KEYWORDS([ovsdb server monitor monitor-cond disk-store])
```
- Start server with `--disk-store`
- Insert 50 rows, let them flush to disk
- Start `monitor-cond` with `[["name","==","target"]]`
- Verify only matching row returned

**Test 5: Mixed conditions across tables**
```
AT_SETUP([monitor-cond per-table conditions])
AT_KEYWORDS([ovsdb server monitor monitor-cond])
```
- Monitor two tables: one with condition, one with `[true]`
- Verify conditioned table returns only matches, unconditioned returns all

---

## Phase 2: Server -- Condition-Aware Monitor Condition Change

### Problem
`ovsdb_monitor_compose_cond_change_update()` (monitor.c:1274) does a full table scan via `ovsdb_table_for_each_row_from_disk()` when conditions change at runtime.

### Changes

**`ovsdb/monitor.c`**

Replace `ovsdb_table_for_each_row_from_disk()` at line 1274 with `ovsdb_table_query()` using the `new_condition`. The callback `cond_change_row_cb()` already checks old_condition vs new_condition to determine INSERT/DELETE/MODIFY -- this logic stays. The improvement: `ovsdb_table_query(table, new_condition, ...)` only yields rows matching the new condition, so the callback doesn't need to evaluate `new_cond` (it's guaranteed true). It only needs to check `!old_cond` to determine INSERTs vs MODIFYs.

For departing rows (matched old but not new): iterate the monitor's tracked change set `mcst->rows` and check `!ovsdb_condition_match_any(row, new_condition)` -- these are DELETEs. This is already in-memory (no disk I/O).

### Tests -- `tests/ovsdb-monitor.at`

**Test 6: Condition change uses optimized path**
```
AT_SETUP([monitor-cond-change filtered scan])
AT_KEYWORDS([ovsdb server monitor monitor-cond])
```
- Insert 100 rows with values 0-99
- Start `monitor-cond` with `[["i","<","10"]]` (10 rows)
- Change condition to `[["i","<","20"]]`
- Verify: 10 new INSERT notifications (rows 10-19)
- Verify: no spurious DELETEs for rows 0-9

**Test 7: Condition change from broad to narrow**
```
AT_SETUP([monitor-cond-change narrow])
AT_KEYWORDS([ovsdb server monitor monitor-cond])
```
- Start with `[["i","<","50"]]`, change to `[["i","<","5"]]`
- Verify: 45 DELETE notifications (rows 5-49), rows 0-4 unchanged

---

## Phase 3: Server -- RBAC Optimized Lookup

### Problem
`ovsdb_find_row_by_string_key()` in rbac.c:42 does O(n) linear scan on `table->rows` with `HMAP_FOR_EACH`. Has an `XXX` comment acknowledging the issue. Bypasses disk-store entirely.

### Changes

**`ovsdb/rbac.c`** -- Replace with `ovsdb_query_row_set()`:
```c
static const struct ovsdb_row *
ovsdb_find_row_by_string_key(const struct ovsdb_table *table,
                             const char *column_name,
                             const char *key)
{
    const struct ovsdb_column *column =
        ovsdb_table_schema_get_column(table->schema, column_name);
    if (!column) {
        return NULL;
    }

    struct ovsdb_datum arg;
    ovsdb_datum_init_string(&arg, key);

    struct ovsdb_condition cnd;
    ovsdb_condition_init(&cnd);
    ovsdb_condition_add_clause(&cnd, OVSDB_F_INCLUDES, column, &arg);

    struct ovsdb_row_set results = OVSDB_ROW_SET_INITIALIZER;
    ovsdb_query_row_set(CONST_CAST(struct ovsdb_table *, table),
                        &cnd, &results);

    const struct ovsdb_row *found = results.n_rows > 0
                                    ? results.rows[0] : NULL;
    ovsdb_row_set_destroy(&results);
    ovsdb_condition_destroy(&cnd);
    ovsdb_datum_destroy(&arg, &column->type);
    return found;
}
```
Now goes through `ovsdb_query_row_set()` -> `ovsdb_query()` -> `ovsdb_table_query()` -- the same path as everything else.

### Tests

**Test 8: RBAC lookup through query path**
```
AT_SETUP([rbac lookup uses query path])
AT_KEYWORDS([ovsdb server rbac])
```
- Create RBAC role and permission entries
- Verify RBAC authorization still works correctly (regression test)

---

## Phase 4: Client -- `ovsdb_idl_select()` Transact-Based Query API

### Problem
CLI tools use monitor-based IDL which syncs everything. A `transact` -> `select` RPC goes directly through `ovsdb_execute_select()` -> `ovsdb_query()` -> `ovsdb_table_query()`.

### Changes

**`lib/ovsdb-idl.h`** -- New public API:
```c
struct ovsdb_idl_select_result {
    struct json **rows;
    size_t n_rows;
};

void ovsdb_idl_select_result_destroy(struct ovsdb_idl_select_result *);

int ovsdb_idl_select(struct ovsdb_idl *,
                     const struct ovsdb_idl_table_class *,
                     const struct ovsdb_idl_condition *,
                     const struct ovsdb_idl_column **columns,
                     size_t n_columns,
                     struct ovsdb_idl_select_result *result);
```

**`lib/ovsdb-idl.c`** -- Implementation:
1. Build JSON: `{"op":"select", "table":"<name>", "where":<cond>, "columns":[...]}`
2. Wrap in transact: `["<db_name>", <select_op>]`
3. Send as `"transact"` JSON-RPC request
4. Block-wait for response
5. Parse `result[0].rows` array

**`lib/ovsdb-cs.h` / `lib/ovsdb-cs.c`** -- Synchronous RPC helper:
```c
struct jsonrpc_msg *ovsdb_cs_send_request_block(
    struct ovsdb_cs *, struct jsonrpc_msg *request,
    long long int timeout_msec);
```

### Tests -- `tests/ovsdb-idl.at`

**Test 9: IDL select by UUID** -- 1 row returned from 10
**Test 10: IDL select by string** -- matching rows from diverse set
**Test 11: IDL select with column projection** -- only requested columns
**Test 12: IDL select no matches** -- 0 rows, no error
**Test 13: IDL select all rows** -- empty/true condition
**Test 14: IDL select with disk-store** -- exercises bloom -> cache -> disk

**`tests/test-ovsdb.c`** -- New `do_idl_select()` test command.

---

## Phase 5: Client -- Wire `db-ctl-base.c` to Server-Side Select

### Problem
`ctl_get_row()`, `cmd_find()`, and `get_row_by_id()` iterate all IDL rows locally.

### Changes

**`lib/db-ctl-base.c`**

1. **`cmd_find()` (line 1281)** -- Primary target:
   - Parse user conditions into `ovsdb_idl_condition`
   - Call `ovsdb_idl_select()` with the condition
   - Format results from JSON directly
   - Fallback to local iteration on error

2. **`ctl_get_row()` (line 366)** -- UUID/name lookups:
   - UUID: condition `[["_uuid","==",uuid]]`
   - Name: condition on name column
   - Partial UUID: keep local scan (no server-side partial match)

3. **New helpers:**
   - `ctl_add_condition_clause()` -- translate CLI condition to IDL clause
   - `list_record_from_json()` -- format JSON row into output table

### Write-Command Handling
- `ovsdb_idl_select()` finds UUID server-side
- IDL row from monitor sync used for transaction prereqs
- Transaction `wait` ops provide safety net

### Tests -- `tests/ovsdb-idl.at`

**Test 15: CLI find server-side** -- correct rows from server
**Test 16: CLI get by UUID** -- single row via select
**Test 17: CLI get by name** -- single row via select
**Test 18: CLI find no matches** -- empty output, no error
**Test 19: CLI find fallback** -- old server, local iteration
**Test 20: CLI find with disk-store** -- full optimized path

---

## Phase 6: Homogeneity Verification

### Remaining Direct-Access Audit

After Phases 0-5, verify these are the ONLY remaining direct accesses:

| Path | File:Line | Acceptable? | Reason |
|------|-----------|-------------|--------|
| `ovsdb_relay_clear()` | relay.c:300 | YES | Full table clear |
| `reset_database()` | replication.c:536 | YES | Sync reset |
| `ovsdb_table_destroy()` | table.c:340 | YES | Shutdown cleanup |
| `ovsdb_convert_table()` | file.c:321 | YES | Schema migration |
| `query_db_string()` | ovsdb-server.c:1425 | YES | Bootstrap on small tables |
| `query_db_remotes()` | ovsdb-server.c:1544 | YES | Startup on small tables |
| `ovsdb_to_txn_json()` | file.c:463 | YES | Full serialization |
| `ovsdb_table_for_each_row()` | table.c:462 | YES | Background threads, no cache |
| `ovsdb_table_for_each_loaded_row()` | table.c:546 | YES | Cache-only, stable pointers |

### After-Refactor Server Path Diagram

```
All server-side row access:

  execution.c (SELECT/UPDATE/MUTATE/DELETE/WAIT)
  monitor.c   (initial dump, cond_change)
  rbac.c      (permission lookup)
       |
       v
  ovsdb_query() / ovsdb_query_row_set() / ovsdb_query_distinct()
       |
       v
  ovsdb_table_query(table, condition, cb, aux)    <-- SINGLE ENTRY POINT
       |
       +---> UUID == match --> ovsdb_table_get_row() --> bloom -> cache -> disk
       |
       +---> Unified iteration (one path, works for all tables):
             1. yield table->rows (condition-filtered)
             2. if disk_store: walk cursor (condition-filtered, dedup, selective cache)
             3. no disk_store = done after step 1
```

### Tests

**Test 21: End-to-end condition-aware path verification**
```
AT_SETUP([condition-aware end-to-end])
AT_KEYWORDS([ovsdb server monitor idl disk-store])
```
- Start server with `--disk-store`, insert 500 rows
- **Monitor**: `monitor-cond` with `[["name","==","target"]]` -> 1 row
- **Select**: `ovsdb_idl_select()` with same condition -> 1 row
- **Transact**: `transact select` with same condition -> 1 row
- **CLI**: `find` with same condition -> 1 row
- All four paths return identical results

**Test 22: No regressions with unconditioned access**
```
AT_SETUP([unconditioned backward compatibility])
AT_KEYWORDS([ovsdb server monitor idl])
```
- Start server (no disk-store), insert rows
- Monitor `[true]` -> all rows
- Select empty condition -> all rows
- `find` no conditions -> all rows

---

## Future Improvement: Secondary Name->UUID Index

### Current Gap

`ovsdb_disk_store_cursor_open()` (disk-store.c:1180) does 2x `HMAP_FOR_EACH` over ALL index entries to build the cursor, even when the subsequent condition filtering will discard most rows. For non-UUID conditions (e.g., `name == "br0"`), this is O(n) cursor setup + O(n) condition evaluation, reading and deserializing every row from disk.

The UUID->offset index only helps for `_uuid ==` conditions (where `ovsdb_table_get_row()` provides O(1) lookup). Non-UUID column conditions have no index to exploit.

### Proposed Improvement: Name->UUID Secondary Index

Add a secondary in-memory index mapping commonly-queried string columns (e.g., `name`) to their row UUID:

```
name -> UUID -> bloom filter -> UUID->offset -> pread()
```

**Lookup chain:**
1. `name_index_lookup("br0")` -> UUID (O(1) hmap lookup)
2. `ovsdb_bloom_filter_may_contain(bloom, uuid)` -> confirm existence (O(1))
3. `disk_store_find_entry(uuid)` -> `(offset, length)` (O(1) hmap lookup)
4. `pread(fd, record, length, offset)` -> row data (O(1) disk seek)

**Total: O(1) for name-based lookups instead of O(n) cursor scan.**

### Design Sketch

```c
/* In ovsdb/table.h or a new ovsdb/secondary-index.h */
struct ovsdb_name_index {
    struct hmap by_name;     /* string hash -> UUID mapping */
    const struct ovsdb_column *column;  /* indexed column */
};

struct ovsdb_name_index_entry {
    struct hmap_node hmap_node;
    char *name;              /* column value (string key) */
    struct uuid uuid;        /* row UUID */
};
```

**Integration points:**
- Built during startup (same pass that builds bloom filter and UUID->offset index)
- Updated on `ovsdb_disk_store_write_row()` and `ovsdb_disk_store_delete_row()`
- Used by `ovsdb_table_query()` when condition is `column == <string>` on an indexed column
- Falls back to cursor scan for non-indexed columns

**Scope:** This is a separate effort from the condition-aware plan. It can be implemented after Phase 6 as an optimization layer. The current plan's unified `ovsdb_table_query()` provides the correct hook point — the UUID fast-path check (path 1) can be extended to also check secondary indexes before falling through to cursor iteration.

### Which columns to index

Schema tables already declare indexes via `"indexes"` in `.ovsschema`. The `name` column is the most common lookup key across OVS/OVN tables. Candidates:
- `Bridge.name`, `Port.name`, `Interface.name` (vswitch.ovsschema)
- `Logical_Switch.name`, `Logical_Router.name` (ovn-nb.ovsschema)
- `Chassis.name`, `Port_Binding.logical_port` (ovn-sb.ovsschema)

---

## Phase 7: Quality Gates

### 7a: Code Simplification (`/simplify`)
After each phase completes:
- Run code simplification review on changed files
- Eliminate dead code from deprecated paths
- Ensure no unnecessary abstractions were introduced
- Verify function signatures are minimal (no unused parameters)

### 7b: Code Review (`/review`)
Before merging each phase:
- 5-axis review: correctness, readability, architecture, security, performance
- Verify thread safety (table.c functions are main-thread only; `ovsdb_table_get_row()` uses rwlock)
- Verify memory ownership (transient pointer contract, cache ownership transfer)
- Verify condition lifetime (conditions must outlive query call)
- Check for condition injection attacks (conditions from client JSON-RPC)

### 7c: Full Test Suite
```bash
make check TESTSUITEFLAGS="-j4 -k ovsdb"   # OVSDB tests
make check TESTSUITEFLAGS="-j4 -k monitor"  # Monitor tests
make check TESTSUITEFLAGS="-j4 -k idl"      # IDL tests
make check TESTSUITEFLAGS="-j4"             # Full suite
```

---

## Implementation Order

```
Phase 0 (refactor)
    |
    +---> Phase 1 (monitor initial) ---> Phase 2 (monitor cond_change)
    |                                          |
    +---> Phase 3 (RBAC)                       |
    |                                          |
    +---> Phase 4 (IDL select API) ---> Phase 5 (wire CLI) ---> Phase 6 (verify)
                                                                      |
                                                                Phase 7 (quality)
```

1. **Phase 0** -- Unify `ovsdb_table_query()` (prerequisite for all others)
2. **Phase 1** -- Conditioned monitor initial dump
3. **Phase 3** -- RBAC (small, self-contained)
4. **Phase 2** -- Monitor cond_change
5. **Phase 4** -- `ovsdb_idl_select()` API
6. **Phase 5** -- Wire CLI tools
7. **Phase 6** -- Homogeneity verification
8. **Phase 7** -- Simplify + Review + Full test suite

## Key Files

| File | Phases | Changes |
|------|--------|---------|
| `ovsdb/table.h` | 0 | New `ovsdb_table_query()` declaration |
| `ovsdb/table.c` | 0 | Unified query implementation; deprecate `for_each_row_from_disk` |
| `ovsdb/query.c` | 0 | Delegate to `ovsdb_table_query()` |
| `ovsdb/monitor.c` | 1, 2 | Condition-aware initial dump and cond_change |
| `ovsdb/monitor.h` | 1 | Expose new function |
| `ovsdb/jsonrpc-server.c` | 1 | Route conditioned sessions |
| `ovsdb/rbac.c` | 3 | Replace O(n) scan with `ovsdb_query()` |
| `lib/ovsdb-idl.h` | 4 | `ovsdb_idl_select()` API |
| `lib/ovsdb-idl.c` | 4 | Implement select |
| `lib/ovsdb-cs.h` / `.c` | 4 | Synchronous RPC helper |
| `lib/db-ctl-base.h` | 5 | New helpers |
| `lib/db-ctl-base.c` | 5 | Wire cmd_find, ctl_get_row |
| `tests/test-ovsdb.c` | 0, 4, 5 | Test harness |
| `tests/ovsdb-monitor.at` | 1, 2 | Monitor condition tests |
| `tests/ovsdb-idl.at` | 4, 5, 6 | IDL select and CLI tests |
