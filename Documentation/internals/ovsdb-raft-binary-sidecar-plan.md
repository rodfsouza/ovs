# Plan: Binary Format Support for OVSDB Raft (Clustered) Mode

## Context

All disk-store, row-cache, worker-pool, and lazy-loading infrastructure exists and works for **standalone** databases (Phases 0-2 complete). The README explicitly states: *"Clustered (Raft) mode is NOT supported with binary format"* and describes a future *"local cache architecture"* where Raft continues to replicate JSON while each node materializes its local copy into binary.

This plan implements that architecture. The key idea:

- **Raft log remains the source of truth** for consensus and replication (JSON, unchanged protocol)
- Each node maintains a **binary sidecar file** (`<db>.binstore`) as a local materialized view
- On startup, load from the sidecar instead of replaying the full Raft JSON log
- Periodically flush in-memory state to the sidecar (by change count or time)
- The sidecar is **advisory, never authoritative** — if missing/stale/corrupt, fall back to standard Raft replay

This removes the limitation documented in the README without changing the Raft wire protocol.

---

## Design Decisions

### 1. Storage Abstraction: Raft + Disk Store Coexistence

Currently `struct ovsdb_storage` treats `log`, `raft`, and `ds` as mutually exclusive. The change: allow `raft` and `ds` to be non-null simultaneously. When both are set, all reads/writes go through Raft; the `ds` is used only for fast startup and periodic flush.

### 2. Sidecar File Naming

For a clustered database at `/path/to/db.db`, the sidecar lives at `/path/to/db.db.binstore`. No collision with the Raft log file.

### 3. Raft Watermark in Binary Header

The BINARYV1 header (currently 36 bytes, 4 bytes reserved) is extended to store:
- `raft_applied_index` (uint64_t) — Raft log index up to which this store is current
- `raft_applied_eid` (uuid, 16 bytes) — entry ID of the last applied entry

This watermark validates the sidecar against Raft state at startup.

### 4. Flush Trigger Policy

Flush when **either** condition is met:
- `changes_since_flush >= 1000` (configurable)
- `time_since_last_flush >= 5 minutes` (configurable)

Flush runs in a background thread (same pattern as `ovsdb_snapshot()` / `compaction_thread()`): clone the database on the main thread, write the clone to the sidecar in the background via `ovsdb_disk_store_write_db()`.

### 5. Startup Validation

1. Open Raft log normally
2. Probe for `<db>.binstore`; if absent, fall back to standard Raft replay
3. Read sidecar header, extract `raft_applied_index` and `raft_applied_eid`
4. Validate: index is between `raft->log_start - 1` and `raft->commit_index`, and EID matches
5. If valid: load from sidecar, replay only Raft entries from `applied_index + 1` onward
6. If invalid: log warning, delete stale sidecar, full Raft replay

---

## Implementation Steps

### Step 1: Extend BINARYV1 Header with Raft Watermark

**Files:** `ovsdb/disk-store.c`, `ovsdb/disk-store.h`

- Change `DISK_STORE_HEADER_SIZE` from 36 to 60 (add 24 bytes: 8 for uint64_t index + 16 for UUID)
- Add `raft_applied_index` and `raft_applied_eid` fields to `struct ovsdb_disk_store`
- Update `disk_store_write_header()` / `disk_store_read_header()` to read/write these fields
- Backward compat: if file is old 36-byte header, treat watermark as 0/UUID_ZERO
- New public API:
  - `ovsdb_disk_store_get_applied_index(ds)` → uint64_t
  - `ovsdb_disk_store_get_applied_eid(ds)` → const struct uuid *
  - `ovsdb_disk_store_set_watermark(ds, index, eid)` → writes header fields

### Step 2: Add `ovsdb_disk_store_write_db()` — Full Database Dump

**Files:** `ovsdb/disk-store.c`, `ovsdb/disk-store.h`

New function that writes an entire `struct ovsdb *` to a binary sidecar file:

```c
struct ovsdb_error *ovsdb_disk_store_write_db(
    const char *filename,
    const struct ovsdb *db,
    uint64_t applied_index,
    const struct uuid *applied_eid);
```

Implementation:
1. Create temp file `<filename>.tmp`
2. Write header with schema hash + watermark
3. Iterate all tables and rows, serialize each via existing `disk_store_serialize_row()`
4. `fsync()` + atomic rename over the existing file
5. Similar to existing `ovsdb_disk_store_compact()` but reads from in-memory `ovsdb *`

### Step 3: Modify Storage Abstraction for Dual Mode

**Files:** `ovsdb/storage.c`, `ovsdb/storage.h`

- Allow `storage->raft` and `storage->ds` to coexist (remove mutual exclusivity assumption)
- New functions:
  - `ovsdb_storage_attach_binstore(storage, path, schema)` — opens/creates sidecar, sets `storage->ds`
  - `ovsdb_storage_has_binstore(storage)` → bool (true when `raft && ds`)
  - `ovsdb_storage_binstore_valid(storage, *index, *eid)` → bool (reads watermark, validates against Raft state)
- Update `ovsdb_storage_get_model()` — when `raft && ds`, still return "clustered"
- Update `ovsdb_storage_close()` — already handles `ds` cleanup, just verify

### Step 4: Add `raft_set_applied_index()` for Fast-Forward

**Files:** `ovsdb/raft.c`, `ovsdb/raft.h`

New function to advance the Raft applied position after loading from sidecar:

```c
void raft_set_applied_index(struct raft *raft, uint64_t index,
                            const struct uuid *eid);
```

Sets `raft->last_applied = index` so `raft_next_entry()` starts from `index + 1`. Validates that `index >= raft->log_start - 1` and `index <= raft->commit_index`.

### Step 5: Create Flush State Tracking

**Files:** New `ovsdb/binstore-flush.c`, `ovsdb/binstore-flush.h`; modify `ovsdb/ovsdb.h`

New structure and lifecycle:

```c
struct ovsdb_binstore_flush_state {
    uint64_t changes_since_flush;
    long long last_flush_time_msec;
    uint64_t last_flushed_index;
    bool flush_in_progress;
    /* Background thread state (same pattern as compaction_thread) */
    pthread_t thread;
    struct seq *done_seq;
    uint64_t done_seqno;
    /* Configurable thresholds */
    uint64_t change_threshold;       /* Default: 1000 */
    long long time_threshold_msec;   /* Default: 300000 */
};
```

API:
- `ovsdb_binstore_flush_init(db, change_threshold, time_threshold_msec)`
- `ovsdb_binstore_flush_destroy(db)`
- `ovsdb_binstore_flush_notify_commit(db)` — increments `changes_since_flush`
- `ovsdb_binstore_flush_should_run(db)` → bool
- `ovsdb_binstore_flush_run(db)` — clones DB, spawns background writer thread
- `ovsdb_binstore_flush_ready(db)` → bool (check if background flush done)
- `ovsdb_binstore_flush_finish(db)` — join thread, clean up
- `ovsdb_binstore_flush_wait(db)` — register with poll loop

Add `struct ovsdb_binstore_flush_state *binstore_flush` to `struct ovsdb`.

### Step 6: Implement Fast Startup Path

**Files:** `ovsdb/ovsdb-server.c`

In `open_db()`, after Raft storage is opened, when `disk_store_enabled`:

1. Compute sidecar path: `xasprintf("%s.binstore", db->filename)`
2. Probe with `ovsdb_disk_store_is_binary(binstore_path)`
3. If found, attach via `ovsdb_storage_attach_binstore()`
4. Validate watermark via `ovsdb_storage_binstore_valid()`
5. If valid:
   - Load DB from sidecar using `ovsdb_attach_disk_store()` (existing function)
   - Fast-forward Raft via `raft_set_applied_index()`
   - Log: "fast startup from binary store (applied_index=N)"
6. If invalid: log warning, delete stale file, proceed with normal Raft replay
7. Initialize flush state regardless: `ovsdb_binstore_flush_init()`

### Step 7: Hook Transaction Commits + Main Loop

**Files:** `ovsdb/ovsdb-server.c`

- After each successful `ovsdb_txn_replay_commit()` in `read_db()` / `parse_txn()`, call `ovsdb_binstore_flush_notify_commit(db->db)` if flush state exists
- In `main_loop()`, after trigger processing, add:
  ```
  if flush_should_run → flush_run (spawn background thread)
  if flush_ready → flush_finish (join thread)
  flush_wait (register with poll)
  ```
- Guard: do NOT flush while a Raft snapshot/compaction is in progress (avoid double memory amplification from two concurrent clones)

### Step 8: Coordinate with Raft Snapshots

**Files:** `ovsdb/ovsdb.c`

After a successful `ovsdb_storage_store_snapshot()`, if `db->binstore_flush` exists:
- Force a flush (`changes_since_flush = UINT64_MAX`) to keep the sidecar's watermark valid relative to the new `log_start`
- If the Raft log gets truncated past the sidecar's watermark, the sidecar becomes unvalidatable

### Step 9: Error Handling and Recovery

**Files:** `ovsdb/ovsdb-server.c`, `ovsdb/binstore-flush.c`

| Scenario | Response |
|----------|----------|
| Corrupt sidecar at startup | Log warning, delete file, full Raft replay. Next flush recreates it. |
| Schema hash mismatch | Same as corrupt — delete and rebuild. |
| Flush write failure | Log error, reset `flush_in_progress`, retry at next threshold. |
| Schema change applied | Delete sidecar, reset flush state. Next flush writes new schema. |
| Node rejoining after long absence | If `bs_index < raft->log_start - 1`, sidecar is stale — delete and full replay. |

### Step 10: Update `--disk-store` Flag for Clustered Awareness

**Files:** `ovsdb/ovsdb-server.c`

- For standalone databases: behavior unchanged (opens BINARYV1 directly)
- For clustered databases: activates binary sidecar mode
- No new flags needed

### Step 11: Add Unixctl Commands

**Files:** `ovsdb/ovsdb-server.c`

- `ovsdb/binstore/status <db>` — path, applied_index, changes_since_flush, last_flush_time
- `ovsdb/binstore/flush <db>` — force immediate flush
- `ovsdb/binstore/set-thresholds <db> <changes> <time_ms>` — adjust at runtime

### Step 12: Update Documentation

**Files:** `Documentation/internals/ovsdb-disk-backed-storage-README.md`

- Remove the "Clustered (Raft) mode is NOT supported" limitation
- Add new section: "Using Binary Sidecar with Clustered Databases"
- Document sidecar file, flush thresholds, fast startup behavior, unixctl commands

### Step 13: Tests

**Files:** New `tests/ovsdb-raft-binstore.at`, modify `tests/ovsdb.at`

Test cases:
1. **Sidecar creation**: Start clustered DB with `--disk-store`, verify `.binstore` file is created after flush threshold
2. **Fast startup**: Create cluster, write data, stop node, restart — verify log shows "fast startup from binary store"
3. **Stale sidecar recovery**: Corrupt/truncate sidecar, restart — verify fallback to Raft replay
4. **Schema change invalidation**: Apply schema change, verify sidecar is rebuilt
5. **Flush thresholds**: Set low thresholds, verify flush triggers after N transactions
6. **Unixctl status**: Query `ovsdb/binstore/status`, verify output

### Step 14: Update `ovsdb/automake.mk`

Add `binstore-flush.c` and `binstore-flush.h` to `libovsdb_la_SOURCES`.

---

## Dependency Graph

```
Step 1 (header extension)
  └─> Step 2 (write_db API)
        └─> Step 3 (storage dual mode) ──> Step 4 (raft_set_applied_index)
              └─> Step 5 (flush state)        │
                    └─> Step 6 (fast startup) <┘
                          └─> Step 7 (main loop hooks)
                                └─> Step 8 (snapshot coordination)
                                      └─> Step 9 (error handling)
                                            └─> Step 10 (--disk-store flag)
                                                  ├─> Step 11 (unixctl)
                                                  ├─> Step 12 (docs)
                                                  ├─> Step 13 (tests)
                                                  └─> Step 14 (automake)
```

## Thread Safety

The flush uses the same pattern as `compaction_thread()`:
- Main thread clones the database via `ovsdb_clone_data()`
- Background thread operates exclusively on the clone — no shared state
- Background thread writes to a temp file, then atomic rename
- Main thread joins the background thread in `flush_finish()`
- The worker pool (used for lazy-load) is NOT used for flush — dedicated thread

## Critical Files

| File | Role |
|------|------|
| `ovsdb/disk-store.c` | Extend header, add `write_db()` |
| `ovsdb/disk-store.h` | New API declarations |
| `ovsdb/storage.c` | Dual raft+ds mode, attach/validate |
| `ovsdb/storage.h` | New function declarations |
| `ovsdb/raft.c` | `raft_set_applied_index()` |
| `ovsdb/raft.h` | Declare new function |
| `ovsdb/ovsdb.h` | Add `binstore_flush` field |
| `ovsdb/ovsdb.c` | Snapshot coordination |
| `ovsdb/ovsdb-server.c` | Startup path, main loop, --disk-store, unixctl |
| `ovsdb/binstore-flush.c` | **New** — flush state machine |
| `ovsdb/binstore-flush.h` | **New** — flush API |
| `ovsdb/automake.mk` | Add new source files |

## Risks

| Risk | Mitigation |
|------|-----------|
| Memory amplification during flush (clone doubles memory) | Same cost as existing compaction. Guard: don't flush and compact simultaneously. |
| Stale sidecar with data from overwritten Raft term | EID-based validation ensures consistency. Mismatch → delete and rebuild. |
| Disk space (sidecar duplicates data) | Binary is more compact than JSON. Feature is opt-in via `--disk-store`. Document trade-off. |
| Schema changes invalidate sidecar | Detect via schema hash mismatch, auto-rebuild. Infrequent in practice. |

## Verification

1. `make -j4` — compile all changes
2. `make check TESTSUITEFLAGS="-k raft-binstore"` — new tests
3. `make check TESTSUITEFLAGS="-k cluster"` — existing cluster tests still pass
4. `make check TESTSUITEFLAGS="-k binary-serve"` — existing binary serve tests still pass
5. Manual 3-node cluster test:
   ```bash
   # Create cluster
   ovsdb-tool create-cluster db1.db schema.ovsschema tcp:127.0.0.1:6001
   ovsdb-tool join-cluster db2.db OVN_Southbound tcp:127.0.0.1:6002 tcp:127.0.0.1:6001
   ovsdb-tool join-cluster db3.db OVN_Southbound tcp:127.0.0.1:6003 tcp:127.0.0.1:6001

   # Start with --disk-store
   ovsdb-server --disk-store db1.db --remote=ptcp:6641:127.0.0.1 &
   ovsdb-server --disk-store db2.db --remote=ptcp:6642:127.0.0.1 &
   ovsdb-server --disk-store db3.db --remote=ptcp:6643:127.0.0.1 &

   # Insert data, wait for flush
   ovsdb-client transact tcp:127.0.0.1:6641 '[...]'
   ovs-appctl -t db1 ovsdb/binstore/status OVN_Southbound

   # Stop and restart node 1 — verify fast startup
   kill %1
   ovsdb-server --disk-store db1.db --remote=ptcp:6641:127.0.0.1 &
   # Check log for "fast startup from binary store"
   ```
