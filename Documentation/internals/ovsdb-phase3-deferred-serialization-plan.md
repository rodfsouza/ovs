# Phase 3: Deferred JSON Serialization — Detailed Implementation Plan

## Context

OVSDB uses a single-threaded reactor pattern (`ovsdb-server.c:main_loop()`). All client
I/O, Raft consensus, transaction execution, and JSON serialization run on one thread.
When a large monitor update or initial snapshot is sent, `json_to_ds()` inside
`jsonrpc_send()` serializes the entire JSON tree into a contiguous string — blocking
the main thread for seconds to minutes on multi-GB databases.

Phases 0–2 are already implemented:
- **Phase 0**: Standalone snapshot threading (`compaction_thread()` pattern)
- **Phase 1**: Binary disk store + LRU row cache (`ovsdb/disk-store.c`, `ovsdb/row-cache.c`)
- **Phase 2**: I/O worker pool + lazy loading (`ovsdb/worker-pool.c`, `ovsdb/lazy-load.c`)

Phase 3 offloads `json_to_ds()` for large payloads to background worker threads so
the main loop can continue processing transactions, Raft, and other clients while
serialization happens in parallel.

---

## Current Bottleneck

### The synchronous send path (`lib/jsonrpc.c:254-306`)

```c
int jsonrpc_send(struct jsonrpc *rpc, struct jsonrpc_msg *msg) {
    json = jsonrpc_msg_to_json(msg);     /* Cheap: wraps msg fields in JSON object */
    json_to_ds(json, 0, &ds);           /* EXPENSIVE: O(n) recursive serialize */
    json_destroy(json);

    buf = xmalloc(sizeof *buf);
    ofpbuf_use_ds(buf, &ds);
    ovs_list_push_back(&rpc->output, &buf->list_node);
    rpc->backlog += length;
    jsonrpc_run(rpc);                    /* Non-blocking stream_send() */
}
```

Every call to `jsonrpc_send()` — monitor updates, initial snapshots, query replies —
goes through this path. For a 2GB database snapshot, `json_to_ds()` takes 30–60 seconds.

### Where large sends originate

| Call site | File | When |
|-----------|------|------|
| `ovsdb_jsonrpc_monitor_flush_all()` | `jsonrpc-server.c:1902` | Every main loop iteration, per session |
| `ovsdb_jsonrpc_monitor_create()` | `jsonrpc-server.c:1489` | Initial monitor reply (full snapshot) |
| `ovsdb_jsonrpc_session_got_request()` | `jsonrpc-server.c:1103` | `transact` and `get_schema` replies |
| `ovsdb_jsonrpc_create_notify()` | `jsonrpc-server.c:1873` | Monitor update notifications |

### Existing optimization: `JSON_SERIALIZED_OBJECT` cache

`ovsdb/monitor.c` already pre-serializes V2/V3 unconditional monitor updates via
`json_serialized_object_create_with_yield()` and caches them in a per-change-set hmap.
When `json_serialize()` encounters a `JSON_SERIALIZED_OBJECT`, it outputs the cached
string directly (`json.c:1651-1652`). This helps for incremental updates shared across
clients but does NOT help for:
- Initial snapshots (one-shot, not cached)
- Conditional monitor updates (per-client, not cacheable)
- Large `transact` replies
- Raft `InstallSnapshot` encoding

---

## Design Overview

### Architecture

```
Main Thread                         Serialize Worker Pool (2 threads)
    │                                        │
    ├─ Transaction commit                    │
    │  (single-threaded, fast)               │
    │                                        │
    ├─ Monitor flush / reply needed          │
    │  Build JSON tree (cheap)               │
    │                                        │
    ├─ json_estimate_size(json) > threshold? │
    │                                        │
    │  [SMALL] jsonrpc_send() inline         │
    │  (same as today, no change)            │
    │                                        │
    │  [LARGE] ovsdb_deferred_send_enqueue() │
    │   ├─ jsonrpc_msg_to_json() on main     │
    │   ├─ Submit to worker pool ──────────► │
    │   │  {json_tree, session_id}           ├─ json_to_ds(json, 0, &ds)
    │   ├─ Mark session: has_deferred=true   │   (CPU-bound, off main thread)
    │   │                                    │
    │   │  ... process other sessions ...    │
    │   │                                    │
    │   │  ◄─── seq_change() ──────────────┤
    │   ├─ ovsdb_deferred_send_run()         │
    │   │  done_fn(ds, session)              │
    │   │  jsonrpc_send_preformatted(rpc, ds)│
    │   │  Drain consecutive ready entries   │
    │   └─ Clear has_deferred if queue empty │
    │                                        │
    ├─ jsonrpc_run()                         │
    │  stream_send() (non-blocking)          │
```

### Four components

1. **`json_estimate_size()`** — Fast size estimation for JSON trees (`lib/json.c`)
2. **`jsonrpc_send_preformatted()`** — Enqueue pre-serialized buffer (`lib/jsonrpc.c`)
3. **`ovsdb/deferred-send.c`** — Per-session deferred serialization queue
4. **`serialize_worker_pool`** — Dedicated serialization pool (`ovsdb-server.c`)

### Key invariants

- **Message ordering**: Within a session, messages are delivered in the order they
  were enqueued. If message N is deferred, messages N+1, N+2, ... queue behind it.
- **Data immutability**: `jsonrpc_msg_to_json()` is called on the main thread. The
  resulting JSON tree is read-only in the worker. The main thread does not free it
  until the done callback runs. COW semantics of transactions guarantee old row
  data is stable.
- **No new locking in OVSDB core**: All OVSDB data structures remain single-threaded.
  Only the worker pool mutex (already exists) is used.
- **Graceful degradation**: If the worker pool is NULL or full, messages serialize
  inline as today. No correctness impact — only performance.

---

## Component 1: JSON Size Estimation

### New function in `lib/json.c`

```c
/* Returns an estimate of the serialized byte size of 'json'.
 *
 * This is O(nodes) but with a tiny constant — just pointer chasing and
 * arithmetic, no allocation or string building. The estimate is always >=
 * the actual size (it overestimates slightly due to ignoring escaping
 * savings and underestimates for deeply nested structures, but in practice
 * is within 10% for OVSDB payloads). */
size_t
json_estimate_size(const struct json *json)
```

### Implementation

Walk the JSON tree recursively. For each node type:

| Type | Estimate |
|------|----------|
| `JSON_NULL` | 4 (`null`) |
| `JSON_TRUE` | 4 (`true`) |
| `JSON_FALSE` | 5 (`false`) |
| `JSON_INTEGER` | 20 (max digits for int64_t) |
| `JSON_REAL` | 24 (max `-1.7976931348623157e+308`) |
| `JSON_STRING` | `strlen(json->string) + 2` (quotes) |
| `JSON_SERIALIZED_OBJECT` | `strlen(json->string)` (already serialized) |
| `JSON_OBJECT` | Sum of children + `6 * n` (quotes, colon, comma, braces) |
| `JSON_ARRAY` | Sum of children + `n` (commas) + 2 (brackets) |

Use an iterative approach with an explicit stack to avoid C stack overflow on
deeply nested objects. Cap recursion at a configurable depth (e.g., 256) and
return `SIZE_MAX` if exceeded (forces inline serialization, which handles
depth gracefully).

### Declaration in `include/openvswitch/json.h`

```c
size_t json_estimate_size(const struct json *);
```

### Threshold constant

```c
/* In ovsdb/deferred-send.h or ovsdb-server.c */
#define OVSDB_DEFERRED_SERIALIZE_THRESHOLD (1024 * 1024)  /* 1 MB */
```

Messages below this threshold serialize inline. Above it, they are deferred
to the worker pool.

---

## Component 2: Pre-formatted Send Path

### New function in `lib/jsonrpc.c`

```c
/* Enqueue a pre-serialized message for sending.
 *
 * 'ds' contains the complete JSON-RPC message as a serialized string
 * (including the outer {"method":...} or {"result":...} wrapper).
 * Ownership of 'ds' contents is transferred to 'rpc'.
 *
 * This bypasses json_to_ds() entirely — the data is already serialized. */
int
jsonrpc_send_preformatted(struct jsonrpc *rpc, struct ds *ds)
{
    struct ofpbuf *buf;
    size_t length;

    if (rpc->status) {
        ds_destroy(ds);
        return rpc->status;
    }

    length = ds->length;
    buf = xmalloc(sizeof *buf);
    ofpbuf_use_ds(buf, ds);
    ovs_list_push_back(&rpc->output, &buf->list_node);
    rpc->output_count++;
    rpc->backlog += length;

    /* Same backlog threshold logic as jsonrpc_send(). */
    if (rpc->output_count >= 50) {
        /* ... backlog checking (extract to shared helper) ... */
    }

    if (rpc->backlog == length) {
        jsonrpc_run(rpc);
    }
    return rpc->status;
}
```

### Session-level wrapper in `lib/jsonrpc.c`

```c
int
jsonrpc_session_send_preformatted(struct jsonrpc_session *s, struct ds *ds)
{
    if (s->rpc) {
        return jsonrpc_send_preformatted(s->rpc, ds);
    } else {
        ds_destroy(ds);
        return ENOTCONN;
    }
}
```

### Declarations in `lib/jsonrpc.h`

```c
int jsonrpc_send_preformatted(struct jsonrpc *, struct ds *);
int jsonrpc_session_send_preformatted(struct jsonrpc_session *, struct ds *);
```

### Refactor: Extract backlog checking

The backlog threshold check (lines 280–301 of `jsonrpc_send()`) should be extracted
into a static helper:

```c
static void
jsonrpc_check_backlog(struct jsonrpc *rpc)
{
    if (rpc->output_count >= 50) {
        /* ... existing threshold logic ... */
    }
}
```

Called from both `jsonrpc_send()` and `jsonrpc_send_preformatted()`.

---

## Component 3: Deferred Send Queue

### New files: `ovsdb/deferred-send.c`, `ovsdb/deferred-send.h`

This module manages per-session FIFO queues of messages awaiting serialization.

### Data structures (`ovsdb/deferred-send.h`)

```c
#ifndef OVSDB_DEFERRED_SEND_H
#define OVSDB_DEFERRED_SEND_H 1

#include <stdbool.h>
#include <stddef.h>
#include "openvswitch/list.h"

struct ds;
struct json;
struct jsonrpc_session;
struct ovsdb_worker_pool;

/* State of a single deferred serialization entry. */
enum ovsdb_deferred_state {
    OVSDB_DEFERRED_QUEUED,      /* Waiting for a worker slot. */
    OVSDB_DEFERRED_SERIALIZING, /* Worker thread is running json_to_ds(). */
    OVSDB_DEFERRED_READY,       /* Serialization complete, awaiting send. */
};

/* A single message in the deferred queue. */
struct ovsdb_deferred_entry {
    struct ovs_list list_node;          /* In ovsdb_deferred_send->entries. */
    enum ovsdb_deferred_state state;

    /* Input: set on enqueue, consumed by worker. */
    struct json *json;                  /* Full JSON-RPC message tree.
                                         * Owned by this entry until worker
                                         * done callback frees it. */

    /* Output: set by worker done callback. */
    struct ds serialized;               /* Serialized string.  Valid only
                                         * when state == READY. */
};

/* Per-session deferred send queue.
 *
 * Entries are appended at the tail and drained from the head.
 * A session's deferred queue preserves message ordering: if entry N
 * is still serializing, entries N+1..M are not sent even if ready. */
struct ovsdb_deferred_send {
    struct ovs_list entries;            /* List of ovsdb_deferred_entry. */
    size_t n_entries;                   /* Number of entries in the list. */
    size_t n_serializing;              /* Entries currently in workers. */
    struct jsonrpc_session *js;         /* Backpointer for sending. */
};

/* Lifecycle. */
void ovsdb_deferred_send_init(struct ovsdb_deferred_send *,
                               struct jsonrpc_session *js);
void ovsdb_deferred_send_destroy(struct ovsdb_deferred_send *);

/* Enqueue a message for deferred serialization.
 * Takes ownership of 'json' (the full JSON-RPC message tree). */
void ovsdb_deferred_send_enqueue(struct ovsdb_deferred_send *,
                                  struct json *json,
                                  struct ovsdb_worker_pool *pool);

/* Drain ready entries: send consecutive completed serializations.
 * Called from the main thread after ovsdb_worker_pool_run(). */
void ovsdb_deferred_send_run(struct ovsdb_deferred_send *);

/* Returns true if the queue has any entries (queued, serializing, or ready). */
bool ovsdb_deferred_send_has_pending(const struct ovsdb_deferred_send *);

/* Returns the number of entries currently being serialized by workers. */
size_t ovsdb_deferred_send_n_serializing(const struct ovsdb_deferred_send *);

#endif /* ovsdb/deferred-send.h */
```

### Implementation (`ovsdb/deferred-send.c`)

#### `ovsdb_deferred_send_init()`

```c
void
ovsdb_deferred_send_init(struct ovsdb_deferred_send *ds,
                          struct jsonrpc_session *js)
{
    ovs_list_init(&ds->entries);
    ds->n_entries = 0;
    ds->n_serializing = 0;
    ds->js = js;
}
```

#### `ovsdb_deferred_send_destroy()`

Drains remaining entries, destroying unsent JSON and serialized buffers:

```c
void
ovsdb_deferred_send_destroy(struct ovsdb_deferred_send *ds)
{
    struct ovsdb_deferred_entry *entry;

    LIST_FOR_EACH_POP (entry, list_node, &ds->entries) {
        if (entry->json) {
            json_destroy(entry->json);
        }
        if (entry->state == OVSDB_DEFERRED_READY) {
            ds_destroy(&entry->serialized);
        }
        free(entry);
    }
    ds->n_entries = 0;
    ds->n_serializing = 0;
}
```

**Caution**: If an entry is in `SERIALIZING` state during destroy, the worker
thread still holds a reference to `entry->json`. The destroy must wait for
the worker to finish. This is handled by calling
`ovsdb_worker_pool_destroy()` (which joins all threads) **before** destroying
deferred-send queues. The shutdown order in `ovsdb-server.c:main()` must be:

1. `ovsdb_worker_pool_destroy(serialize_worker_pool)` — joins threads
2. Destroy sessions (which destroy deferred-send queues)

#### `ovsdb_deferred_send_enqueue()`

```c
void
ovsdb_deferred_send_enqueue(struct ovsdb_deferred_send *dfs,
                             struct json *json,
                             struct ovsdb_worker_pool *pool)
{
    struct ovsdb_deferred_entry *entry;

    entry = xzalloc(sizeof *entry);
    entry->json = json;
    entry->state = OVSDB_DEFERRED_QUEUED;
    ds_init(&entry->serialized);

    ovs_list_push_back(&dfs->entries, &entry->list_node);
    dfs->n_entries++;

    /* Submit to worker pool immediately. */
    entry->state = OVSDB_DEFERRED_SERIALIZING;
    dfs->n_serializing++;
    ovsdb_worker_pool_submit(pool,
                             deferred_serialize_fn, entry,
                             deferred_serialize_done, dfs);
}
```

#### Worker function (runs in worker thread)

```c
static void *
deferred_serialize_fn(void *arg)
{
    struct ovsdb_deferred_entry *entry = arg;

    json_to_ds(entry->json, 0, &entry->serialized);

    return entry;
}
```

**Thread safety**: `json_to_ds()` reads the JSON tree (immutable since the main
thread created it and won't touch it until the done callback). It writes to
`entry->serialized` which is a per-entry buffer not accessed by anyone else.
No locking needed.

#### Done callback (runs on main thread)

```c
static void
deferred_serialize_done(void *result, void *aux)
{
    struct ovsdb_deferred_entry *entry = result;
    struct ovsdb_deferred_send *dfs = aux;

    /* Free the JSON tree — no longer needed after serialization. */
    json_destroy(entry->json);
    entry->json = NULL;

    entry->state = OVSDB_DEFERRED_READY;
    dfs->n_serializing--;

    /* Don't send here — ovsdb_deferred_send_run() handles ordering. */
}
```

#### `ovsdb_deferred_send_run()`

Drain consecutive ready entries from the head of the FIFO:

```c
void
ovsdb_deferred_send_run(struct ovsdb_deferred_send *dfs)
{
    while (!ovs_list_is_empty(&dfs->entries)) {
        struct ovsdb_deferred_entry *entry;

        entry = CONTAINER_OF(ovs_list_front(&dfs->entries),
                             struct ovsdb_deferred_entry, list_node);

        if (entry->state != OVSDB_DEFERRED_READY) {
            /* Head is not ready yet — stop.  Preserves ordering. */
            break;
        }

        /* Send the pre-serialized buffer. */
        jsonrpc_session_send_preformatted(dfs->js, &entry->serialized);

        ovs_list_remove(&entry->list_node);
        dfs->n_entries--;
        free(entry);
    }
}
```

---

## Component 4: Worker Pool for Serialization

### Strategy: Separate pool, not reuse `io_worker_pool`

**Rationale**: Serialization is CPU-bound; I/O loading is latency-bound. Mixing
them risks serialization jobs starving I/O workers (or vice versa). A dedicated
pool with a small thread count (2 threads) keeps CPU usage predictable.

### New static in `ovsdb-server.c`

```c
#define OVSDB_SERIALIZE_WORKER_THREADS 2
static struct ovsdb_worker_pool *serialize_worker_pool;
```

### Initialization (in `main()`, after `io_worker_pool` creation)

```c
/* Create serialization worker pool (Phase 3). */
serialize_worker_pool = ovsdb_worker_pool_create(
    OVSDB_SERIALIZE_WORKER_THREADS, "ser-worker");
```

### Main loop integration (`main_loop()`)

After the existing `io_worker_pool` run/wait calls:

```c
/* In run phase: */
if (serialize_worker_pool) {
    ovsdb_worker_pool_run(serialize_worker_pool);
}

/* After pool_run, drain deferred send queues for all sessions. */
ovsdb_jsonrpc_server_drain_deferred(jsonrpc);

/* In wait phase: */
if (serialize_worker_pool) {
    ovsdb_worker_pool_wait(serialize_worker_pool);
}
```

### Shutdown order (in `main()` after `main_loop()`)

```c
/* Phase 3: Join serialization workers before destroying sessions. */
ovsdb_worker_pool_destroy(serialize_worker_pool);
serialize_worker_pool = NULL;

/* Phase 2: Join I/O workers before destroying lazy-load state. */
ovsdb_lazy_load_destroy();
ovsdb_worker_pool_destroy(io_worker_pool);
io_worker_pool = NULL;
```

### Global in-flight cap

To prevent memory bloat from too many concurrent large serializations:

```c
#define OVSDB_MAX_DEFERRED_IN_FLIGHT 4

/* In ovsdb_jsonrpc_session_send_or_defer() — see integration section. */
if (total_in_flight >= OVSDB_MAX_DEFERRED_IN_FLIGHT) {
    /* Fall back to inline serialization. */
    jsonrpc_session_send(s->js, msg);
} else {
    ovsdb_deferred_send_enqueue(&s->deferred, json, serialize_worker_pool);
}
```

The `total_in_flight` counter is maintained by the server across all sessions.
Increment in `ovsdb_deferred_send_enqueue()`, decrement in `deferred_serialize_done()`.

---

## Integration Points

### 1. Session struct changes (`ovsdb/jsonrpc-server.c`)

Add deferred send queue to the session:

```c
struct ovsdb_jsonrpc_session {
    struct ovs_list node;
    struct ovsdb_session up;
    struct ovsdb_jsonrpc_remote *remote;
    bool db_change_aware;
    struct hmap triggers;
    struct hmap monitors;
    struct jsonrpc_session *js;
    unsigned int js_seqno;
    bool read_only;
    /* Phase 3: deferred serialization queue. */
    struct ovsdb_deferred_send deferred;
};
```

Initialize in `ovsdb_jsonrpc_session_create()`:
```c
ovsdb_deferred_send_init(&s->deferred, s->js);
```

Destroy in `ovsdb_jsonrpc_session_close()`:
```c
ovsdb_deferred_send_destroy(&s->deferred);
```

Re-initialize on reconnect (when `js_seqno` changes in `ovsdb_jsonrpc_session_run()`):
```c
if (s->js_seqno != jsonrpc_session_get_seqno(s->js)) {
    /* ... existing cleanup ... */
    ovsdb_deferred_send_destroy(&s->deferred);
    ovsdb_deferred_send_init(&s->deferred, s->js);
}
```

### 2. New send-or-defer wrapper (`ovsdb/jsonrpc-server.c`)

Replace direct `jsonrpc_session_send()` calls at large-payload sites:

```c
/* Send 'msg' to the session.  If the estimated serialized size exceeds
 * the deferred threshold and a serialization worker pool is available,
 * enqueue for background serialization.  Otherwise, send inline. */
static void
ovsdb_jsonrpc_session_send_or_defer(struct ovsdb_jsonrpc_session *s,
                                     struct jsonrpc_msg *msg,
                                     struct ovsdb_worker_pool *pool)
{
    /* If no pool, or if deferred queue already has pending entries
     * (ordering constraint), or if the message is small, send inline. */
    if (!pool
        || ovsdb_deferred_send_has_pending(&s->deferred)) {
        /* If deferred queue has pending entries, we must queue this one
         * too to preserve ordering — but only if it's a notification.
         * For replies (which have an id), inline send is fine since
         * replies and notifications are independent streams. */
        jsonrpc_session_send(s->js, msg);
        return;
    }

    /* Estimate size.  Use the params/result field as the proxy since
     * the msg wrapper is tiny. */
    struct json *payload = msg->params ? msg->params : msg->result;
    if (!payload || json_estimate_size(payload)
                    < OVSDB_DEFERRED_SERIALIZE_THRESHOLD) {
        jsonrpc_session_send(s->js, msg);
        return;
    }

    /* Convert to JSON tree and defer. */
    struct json *json = jsonrpc_msg_to_json(msg);  /* Cheap, main thread. */
    ovsdb_deferred_send_enqueue(&s->deferred, json, pool);
}
```

### 3. Monitor flush integration (`ovsdb/jsonrpc-server.c`)

Modify `ovsdb_jsonrpc_monitor_flush_all()`:

```c
static void
ovsdb_jsonrpc_monitor_flush_all(struct ovsdb_jsonrpc_session *s)
{
    struct ovsdb_jsonrpc_monitor *m;

    HMAP_FOR_EACH (m, node, &s->monitors) {
        struct json *json;

        json = ovsdb_jsonrpc_monitor_compose_update(m, false);
        if (json) {
            struct jsonrpc_msg *msg;
            struct json *params;

            /* ... existing params construction ... */

            msg = ovsdb_jsonrpc_create_notify(m, params);

            /* Phase 3: use deferred send for large updates. */
            ovsdb_jsonrpc_session_send_or_defer(s, msg,
                                                 serialize_worker_pool);
        }
    }
}
```

### 4. Session run loop backlog check (`ovsdb/jsonrpc-server.c`)

Extend the backlog gate in `ovsdb_jsonrpc_session_run()` to also check
the deferred queue:

```c
static int
ovsdb_jsonrpc_session_run(struct ovsdb_jsonrpc_session *s)
{
    jsonrpc_session_run(s->js);

    /* ... reconnect handling ... */

    ovsdb_jsonrpc_trigger_complete_done(s);

    if (!jsonrpc_session_get_backlog(s->js)
        && !ovsdb_deferred_send_has_pending(&s->deferred)) {
        /* Only flush monitors and receive requests when both the
         * transport buffer and deferred queue are empty. */
        ovsdb_jsonrpc_monitor_flush_all(s);

        struct jsonrpc_msg *msg = jsonrpc_session_recv(s->js);
        /* ... */
    }
    return jsonrpc_session_is_alive(s->js) ? 0 : ETIMEDOUT;
}
```

### 5. Server-level drain function (`ovsdb/jsonrpc-server.c`)

New function called from `main_loop()` after `ovsdb_worker_pool_run()`:

```c
/* Drain deferred send queues for all sessions across all remotes.
 * Must be called after ovsdb_worker_pool_run(serialize_worker_pool)
 * so that done callbacks have marked entries as READY. */
void
ovsdb_jsonrpc_server_drain_deferred(struct ovsdb_jsonrpc_server *svr)
{
    struct shash_node *node;

    SHASH_FOR_EACH (node, &svr->remotes) {
        struct ovsdb_jsonrpc_remote *remote = node->data;
        struct ovsdb_jsonrpc_session *s;

        LIST_FOR_EACH (s, node, &remote->sessions) {
            ovsdb_deferred_send_run(&s->deferred);
        }
    }
}
```

### 6. Initial monitor reply (`ovsdb/jsonrpc-server.c`)

The initial monitor reply in `ovsdb_jsonrpc_monitor_create()` is currently:

```c
return jsonrpc_create_reply(json, request_id);
```

This reply goes through `ovsdb_jsonrpc_session_send()`. To defer it:

```c
struct jsonrpc_msg *reply = jsonrpc_create_reply(json, request_id);
ovsdb_jsonrpc_session_send_or_defer(s, reply, serialize_worker_pool);
return NULL;  /* Reply handled asynchronously. */
```

**Note**: This changes the return convention. The caller
(`ovsdb_jsonrpc_session_got_request()`) currently expects a non-NULL reply
to send. The call site must handle the NULL case:

```c
reply = ovsdb_jsonrpc_monitor_create(s, db, request->params,
                                      version, request->id);
if (reply) {
    ovsdb_jsonrpc_session_send(s, reply);
}
/* If NULL, reply was deferred — it will be sent later. */
```

---

## Handling Edge Cases

### Session disconnect while serialization is in flight

When a session closes (disconnect or reconnect):
1. `ovsdb_deferred_send_destroy()` is called
2. Entries in `SERIALIZING` state still have a worker thread reading `entry->json`
3. **Problem**: If we free `entry` now, the worker has a dangling pointer

**Solution**: Use a two-phase approach:
- Each `ovsdb_deferred_entry` gets a reference count (or a `cancelled` flag)
- `ovsdb_deferred_send_destroy()` marks entries as cancelled but does not free
  entries in `SERIALIZING` state
- The done callback checks the cancelled flag:
  - If cancelled: frees the entry (worker is done, safe now)
  - If not cancelled: marks READY as normal
- This requires the done callback `aux` to remain valid even after session close

**Simpler alternative**: The `ovsdb_deferred_send` struct can be heap-allocated
and ref-counted. The session holds one ref, and each in-flight worker holds one
ref. The struct is freed when the last ref drops. This avoids the cancelled-flag
complexity.

```c
struct ovsdb_deferred_send {
    struct ovs_list entries;
    size_t n_entries;
    size_t n_serializing;
    struct jsonrpc_session *js;   /* NULL if session closed. */
    size_t refcount;              /* Session + in-flight workers. */
};
```

In the done callback:
```c
static void
deferred_serialize_done(void *result, void *aux)
{
    struct ovsdb_deferred_entry *entry = result;
    struct ovsdb_deferred_send *dfs = aux;

    json_destroy(entry->json);
    entry->json = NULL;
    dfs->n_serializing--;

    if (dfs->js) {
        /* Session still alive — mark ready for sending. */
        entry->state = OVSDB_DEFERRED_READY;
    } else {
        /* Session gone — discard. */
        ds_destroy(&entry->serialized);
        ovs_list_remove(&entry->list_node);
        dfs->n_entries--;
        free(entry);
    }

    if (--dfs->refcount == 0) {
        free(dfs);
    }
}
```

### JSON_SERIALIZED_OBJECT fast path

If the JSON payload is already a `JSON_SERIALIZED_OBJECT` (from monitor cache),
`json_to_ds()` is already fast (just `ds_put_cstr`). The size estimation should
detect this and not defer:

```c
/* In json_estimate_size() */
case JSON_SERIALIZED_OBJECT:
    return strlen(json->string);  /* Already serialized — cheap to send. */
```

And in `ovsdb_jsonrpc_session_send_or_defer()`, if the main payload is a
`JSON_SERIALIZED_OBJECT`, always send inline since the serialization cost
is negligible.

### Raft snapshot encoding

The `compaction_thread()` already serializes snapshots in a background thread.
Phase 3 does NOT change this path — it focuses on the client-facing send path.
However, if a Raft follower sends a large `InstallSnapshot` response, that goes
through `jsonrpc_send()` via `raft_send_append_request()`. This path could
benefit from deferred serialization in a future enhancement but is out of scope
for the initial Phase 3 implementation.

---

## Files Summary

### New files

| File | Est. Lines | Purpose |
|------|-----------|---------|
| `ovsdb/deferred-send.c` | ~250 | Per-session deferred serialization queue |
| `ovsdb/deferred-send.h` | ~70 | Public API and data structures |

### Modified files

| File | Changes |
|------|---------|
| `lib/json.c` | Add `json_estimate_size()` (~50 lines) |
| `include/openvswitch/json.h` | Declare `json_estimate_size()` |
| `lib/jsonrpc.c` | Add `jsonrpc_send_preformatted()`, `jsonrpc_session_send_preformatted()`, extract backlog helper (~60 lines) |
| `lib/jsonrpc.h` | Declare new send functions |
| `ovsdb/jsonrpc-server.c` | Add `ovsdb_deferred_send` to session, `ovsdb_jsonrpc_session_send_or_defer()`, modify flush and run paths, add `ovsdb_jsonrpc_server_drain_deferred()` (~80 lines) |
| `ovsdb/ovsdb-server.c` | Create/destroy `serialize_worker_pool`, integrate run/wait in main loop (~20 lines) |
| `ovsdb/automake.mk` | Add `deferred-send.c`, `deferred-send.h` to build |

### Unchanged files (critical to confirm)

| File | Why unchanged |
|------|---------------|
| `ovsdb/transaction.c` | Stays single-threaded — no new locking |
| `ovsdb/row.c`, `ovsdb/table.c` | No changes — deferred serialization reads immutable JSON trees, not row data |
| `ovsdb/monitor.c` | No changes — existing JSON cache and compose_update logic unchanged. The deferral happens at the send layer, not the compose layer |
| `ovsdb/raft.c` | Out of scope for Phase 3 |
| `ovsdb/worker-pool.c` | Already provides the needed API — no changes |

---

## Test Plan

### Unit tests for `json_estimate_size()` (`tests/test-json.c`)

```
AT_SETUP([json - estimate size of null/bool/int/real/string])
AT_SETUP([json - estimate size of array])
AT_SETUP([json - estimate size of nested object])
AT_SETUP([json - estimate size of serialized object])
AT_SETUP([json - estimate size accuracy within 20% of actual])
```

### Unit tests for deferred send (`tests/test-ovsdb.c` or new test file)

```
AT_SETUP([deferred send - small message sends inline])
AT_SETUP([deferred send - large message deferred to worker])
AT_SETUP([deferred send - ordering preserved across deferred entries])
AT_SETUP([deferred send - session close with in-flight serialization])
AT_SETUP([deferred send - reconnect clears deferred queue])
AT_SETUP([deferred send - graceful fallback when pool is NULL])
```

### Integration tests (`tests/ovsdb-server.at`)

```
AT_SETUP([ovsdb-server - deferred serialization for large monitor update])
  # Create database with many rows (>1MB serialized)
  # Connect monitor client
  # Insert rows to trigger large update
  # Verify client receives correct data
  # Verify server remained responsive during serialization
AT_CLEANUP

AT_SETUP([ovsdb-server - deferred initial monitor snapshot])
  # Create database with many rows
  # Connect new monitor client
  # Verify initial snapshot arrives correctly
  # Measure main-thread block time (should be minimal)
AT_CLEANUP

AT_SETUP([ovsdb-server - inline send for small messages])
  # Normal small transactions
  # Verify they still work with deferred infrastructure in place
AT_CLEANUP

AT_SETUP([ovsdb-server - concurrent sessions with deferred sends])
  # Connect multiple monitor clients
  # Trigger large update
  # Verify all clients receive correct, ordered data
AT_CLEANUP
```

### Performance benchmark

Not an automated test, but a manual verification:

1. Create a standalone database with ~500K rows (~1GB)
2. Connect a monitor client requesting all tables
3. Measure time from monitor request to first byte received
4. Measure main-thread responsiveness during initial snapshot (e.g., can
   another client run a small `transact` during the snapshot?)
5. Compare with/without deferred serialization (compile-time flag or
   threshold set to SIZE_MAX to disable)

---

## Risk Analysis

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Worker holds reference to freed JSON tree | HIGH | Ref-counted `ovsdb_deferred_send` + NULL `js` check in done callback |
| Message reordering within a session | HIGH | FIFO drain: only send from head; stop if head is not READY |
| Memory bloat from too many in-flight serializations | MEDIUM | Global `MAX_DEFERRED_IN_FLIGHT` cap; fall back to inline |
| Size estimation inaccuracy causes thrashing | LOW | Overestimate is fine (defer slightly too often). Underestimate means occasional inline serialization (no correctness impact) |
| json_to_ds() not truly thread-safe | LOW | Each call uses its own `struct ds` and `struct json_serializer`. The JSON tree is read-only. No shared mutable state. Confirmed safe. |
| Deferred path adds latency for large messages | LOW | Worker threads start immediately. Net latency is similar or better since main thread is unblocked for other work. Worst case: same as today if pool is busy. |
| Backlog accounting mismatch | MEDIUM | Pre-formatted send updates `rpc->backlog` the same way. The deferred queue itself is tracked separately via `n_entries`. |

---

## Implementation Order

1. **`json_estimate_size()`** in `lib/json.c` — standalone, testable immediately
2. **`jsonrpc_send_preformatted()`** in `lib/jsonrpc.c` — standalone, testable
3. **`ovsdb/deferred-send.c`** — depends on 1 + 2 + existing worker pool
4. **Session integration** in `ovsdb/jsonrpc-server.c` — depends on 3
5. **Pool creation** in `ovsdb-server.c` — depends on 4
6. **Tests** — after each step, but integration tests after step 5

Steps 1 and 2 can be done in parallel. Step 3 can begin as soon as both complete.
