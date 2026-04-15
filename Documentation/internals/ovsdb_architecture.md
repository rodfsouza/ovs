# OVSDB Code Architecture Overview

The Open vSwitch Database (OVSDB) is a lightweight database system designed for managing the configuration of Open vSwitch. It provides ACID transactions, a JSON-RPC based interface, and support for both standalone and clustered (Raft) deployment models.

## Core Components

### 1. OVSDB Server (`ovsdb-server.c`)
The `ovsdb-server` daemon is the main entrance point. Its primary responsibilities include:
- Managing multiple database instances simultaneously.
- Handling client connections via various "remotes" (Unix sockets, TCP, SSL).
- Executing the main run loop that coordinates between storage, triggers, and JSON-RPC servers.
- Managing server-level configuration (remotes, SSL, etc.).

### 2. Database Engine (`ovsdb.c`, `ovsdb.h`)
This is the core in-memory representation of a database.
- **`struct ovsdb`**: The central structure holding the schema, tables, storage back-end, and active monitors.
- **Tables and Rows**: Data is organized into tables (`ovsdb_table`), which consist of rows (`ovsdb_row`). Each row is indexed by a UUID.
- **Columns**: Defined by `ovsdb_column`, specifying types and constraints.

### 3. Storage Abstraction (`storage.c`, `file.h`, `raft.h`)
OVSDB decouples the database logic from how it is persisted.
- **Standalone Model (`file.c`)**: Simple log-structured file on disk. Each transaction is appended to the log.
- **Clustered Model (`raft.c`)**: Uses the Raft consensus algorithm for high availability. Transactions are replicated across a cluster of servers.
- **Relay Model (`relay.c`)**: Acts as a proxy to another OVSDB server, caching data and forwarding transactions.

### 4. Transactions (`transaction.c`)
All database modifications are performed through transactions to ensure ACID properties.
- **`struct ovsdb_txn`**: Represents a pending transaction.
- **ACID**: Ensures that either all changes in a transaction are applied, or none are.
- **Execution**: The `ovsdb_execute()` function processes JSON-RPC requests (like `insert`, `update`, `delete`, `select`, `wait`) within a transaction context.

### 5. Communication Layer (`jsonrpc-server.c`)
Clients interact with OVSDB using JSON-RPC over various transport layers.
- **JSON-RPC**: A lightweight remote procedure call protocol using JSON.
- **Remotes**: Configurable endpoints where the server listens for connections.
- **Operations**: RFC 7047 defines the standard operations (e.g., `transact`, `monitor`, `monitor_cond`).

### 6. Monitoring and Triggers (`monitor.c`, `trigger.c`)
- **Monitoring**: Allows clients to subscribe to changes in specific tables or columns. When a transaction commits, the monitoring engine identifies which clients need notifications and sends them.
- **Triggers**: Internal mechanisms to execute actions when certain conditions are met (e.g., waiting for a specific value to appear in a row).

## Data Flow: A Transaction's Journey

1. **Request**: A client sends a `transact` JSON-RPC request to `ovsdb-server`.
2. **Parsing**: `jsonrpc-server.c` parses the JSON and identifies the target database.
3. **Drafting**: `transaction.c` creates a new `ovsdb_txn`. `execution.c` processes each operation in the request, modifying the in-memory state.
4. **Validation**: The engine checks schema constraints (e.g., uniqueness, foreign keys).
5. **Commit (Storage)**:
   - In **Standalone** mode, the change is written to the disk log.
   - In **Clustered** mode, the change is submitted to the Raft leader and must be committed by a majority of nodes.
6. **Apply**: Once persisted, the changes are formally committed to the in-memory tables.
7. **Notifications**: `monitor.c` scans the changes and notifies all subscribed clients.
8. **Response**: A JSON-RPC response is sent back to the client confirming the transaction status.

## Key Files to Explore

| File | Description |
| :--- | :--- |
| [ovsdb-server.c](file:///Users/rsouza/repos/mgc-iaas/foundation/ovs/ovsdb/ovsdb-server.c) | Main server loop and management logic. |
| [ovsdb.h](file:///Users/rsouza/repos/mgc-iaas/foundation/ovs/ovsdb/ovsdb.h) | Primary data structures for the database engine. |
| [transaction.c](file:///Users/rsouza/repos/mgc-iaas/foundation/ovs/ovsdb/transaction.c) | Logic for ACID transactions. |
| [storage.c](file:///Users/rsouza/repos/mgc-iaas/foundation/ovs/ovsdb/storage.c) | Persistence layer abstraction. |
| [raft.c](file:///Users/rsouza/repos/mgc-iaas/foundation/ovs/ovsdb/raft.c) | Clustered database implementation (Raft). |
| [monitor.c](file:///Users/rsouza/repos/mgc-iaas/foundation/ovs/ovsdb/monitor.c) | Change notification tracking. |
| [jsonrpc-server.c](file:///Users/rsouza/repos/mgc-iaas/foundation/ovs/ovsdb/jsonrpc-server.c) | Handling of client network connections. |
