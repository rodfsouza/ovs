OVSDB Force Recovery
====================

When an OVSDB cluster experiences a split-brain, some nodes may end up with
transactions that reference rows in an inconsistent state (e.g., deleting a
row that does not exist). These inconsistencies cause `ovsdb-tool compact`
and `ovsdb-tool cluster-to-standalone` to abort with a fatal error.

The `--force` flag allows these operations to proceed by dropping entire
transactions that cannot be applied cleanly, rather than aborting.

Usage
-----

The `--force` flag must appear before the subcommand:

    # Compact a standalone DB that has inconsistent transactions
    ovsdb-tool --force compact /path/to/conf.db

    # Convert a clustered DB to standalone when the cluster is down
    ovsdb-tool --force cluster-to-standalone /path/to/recovered.db /path/to/cluster.db

When a transaction is dropped, a warning is logged to stderr:

    ovsdb|WARN|transaction deletes row <UUID> that does not exist (--force, skipping transaction)

Use `-v` for additional verbosity.

Behavior
--------

When `--force` is active:

- Transactions that reference inconsistent state (e.g., delete of a
  non-existent row) are dropped in their entirety.
- No partial transaction is ever applied. Each transaction is all-or-nothing.
- The resulting database file contains only transactions that applied cleanly.
- The output database is self-consistent and can be used to start
  `ovsdb-server` without errors.

Without `--force`, the tool aborts on the first inconsistency (original
behavior, unchanged).

Recovery Workflow
-----------------

1. Stop all `ovsdb-server` instances in the cluster.

2. Identify the most up-to-date cluster database file (typically the largest
   or most recently modified).

3. Convert to a standalone database with `--force`:

       ovsdb-tool --force cluster-to-standalone \
           /var/lib/openvswitch/recovered.db \
           /var/lib/openvswitch/conf.db

4. Review the warnings to understand which transactions were dropped.

5. Start `ovsdb-server` in standalone mode with the recovered database:

       ovsdb-server /var/lib/openvswitch/recovered.db

6. Verify that the configuration is sane:

       ovs-vsctl show

7. To re-establish a cluster from the recovered database:

       ovsdb-tool create-cluster \
           /var/lib/openvswitch/conf.db \
           /var/lib/openvswitch/recovered.db \
           tcp:<local-address>:6644

   Then have other nodes join the new cluster with `join-cluster`.

Caveats
-------

- Dropped transactions represent real configuration changes that were
  committed on one side of the split-brain. Review the warnings to assess
  whether manual re-application of lost changes is needed.

- The old RAFT cluster state (terms, votes, logs) is discarded. The old
  cluster cannot be revived; a new cluster must be created from the
  recovered standalone database.

- This flag is intended for disaster recovery only. It should not be used
  during normal operation.
