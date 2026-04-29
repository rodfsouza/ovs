# 📄 Spec: Clock-Sweep Cache (Postgres-style) + Scan-Aware Eviction

## 1. Overview

This document specifies a new cache replacement strategy to replace the current LRU implementation.

The new design is inspired by PostgreSQL buffer management and introduces:

- Clock-sweep eviction (approximate LRU)
- Read/write separation for high concurrency
- Pin-based safety for in-use entries
- Scan-aware behavior using a ring buffer strategy
- Reduced cache pollution under large scans

## 2. Goals

### Functional
- Maintain hot working set efficiently
- Evict low-value entries under pressure
- Prevent scan workloads from destroying cache

### Performance
- Remove read serialization bottlenecks
- Avoid global mutation on read
- Improve throughput under concurrent access

### Behavioral
- Approximate LRU semantics
- Protect frequently used entries
- Ensure predictable eviction under load

## 3. Core Concepts

### Entry Structure
```
Entry {
  Key key
  Value value

  bool valid
  bool dirty
  bool evicting

  uint8 usage_count
  uint32 pin_count
}
```

MAX_USAGE_COUNT = 5

### Clock Hand
```
Atomic<uint32> next_victim
```

## 4. Cache Operations

### Read (Hit)
1. Lookup entry
2. Increment pin_count
3. Validate
4. Increment usage_count
5. Return value
6. Decrement pin_count

### Write (Miss)
1. Acquire write lock
2. Allocate or evict
3. Flush if dirty
4. Insert new entry

### Victim Selection
```
loop:
  idx = next_victim++
  entry = entries[idx % capacity]

  if pinned → skip
  if usage_count > 0 → decrement
  else → evict
```

## 5. Scan-Aware Behavior

### Problem
Sequential scans can evict hot data.

### Solution
Use BULK_READ mode with ring buffer.

### Ring Design
```
ScanRing {
  Entry* slots[RING_SIZE]
  int current
}
```

### Rules
- usage_count = 0 or 1
- no promotion
- reuse ring slots

## 6. Concurrency

- Reads: parallel
- Writes: isolated
- Pin-based eviction protection
- Sharded locks

## 7. Metrics

- hits/misses
- evictions
- usage histogram
- scan reuse

## 8. Summary

Clock-sweep + scan-aware design ensures high performance and avoids cache pollution.
