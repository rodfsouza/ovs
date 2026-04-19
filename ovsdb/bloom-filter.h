/* Copyright (c) 2026 Magalu Cloud.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef OVSDB_BLOOM_FILTER_H
#define OVSDB_BLOOM_FILTER_H 1

#include <stdbool.h>
#include <stddef.h>

struct uuid;

/* A probabilistic set membership data structure.
 *
 * The bloom filter answers "is this key in the set?" with:
 *   - "definitely NOT" → true negative (guaranteed correct)
 *   - "maybe YES"      → could be a false positive
 *
 * Configured for 10 bits per key and 7 hash functions,
 * yielding a false positive rate below 1%.
 *
 * Memory usage: approximately n_expected * 10 / 8 bytes.
 * For 100K keys = ~122 KB.  For 1M keys = ~1.2 MB.
 *
 * Thread safety: concurrent reads are safe.  Writes (add)
 * are NOT thread-safe and must be externally synchronized. */

struct ovsdb_bloom_filter;

/* Lifecycle. */
struct ovsdb_bloom_filter *ovsdb_bloom_filter_create(
    size_t n_expected_keys);
void ovsdb_bloom_filter_destroy(struct ovsdb_bloom_filter *);

/* Insert a UUID into the filter. */
void ovsdb_bloom_filter_add(struct ovsdb_bloom_filter *,
                            const struct uuid *);

/* Returns true if the UUID MAY be in the set (possible false
 * positive).  Returns false if the UUID is DEFINITELY NOT in
 * the set (guaranteed true negative). */
bool ovsdb_bloom_filter_may_contain(
    const struct ovsdb_bloom_filter *,
    const struct uuid *);

/* Clears the filter (removes all entries).  The capacity
 * remains the same.  Use this before re-populating. */
void ovsdb_bloom_filter_clear(struct ovsdb_bloom_filter *);

/* Returns the number of keys inserted so far. */
size_t ovsdb_bloom_filter_count(
    const struct ovsdb_bloom_filter *);

/* Returns the size of the bit array in bytes. */
size_t ovsdb_bloom_filter_size_bytes(
    const struct ovsdb_bloom_filter *);

#endif /* ovsdb/bloom-filter.h */
