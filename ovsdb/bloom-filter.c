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

#include <config.h>

#include "bloom-filter.h"

#include <string.h>

#include "openvswitch/uuid.h"
#include "hash.h"
#include "util.h"

/* Bloom filter parameters.
 *
 * With 10 bits per key and 7 hash functions, the theoretical
 * false positive rate is:
 *   (1 - e^(-k*n/m))^k ≈ 0.82% for n = m/10
 *
 * We use double-hashing to derive k hash functions from two
 * independent hashes:  h_i(x) = h1(x) + i * h2(x)  mod m
 * This is a well-known technique (Kirsch & Mitzenmacher, 2006)
 * that avoids computing k independent hashes.
 *
 * Reference: Kirsch & Mitzenmacher, "Less Hashing, Same
 * Performance: Building a Better Bloom Filter", 2006. */
#define BLOOM_BITS_PER_KEY  10
#define BLOOM_NUM_HASHES    7

struct ovsdb_bloom_filter {
    uint8_t *bits;       /* Bit array. */
    size_t n_bits;       /* Total bits (always a multiple of 8). */
    size_t n_keys;       /* Number of keys inserted. */
};

/* Returns the bit index for hash function 'i' given two base
 * hashes 'h1' and 'h2' and a filter with 'n_bits' total bits. */
static inline size_t
bloom_bit_index(uint32_t h1, uint32_t h2, int i, size_t n_bits)
{
    return (size_t)(h1 + (uint32_t) i * h2) % n_bits;
}

struct ovsdb_bloom_filter *
ovsdb_bloom_filter_create(size_t n_expected_keys)
{
    struct ovsdb_bloom_filter *bf;
    size_t n_bits;

    bf = xmalloc(sizeof *bf);

    /* Compute bit array size: n_expected * BITS_PER_KEY, rounded
     * up to a multiple of 8.  Minimum 64 bits to avoid degenerate
     * tiny filters. */
    n_bits = n_expected_keys * BLOOM_BITS_PER_KEY;
    if (n_bits < 64) {
        n_bits = 64;
    }
    n_bits = ROUND_UP(n_bits, 8);

    bf->n_bits = n_bits;
    bf->bits = xzalloc(n_bits / 8);
    bf->n_keys = 0;

    return bf;
}

void
ovsdb_bloom_filter_destroy(struct ovsdb_bloom_filter *bf)
{
    if (bf) {
        free(bf->bits);
        free(bf);
    }
}

void
ovsdb_bloom_filter_add(struct ovsdb_bloom_filter *bf,
                       const struct uuid *uuid)
{
    uint32_t h1, h2;
    int i;

    /* Two independent hashes from the UUID bytes. */
    h1 = hash_bytes(uuid, sizeof *uuid, 0);
    h2 = hash_bytes(uuid, sizeof *uuid, h1);

    /* Ensure h2 is odd so the probes cover all positions
     * modulo n_bits (a standard bloom filter trick). */
    h2 |= 1;

    for (i = 0; i < BLOOM_NUM_HASHES; i++) {
        size_t bit = bloom_bit_index(h1, h2, i, bf->n_bits);
        bf->bits[bit / 8] |= (uint8_t)(1u << (bit % 8));
    }

    bf->n_keys++;
}

bool
ovsdb_bloom_filter_may_contain(const struct ovsdb_bloom_filter *bf,
                               const struct uuid *uuid)
{
    uint32_t h1, h2;
    int i;

    if (!bf || bf->n_keys == 0) {
        return false;
    }

    h1 = hash_bytes(uuid, sizeof *uuid, 0);
    h2 = hash_bytes(uuid, sizeof *uuid, h1);
    h2 |= 1;

    for (i = 0; i < BLOOM_NUM_HASHES; i++) {
        size_t bit = bloom_bit_index(h1, h2, i, bf->n_bits);
        if (!(bf->bits[bit / 8] & (1u << (bit % 8)))) {
            return false;  /* Definitely not in the set. */
        }
    }

    return true;  /* Maybe in the set. */
}

void
ovsdb_bloom_filter_clear(struct ovsdb_bloom_filter *bf)
{
    if (bf) {
        memset(bf->bits, 0, bf->n_bits / 8);
        bf->n_keys = 0;
    }
}

size_t
ovsdb_bloom_filter_count(const struct ovsdb_bloom_filter *bf)
{
    return bf ? bf->n_keys : 0;
}

size_t
ovsdb_bloom_filter_size_bytes(const struct ovsdb_bloom_filter *bf)
{
    return bf ? bf->n_bits / 8 : 0;
}
