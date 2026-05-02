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

#ifndef OVSDB_INDEX_CONFIG_H
#define OVSDB_INDEX_CONFIG_H 1

#include <stdbool.h>
#include <stddef.h>

/* Index configuration file parser.
 *
 * Reads an INI-style config file that declares additional indexes
 * beyond what the schema provides.  Format:
 *
 *   [TableName]
 *   bloom = true|false       (default: true)
 *   hash = column_name       (one per line, multiple allowed)
 *
 * Schema-declared indexes are always created.  The config file
 * adds ADDITIONAL indexes or overrides the bloom setting. */

struct ovsdb_index_config;

/* Parse a config file.  Returns NULL on error (logged via VLOG). */
struct ovsdb_index_config *ovsdb_index_config_from_file(
    const char *filename);

/* Destroy a config. */
void ovsdb_index_config_destroy(struct ovsdb_index_config *);

/* Query: extra hash column names for a table.
 * Returns the count and sets *column_names to an internal array
 * (do not free).  Returns 0 if table not in config. */
size_t ovsdb_index_config_get_hash_columns(
    const struct ovsdb_index_config *,
    const char *table_name,
    const char ***column_names);

/* Query: should bloom be created for this table?
 * Returns true (default) unless config says "bloom = false". */
bool ovsdb_index_config_get_bloom(
    const struct ovsdb_index_config *,
    const char *table_name);

#endif /* ovsdb/index-config.h */
