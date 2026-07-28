/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Cache-aware Lucene DocValues iterators backed by Parquet columns: numeric, sorted-numeric,
 * binary, sorted, and sorted-set. Each consults the column reader's page cache for hot-path
 * reads and crosses the FFM boundary only on a page miss.
 */
package org.opensearch.parquet.codec.iter;
