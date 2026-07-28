/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Layered cache structures for the Parquet DocValues codec.
 *
 * <ul>
 *   <li>{@link org.opensearch.parquet.codec.cache.BufferPool} — Layer 5, reusable native
 *       scratch buffers for FFM out-parameters.</li>
 *   <li>{@link org.opensearch.parquet.codec.cache.ColumnPageIndex} — Layer 3 jump table +
 *       Layer 4 page-stat skip, a binary-searchable view of the Parquet page index.</li>
 *   <li>{@link org.opensearch.parquet.codec.cache.PageCache} — Layer 1 page-resident value
 *       cache + Layer 2 presence bitset for one decoded page.</li>
 * </ul>
 */
package org.opensearch.parquet.codec.cache;
