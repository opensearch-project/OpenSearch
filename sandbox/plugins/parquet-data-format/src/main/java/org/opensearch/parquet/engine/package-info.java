/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Parquet indexing engine and data format descriptor.
 *
 * <p>This package implements the {@code IndexingExecutionEngine} interface from OpenSearch's
 * data format framework, providing the entry point for shard-level Parquet file generation.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link org.opensearch.parquet.engine.ParquetDataFormat} — Data format descriptor
 *       declaring the format name ({@code "parquet"}), priority, and supported field types.</li>
 *   <li>{@link org.opensearch.parquet.engine.ParquetIndexingEngine} — Per-shard engine that
 *       creates writers for each writer generation, manages the shared Arrow buffer pool,
 *       and reports native memory usage.</li>
 * </ul>
 *
 * @see org.opensearch.parquet.engine.ParquetIndexingEngine
 * @see org.opensearch.parquet.engine.ParquetDataFormat
 */
package org.opensearch.parquet.engine;
