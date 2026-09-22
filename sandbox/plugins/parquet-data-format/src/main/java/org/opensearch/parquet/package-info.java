/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Parquet data format plugin for OpenSearch.
 *
 * <p>This plugin enables columnar Parquet file generation from OpenSearch indexing operations
 * using Apache Arrow as the in-memory columnar representation and a native Rust backend
 * (via JNI) for high-performance Parquet file writing.
 *
 * <h2>Architecture Overview</h2>
 * <p>The plugin follows a layered pipeline:
 * <ol>
 *   <li><strong>Plugin layer</strong> ({@link org.opensearch.parquet.ParquetDataFormatPlugin}) —
 *       Entry point that registers settings and creates the indexing engine per shard.</li>
 *   <li><strong>Engine layer</strong> ({@link org.opensearch.parquet.engine}) —
 *       Implements the {@code IndexingExecutionEngine} interface, managing writer lifecycle
 *       and file generation per writer generation.</li>
 *   <li><strong>Writer layer</strong> ({@link org.opensearch.parquet.writer}) —
 *       Accepts documents as field-value pairs, delegates batching to the VSR layer.</li>
 *   <li><strong>VSR layer</strong> ({@link org.opensearch.parquet.vsr}) —
 *       Manages Arrow {@code VectorSchemaRoot} instances with ACTIVE/FROZEN/CLOSED lifecycle,
 *       row-count-based rotation, and memory-bounded batching.</li>
 *   <li><strong>Bridge layer</strong> ({@link org.opensearch.parquet.bridge}) —
 *       JNI bridge to the native Rust Parquet writer, handling Arrow C Data Interface
 *       pointer exchange and native resource lifecycle.</li>
 *   <li><strong>Fields layer</strong> ({@link org.opensearch.parquet.fields}) —
 *       Extensible type mapping from OpenSearch field types to Arrow vector types,
 *       organized via a plugin registry pattern.</li>
 *   <li><strong>Memory layer</strong> ({@link org.opensearch.parquet.memory}) —
 *       Arrow buffer allocation with configurable limits derived from node settings.</li>
 * </ol>
 *
 * <h2>Data Flow</h2>
 * <pre>
 * Document → ParquetDocumentInput (field collection)
 *          → VSRManager.addDocument (field transfer to Arrow vectors)
 *          → VSRPool rotation (when row threshold reached)
 *          → NativeParquetWriter.write (Arrow C Data export to Rust)
 *          → flush/close (Parquet file finalization + fsync)
 * </pre>
 *
 * @see org.opensearch.parquet.ParquetDataFormatPlugin
 * @see org.opensearch.parquet.ParquetSettings
 */
package org.opensearch.parquet;
