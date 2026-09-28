/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * JNI bridge to the native Rust Parquet writer.
 *
 * <p>This package provides the interface between Java and the native Rust library
 * ({@code parquet_dataformat_jni}) that performs the actual Parquet file generation.
 * Communication uses the <a href="https://arrow.apache.org/docs/format/CDataInterface.html">
 * Arrow C Data Interface</a>, exchanging memory addresses of Arrow arrays and schemas
 * across the JNI boundary without data copying.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link org.opensearch.parquet.bridge.RustBridge} — Static JNI method declarations
 *       and native library loading. Writer lifecycle methods are package-private.</li>
 *   <li>{@link org.opensearch.parquet.bridge.NativeParquetWriter} — Type-safe handle wrapping
 *       the native writer with lifecycle management (create → write → close → flush).</li>
 *   <li>{@link org.opensearch.nativebridge.spi.ArrowExport} — RAII container for exported
 *       Arrow C Data Interface pointers ({@code ArrowArray} + {@code ArrowSchema}).</li>
 *   <li>{@link org.opensearch.parquet.bridge.ParquetFileMetadata} — Immutable record of
 *       metadata returned by the native writer after file finalization.</li>
 * </ul>
 *
 * <h2>Writer Lifecycle</h2>
 * <pre>
 * createWriter(filePath, schemaAddress)  — initialize native writer with Arrow schema
 * write(arrayAddress, schemaAddress)     — send a batch of Arrow data (repeatable)
 * finalizeWriter(filePath)                  — finalize Parquet file, returns metadata
 * syncToDisk(filePath)                  — fsync the file to durable storage
 * </pre>
 *
 * @see org.opensearch.parquet.bridge.NativeParquetWriter
 * @see org.opensearch.parquet.bridge.RustBridge
 */
package org.opensearch.parquet.bridge;
