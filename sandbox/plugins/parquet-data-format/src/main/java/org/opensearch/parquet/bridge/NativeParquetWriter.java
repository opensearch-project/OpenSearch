/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.common.SetOnce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe handle for the native Rust Parquet writer with lifecycle management.
 *
 * <p>Wraps the stateless JNI methods in {@link RustBridge} with a file-scoped lifecycle:
 * <ol>
 *   <li>{@code new NativeParquetWriter(filePath, indexName, schemaAddress, sortConfig)} — creates the native writer</li>
 *   <li>{@link #write(long, long)} — sends one or more Arrow batches (repeatable)</li>
 *   <li>{@link #flush()} — finalizes the Parquet file and returns metadata</li>
 *   <li>{@link #sync()} — fsyncs the file to durable storage (calls flush if needed)</li>
 * </ol>
 *
 * <p>This class is not thread-safe. External synchronization is required
 * if instances are shared across threads.
 */
public class NativeParquetWriter {

    private final AtomicBoolean writerFlushed = new AtomicBoolean(false);
    private final String filePath;
    private final SetOnce<ParquetFileMetadata> metadata = new SetOnce<>();

    /**
     * Creates a new NativeParquetWriter.
     *
     * @param filePath      the path to the Parquet file to write
     * @param indexName     the index name for settings lookup
     * @param schemaAddress the native memory address of the Arrow schema
     * @param sortConfig    the sort configuration for the Parquet file
     * @throws IOException if the native writer creation fails
     */
    public NativeParquetWriter(String filePath, String indexName, long schemaAddress, ParquetSortConfig sortConfig) throws IOException {
        this.filePath = filePath;
        RustBridge.createWriter(filePath, indexName, schemaAddress, sortConfig);
    }

    /**
     * Writes an Arrow batch to the Parquet file.
     *
     * @param arrayAddress  the native memory address of the Arrow array
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the write fails or the writer is flushed
     */
    public void write(long arrayAddress, long schemaAddress) throws IOException {
        if (writerFlushed.get()) {
            throw new IOException("Cannot write to flushed Parquet writer: " + filePath);
        }
        RustBridge.write(filePath, arrayAddress, schemaAddress);
    }

    /**
     * Finalizes the Parquet file and returns metadata.
     *
     * @return the file metadata
     * @throws IOException if the finalization fails
     */
    public ParquetFileMetadata flush() throws IOException {
        if (writerFlushed.compareAndSet(false, true)) {
            metadata.set(RustBridge.finalizeWriter(filePath));
        }
        return metadata.get();
    }

    /**
     * Syncs the Parquet file to disk.
     * If flush has not been called yet, it will be called first.
     *
     * @throws IOException if the sync fails
     */
    public void sync() throws IOException {
        if (!writerFlushed.get()) {
            flush();
        }
        RustBridge.syncToDisk(filePath);
    }

    /**
     * Returns the Parquet file metadata captured after flushing the writer.
     *
     * @return the file metadata, or null if the writer has not been flushed
     */
    public ParquetFileMetadata getMetadata() {
        return metadata.get();
    }

}
