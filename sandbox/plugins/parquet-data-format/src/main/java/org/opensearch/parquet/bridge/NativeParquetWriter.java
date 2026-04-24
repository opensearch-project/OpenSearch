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
 * <p>Handle-based: the Rust side holds the writer state, Java holds the handle.
 * No global map on the Rust side — each writer is independent.
 *
 * <ol>
 *   <li>{@code new NativeParquetWriter(storePointer, objectPath, schemaAddress)} — creates the native writer</li>
 *   <li>{@link #write(long, long)} — sends one or more Arrow batches (repeatable)</li>
 *   <li>{@link #flush()} — finalizes the Parquet file, uploads to ObjectStore, returns metadata</li>
 * </ol>
 *
 * <p>This class is not thread-safe. External synchronization is required
 * if instances are shared across threads.
 */
public class NativeParquetWriter {

    private final AtomicBoolean writerFlushed = new AtomicBoolean(false);
    private final long writerHandle;
    private final String objectPath;
    private final SetOnce<ParquetFileMetadata> metadata = new SetOnce<>();

    /**
     * Creates a new NativeParquetWriter backed by an ObjectStore.
     *
     * @param storePointer  the native ObjectStore pointer (shard-scoped)
     * @param objectPath    relative path within the store (e.g., "gen_1/data_0.parquet")
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the native writer creation fails
     */
    public NativeParquetWriter(long storePointer, String objectPath, long schemaAddress) throws IOException {
        this.objectPath = objectPath;
        this.writerHandle = RustBridge.createWriter(storePointer, objectPath, schemaAddress);
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
            throw new IOException("Cannot write to flushed Parquet writer: " + objectPath);
        }
        RustBridge.write(writerHandle, arrayAddress, schemaAddress);
    }

    /**
     * Finalizes the Parquet file, uploads to ObjectStore, and returns metadata.
     * The writer handle is consumed regardless of success or failure.
     *
     * @return the file metadata
     * @throws IOException if the finalization fails
     */
    public ParquetFileMetadata flush() throws IOException {
        if (writerFlushed.compareAndSet(false, true)) {
            metadata.set(RustBridge.finalizeWriter(writerHandle));
        }
        return metadata.get();
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
