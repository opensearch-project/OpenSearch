/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe handle for the native Rust Parquet writer with lifecycle management.
 *
 * <p>Wraps the stateless JNI methods in {@link RustBridge} with a file-scoped lifecycle:
 * <ol>
 *   <li>{@code new NativeParquetWriter(filePath, schemaAddress)} — creates the native writer</li>
 *   <li>{@link #write(long, long)} — sends one or more Arrow batches (repeatable)</li>
 *   <li>{@link #close()} — finalizes the Parquet file and captures metadata</li>
 *   <li>{@link #flush()} — fsyncs the file to durable storage (calls close if needed)</li>
 * </ol>
 *
 * <p>Thread safety: the {@code writerClosed} flag uses {@link java.util.concurrent.atomic.AtomicBoolean}
 * to guard against double-close and write-after-close, but concurrent writes are not supported.
 */
public class NativeParquetWriter implements Closeable {

    private final AtomicBoolean writerClosed = new AtomicBoolean(false);
    private final String filePath;
    private ParquetFileMetadata metadata;

    /**
     * Creates a new NativeParquetWriter.
     *
     * @param filePath      the path to the Parquet file to write
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the native writer creation fails
     */
    public NativeParquetWriter(String filePath, long schemaAddress) throws IOException {
        this.filePath = filePath;
        int result = RustBridge.createWriter(filePath, schemaAddress);
        if (result != 0) {
            throw new IOException("Failed to create native Parquet writer for: " + filePath);
        }
    }

    /**
     * Writes an Arrow batch to the Parquet file.
     *
     * @param arrayAddress  the native memory address of the Arrow array
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the write fails or the writer is closed
     */
    public void write(long arrayAddress, long schemaAddress) throws IOException {
        if (writerClosed.get()) {
            throw new IOException("Cannot write to closed Parquet writer: " + filePath);
        }
        int result = RustBridge.write(filePath, arrayAddress, schemaAddress);
        if (result != 0) {
            throw new IOException("Failed to write data to Parquet file: " + filePath);
        }
    }

    @Override
    public void close() throws IOException {
        if (writerClosed.compareAndSet(false, true)) {
            metadata = RustBridge.closeWriter(filePath);
        }
    }

    /**
     * Syncs the Parquet file to disk. Must be called after {@link #close()}.
     * If close has not been called yet, it will be called first.
     */
    public void flush() throws IOException {
        if (!writerClosed.get()) {
            close();
        }
        int result = RustBridge.flushToDisk(filePath);
        if (result != 0) {
            throw new IOException("Failed to flush Parquet file to disk: " + filePath);
        }
    }

    /**
     * Returns the Parquet file metadata captured after closing the writer.
     *
     * @return the file metadata, or null if the writer has not been closed
     */
    public ParquetFileMetadata getMetadata() {
        return metadata;
    }

}
