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
 * Type-safe handle for native Parquet writer with lifecycle management.
 * Delegates to {@link RustBridge} for actual native calls and handles error checking.
 *
 * <p>Expected lifecycle: {@code createWriter → write (1+) → close (finalizes Parquet) → flush (syncs to disk)}
 */
public class NativeParquetWriter implements Closeable {

    private final AtomicBoolean writerClosed = new AtomicBoolean(false);
    private final String filePath;
    private ParquetFileMetadata metadata;

    public NativeParquetWriter(String filePath, long schemaAddress) throws IOException {
        this.filePath = filePath;
        int result = RustBridge.createWriter(filePath, schemaAddress);
        if (result != 0) {
            throw new IOException("Failed to create native Parquet writer for: " + filePath);
        }
    }

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
    public void close() {
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

    public ParquetFileMetadata getMetadata() {
        return metadata;
    }

}
