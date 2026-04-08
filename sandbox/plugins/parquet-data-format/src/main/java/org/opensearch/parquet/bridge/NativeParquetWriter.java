/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SetOnce;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe handle for the native Rust async Parquet writer with lifecycle management.
 *
 * <p>Wraps an opaque native handle backed by an {@code AsyncArrowWriter<ParquetObjectWriter>}
 * on the Rust side. The writer targets an {@link NativeObjectStore} backend (local, S3, GCS,
 * Azure) and uses a static tokio runtime for async IO.
 *
 * <p>A {@link Cleaner} is registered as a safety net to release the native handle if
 * {@link #close()} is not called before the object becomes unreachable. This prevents
 * native memory leaks but should not be relied upon — always call {@link #close()} explicitly.
 *
 * <p>This class is not thread-safe. External synchronization is required
 * if instances are shared across threads.
 */
public class NativeParquetWriter {

    private static final Logger logger = LogManager.getLogger(NativeParquetWriter.class);
    private static final Cleaner CLEANER = Cleaner.create();

    private final AtomicBoolean writerFlushed = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String path;
    private final long nativeHandle;
    private final SetOnce<ParquetFileMetadata> metadata = new SetOnce<>();
    private final Cleaner.Cleanable cleanable;

    /**
     * Creates a new NativeParquetWriter backed by the given object store.
     *
     * @param store         the native object store to write to
     * @param path          the object path for the Parquet file
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the native writer creation fails
     */
    public NativeParquetWriter(NativeObjectStore store, String path, long schemaAddress) throws IOException {
        this.path = path;
        this.nativeHandle = RustBridge.createWriter(store.getNativeHandle(), path, schemaAddress);
        if (this.nativeHandle == 0) {
            throw new IOException("Native writer creation returned null handle for: " + path);
        }
        this.cleanable = CLEANER.register(this, new CleanupAction(nativeHandle, path));
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
            throw new IOException("Cannot write to flushed Parquet writer: " + path);
        }
        RustBridge.write(nativeHandle, arrayAddress, schemaAddress);
    }

    /**
     * Finalizes the Parquet file and returns metadata.
     *
     * @return the file metadata
     * @throws IOException if the finalization fails
     */
    public ParquetFileMetadata flush() throws IOException {
        if (writerFlushed.compareAndSet(false, true)) {
            metadata.set(RustBridge.finalizeWriter(nativeHandle));
        }
        return metadata.get();
    }

    /**
     * Returns the native memory used by this writer's internal buffers.
     *
     * @return bytes of native memory used, or 0 if the writer has been closed
     */
    public long getMemoryUsage() {
        if (closed.get()) {
            return 0;
        }
        return RustBridge.getWriterMemoryUsage(nativeHandle);
    }

    /**
     * Returns the Parquet file metadata captured after flushing the writer.
     *
     * @return the file metadata, or null if the writer has not been flushed
     */
    public ParquetFileMetadata getMetadata() {
        return metadata.get();
    }

    /**
     * Releases the native writer resources. Safe to call multiple times.
     * Deregisters the Cleaner so the destructor does not run twice.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cleanable.clean();
        }
    }

    /**
     * Static cleanup action that captures only the native handle value, not the
     * enclosing NativeParquetWriter instance. This is critical — if the action
     * held a reference to the writer, the Cleaner would prevent GC of the writer
     * and the cleanup would never trigger.
     */
    private static class CleanupAction implements Runnable {
        private final long handle;
        private final String path;

        CleanupAction(long handle, String path) {
            this.handle = handle;
            this.path = path;
        }

        @Override
        public void run() {
            logger.warn("Native Parquet writer for [{}] was not explicitly closed — releasing via Cleaner", path);
            RustBridge.destroyWriter(handle);
        }
    }
}
