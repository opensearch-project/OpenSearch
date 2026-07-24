/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.common.SetOnce;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.parquet.stats.ParquetShardStatsTracker;
import org.opensearch.plugin.stats.StatsRecorder;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe handle for the native Rust Parquet writer with lifecycle management.
 *
 * <p>Wraps the stateless JNI methods in {@link RustBridge} with a file-scoped lifecycle:
 * <ol>
 *   <li>{@code new NativeParquetWriter(filePath)} — creates the handle (no native call)</li>
 *   <li>{@link #initialize(String, long, ParquetSortConfig, long)} — creates the native writer with the final schema</li>
 *   <li>{@link #write(long, long)} — sends one or more Arrow batches (repeatable)</li>
 *   <li>{@link #flush()} — finalizes the Parquet file and returns metadata</li>
 * </ol>
 *
 * <p>This class is not thread-safe. External synchronization is required
 * if instances are shared across threads.
 */
public class NativeParquetWriter {

    private final AtomicBoolean writerFlushed = new AtomicBoolean(false);
    private final String filePath;
    private final SetOnce<ParquetFileMetadata> metadata = new SetOnce<>();
    private final SetOnce<RowIdMapping> rowIdMapping = new SetOnce<>();
    private final ParquetShardStatsTracker stats;
    /**
     * Shard-scoped native ObjectStore handle ({@code Box<Arc<dyn ObjectStore>>} pointer) minted by
     * {@code os_create_local_store} and owned by the engine, or 0 for the legacy local-file path.
     * Passed to the native writer at {@link #initialize} time so finalize publishes through the store.
     */
    private final long storePtr;
    private volatile boolean initialized = false;

    /** Reclaims leaked native writers if this object is GC'd without an explicit close/flush. */
    private static final Cleaner CLEANER = Cleaner.create();

    /**
     * Opaque native handle (a {@code Box<WriterState>} pointer) minted by {@link #initialize};
     * 0 until initialized. Every native call that dereferences it runs inside a {@code synchronized}
     * method, so they are serialized (write/finalize/free/memory never race on the same handle).
     */
    private volatile long handle = 0L;
    /** Set once the handle has been consumed (by finalize) or freed (by close/Cleaner). */
    private final AtomicBoolean released = new AtomicBoolean(false);
    /** Cleaner registration; {@code clean()} on close deregisters the GC backstop. */
    private Cleaner.Cleanable cleanable;

    /**
     * Creates a new NativeParquetWriter handle. Does not create the native writer —
     * call {@link #initialize(String, long, ParquetSortConfig, long)} before the first write.
     *
     * @param filePath the path to the Parquet file to write
     * @param stats shard-level stats tracker
     */
    public NativeParquetWriter(String filePath, ParquetShardStatsTracker stats) {
        this(filePath, stats, 0L);
    }

    /**
     * Creates a new NativeParquetWriter handle bound to a shard-scoped ObjectStore.
     *
     * @param filePath the path to the Parquet file to write
     * @param stats shard-level stats tracker
     * @param storePtr shard ObjectStore handle ({@code os_create_local_store} pointer), or 0 for
     *                 the legacy local-file path
     */
    public NativeParquetWriter(String filePath, ParquetShardStatsTracker stats, long storePtr) {
        this.filePath = filePath;
        this.stats = stats;
        this.storePtr = storePtr;
    }

    /**
     * Creates a new NativeParquetWriter handle without stats collection.
     *
     * @param filePath the path to the Parquet file to write
     */
    public NativeParquetWriter(String filePath) {
        this(filePath, new ParquetShardStatsTracker());
    }

    /**
     * Initializes the native Rust Parquet writer with the given schema.
     * Must be called exactly once before the first {@link #write}.
     *
     * @param indexName         the index name for settings lookup
     * @param schemaAddress     the native memory address of the Arrow schema
     * @param sortConfig        the sort configuration for the Parquet file
     * @param writerGeneration  the writer generation to store in file metadata
     * @throws IOException if the native writer creation fails
     * @throws IllegalStateException if already initialized
     */
    public synchronized void initialize(String indexName, long schemaAddress, ParquetSortConfig sortConfig, long writerGeneration)
        throws IOException {
        if (initialized) {
            throw new IllegalStateException("Writer already initialized: " + filePath);
        }
        handle = RustBridge.createWriter(filePath, indexName, schemaAddress, sortConfig, writerGeneration, storePtr);
        initialized = true;
        // GC backstop: if this writer is dropped without close()/flush(), free the native handle.
        // HandleCleanup holds only the primitive handle + shared released flag (never `this`).
        cleanable = CLEANER.register(this, new HandleCleanup(handle, released));
    }

    /**
     * Returns whether the native writer has been initialized.
     *
     * @return true if {@link #initialize} has been called
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Writes an Arrow batch to the Parquet file.
     *
     * @param arrayAddress  the native memory address of the Arrow array
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the write fails or the writer is flushed
     * @throws IllegalStateException if the writer has not been initialized
     */
    public synchronized void write(long arrayAddress, long schemaAddress) throws IOException {
        if (writerFlushed.get()) {
            throw new IOException("Cannot write to flushed Parquet writer: " + filePath);
        }
        if (initialized == false) {
            throw new IllegalStateException("Writer not initialized: " + filePath);
        }
        StatsRecorder.recordOutcome(
            () -> RustBridge.write(handle, arrayAddress, schemaAddress),
            stats::addNativeWriteTimeMillis,
            stats::incNativeWriteTotal,
            stats::incNativeWriteFailures
        );
    }

    /**
     * Finalizes the Parquet file and returns metadata. If the writer was never initialized
     * (no documents were written), returns null rather than throwing — this is expected
     * when a writer is flushed during refresh without having received any documents.
     *
     * @return the file metadata, or null if the writer was never initialized
     * @throws IOException if the finalization fails
     */
    public synchronized ParquetFileMetadata flush() throws IOException {
        if (writerFlushed.compareAndSet(false, true)) {
            if (initialized) {
                try {
                    StatsRecorder.recordOutcome(() -> {
                        RustBridge.WriterFinalizeResult result = RustBridge.finalizeWriter(handle);
                        if (result != null) {
                            metadata.set(result.metadata());
                            if (result.rowIdMapping() != null) {
                                rowIdMapping.set(result.rowIdMapping());
                            }
                        }
                    }, stats::addNativeFinalizeTimeMillis, stats::incNativeFinalizeTotal, stats::incNativeFinalizeFailures);
                } finally {
                    // finalize_writer reclaims the native Box regardless of outcome, so the
                    // handle is consumed — mark released so close()/Cleaner never double-free.
                    released.set(true);
                }
            }
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

    /**
     * Returns the row ID mapping produced during sort-on-close as a memory-efficient
     * packed mapping, or null if no sorting was configured or the file was empty.
     */
    public RowIdMapping getRowIdMapping() {
        return rowIdMapping.get();
    }

    /**
     * Returns the native memory currently reserved by this writer, or 0 if it was never
     * initialized or has already been finalized/freed. Blocks if a native write/finalize is in
     * flight (stats tolerate waiting; the write path is unaffected functionally).
     */
    public synchronized long getNativeBytesUsed() {
        if (initialized == false || released.get()) {
            return 0L;
        }
        return RustBridge.getWriterMemoryUsage(handle);
    }

    /**
     * Releases the native writer if it was not already consumed by {@link #flush()}. Idempotent —
     * safe to call multiple times and after flush. This is the guaranteed cleanup path invoked on
     * writer teardown; the {@link Cleaner} is only a backstop for the case where {@code close()} is
     * never called (e.g. the writer is dropped after a failure).
     */
    public synchronized void close() {
        if (handle != 0L && released.compareAndSet(false, true)) {
            RustBridge.freeWriter(handle);
        }
        if (cleanable != null) {
            cleanable.clean();
        }
    }

    /**
     * Cleaner action reclaiming the native writer if the {@link NativeParquetWriter} becomes
     * unreachable without an explicit {@link #close()}/{@link #flush()}. It captures only the
     * primitive handle and the shared {@code released} flag — never the enclosing writer — so it
     * cannot keep the writer reachable, and the CAS guarantees free happens at most once.
     */
    private static final class HandleCleanup implements Runnable {
        private final long handle;
        private final AtomicBoolean released;

        HandleCleanup(long handle, AtomicBoolean released) {
            this.handle = handle;
            this.released = released;
        }

        @Override
        public void run() {
            if (handle != 0L && released.compareAndSet(false, true)) {
                RustBridge.freeWriter(handle);
            }
        }
    }

}
