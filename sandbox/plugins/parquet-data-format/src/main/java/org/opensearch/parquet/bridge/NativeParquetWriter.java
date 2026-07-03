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
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;

import java.io.IOException;
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
    private volatile boolean initialized = false;

    /**
     * Creates a new NativeParquetWriter handle. Does not create the native writer —
     * call {@link #initialize(String, long, ParquetSortConfig, long)} before the first write.
     *
     * @param filePath the path to the Parquet file to write
     * @param stats shard-level stats tracker
     */
    public NativeParquetWriter(String filePath, ParquetShardStatsTracker stats) {
        this.filePath = filePath;
        this.stats = stats != null ? stats : new ParquetShardStatsTracker();
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
     * Convenience constructor that creates AND immediately initializes the native writer.
     *
     * <p>Intended for tests and simple write-paths that do not need deferred initialization.
     * Uses an empty sort config and writer generation 0. The {@code encryptionInputs}
     * are zeroed immediately after the native writer is created.
     *
     * @param filePath         path to the Parquet file to write
     * @param schemaAddress    native memory address of the Arrow schema
     * @param encryptionInputs per-file PME inputs; {@code null} writes an unencrypted file
     * @throws IOException if the native writer creation fails
     */
    public NativeParquetWriter(String filePath, long schemaAddress, PmeFileEncryptionInputs encryptionInputs) throws IOException {
        this(filePath);
        initialize(
            java.nio.file.Path.of(filePath).getFileName().toString(),
            schemaAddress,
            ParquetSortConfig.empty(),
            0L,
            encryptionInputs
        );
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
    public void initialize(String indexName, long schemaAddress, ParquetSortConfig sortConfig, long writerGeneration) throws IOException {
        initialize(indexName, schemaAddress, sortConfig, writerGeneration, null);
    }

    /**
     * Initializes the native Rust Parquet writer with optional PME encryption.
     *
     * @param indexName         the index name for settings lookup
     * @param schemaAddress     the native memory address of the Arrow schema
     * @param sortConfig        the sort configuration for the Parquet file
     * @param writerGeneration  the writer generation to store in file metadata
     * @param encryptionInputs  per-file PME inputs; {@code null} writes an unencrypted file
     * @throws IOException if the native writer creation fails
     * @throws IllegalStateException if already initialized
     */
    public void initialize(String indexName, long schemaAddress, ParquetSortConfig sortConfig, long writerGeneration,
                           PmeFileEncryptionInputs encryptionInputs) throws IOException {
        if (initialized) {
            throw new IllegalStateException("Writer already initialized: " + filePath);
        }
        try {
            RustBridge.createWriter(filePath, indexName, schemaAddress, sortConfig, writerGeneration, encryptionInputs);
        } finally {
            if (encryptionInputs != null) {
                encryptionInputs.zero();
            }
        }
        initialized = true;
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
    public void write(long arrayAddress, long schemaAddress) throws IOException {
        if (writerFlushed.get()) {
            throw new IOException("Cannot write to flushed Parquet writer: " + filePath);
        }
        if (initialized == false) {
            throw new IllegalStateException("Writer not initialized: " + filePath);
        }
        StatsRecorder.recordOutcome(
            () -> RustBridge.write(filePath, arrayAddress, schemaAddress),
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
    public ParquetFileMetadata flush() throws IOException {
        if (writerFlushed.compareAndSet(false, true)) {
            if (initialized) {
                StatsRecorder.recordOutcome(() -> {
                    RustBridge.WriterFinalizeResult result = RustBridge.finalizeWriter(filePath);
                    if (result != null) {
                        metadata.set(result.metadata());
                        if (result.rowIdMapping() != null) {
                            rowIdMapping.set(result.rowIdMapping());
                        }
                    }
                }, stats::addNativeFinalizeTimeMillis, stats::incNativeFinalizeTotal, stats::incNativeFinalizeFailures);
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

}
