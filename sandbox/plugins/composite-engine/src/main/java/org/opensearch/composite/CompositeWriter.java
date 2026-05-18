/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A composite {@link Writer} that wraps one {@link Writer} per registered data format
 * and delegates write operations to each per-format writer.
 * <p>
 * Constructed from a {@link CompositeIndexingExecutionEngine}, it iterates the engine's
 * delegates to create per-format writers. The primary format's writer is always first
 * in the {@code writers} list. A {@code postWrite} callback releases this writer back
 * to the pool after each write cycle.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class CompositeWriter implements Writer<CompositeDocumentInput> {

    private static final Logger logger = LogManager.getLogger(CompositeWriter.class);

    private final DataFormat primaryFormat;
    private final Writer<DocumentInput<?>> primaryWriter;
    private final Map<DataFormat, Writer<DocumentInput<?>>> secondaryWritersByFormat;
    private final long writerGeneration;
    private final AtomicReference<WriterState> state;
    private final CompositeShardStats stats;
    private long mappingVersion;

    /**
     * Represents the lifecycle state of a {@link CompositeWriter}.
     * <p>
     * State transitions are one-way from {@code ACTIVE}:
     * <ul>
     *   <li>{@code ACTIVE} → {@code FLUSH_PENDING} (when refresh marks the writer for flushing)</li>
     *   <li>{@code ACTIVE} → {@code ABORTED} (when the writer is aborted due to failure)</li>
     * </ul>
     */
    @ExperimentalApi
    enum WriterState {
        /** Writer is actively accepting documents. */
        ACTIVE,
        /** Writer has been marked for flushing and should not accept new documents. */
        FLUSH_PENDING,
        /** Writer has been aborted due to a failure and should not be reused. */
        ABORTED
    }

    /**
     * Constructs a CompositeWriter from the given engine and writer generation.
     * <p>
     * Creates a per-format {@link Writer} for each delegate engine (primary first,
     * then secondaries). The {@code postWrite} callback releases this writer back
     * to the engine's writer pool.
     *
     * @param engine           the composite indexing execution engine
     * @param config           the writer configuration
     * @param stats            the shard-level stats collector
     */
    @SuppressWarnings("unchecked")
    CompositeWriter(CompositeIndexingExecutionEngine engine, WriterConfig config, CompositeShardStats stats) {
        this.state = new AtomicReference<>(WriterState.ACTIVE);
        this.writerGeneration = config.writerGeneration();
        this.stats = stats;

        IndexingExecutionEngine<?, ?> primaryDelegate = engine.getPrimaryDelegate();
        this.primaryFormat = primaryDelegate.getDataFormat();
        this.primaryWriter = (Writer<DocumentInput<?>>) primaryDelegate.createWriter(config);
        this.mappingVersion = primaryWriter.mappingVersion();

        Map<DataFormat, Writer<DocumentInput<?>>> secondaries = new IdentityHashMap<>();
        for (IndexingExecutionEngine<?, ?> delegate : engine.getSecondaryDelegates()) {
            Writer<DocumentInput<?>> secondary = (Writer<DocumentInput<?>>) delegate.createWriter(config);
            assert secondary.isSchemaMutable() && secondary.mappingVersion() >= this.mappingVersion : "Secondary writer mapping version ["
                + secondary.mappingVersion()
                + "] must match primary ["
                + this.mappingVersion
                + "]";
            secondaries.put(delegate.getDataFormat(), secondary);
        }
        this.secondaryWritersByFormat = Collections.unmodifiableMap(secondaries);
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput doc) throws IOException {
        if (state.get() != WriterState.ACTIVE) {
            throw new IllegalStateException("Cannot add document to writer in state " + state.get());
        }

        long startNanos = System.nanoTime();
        try {
            // Write to primary first
            long primaryStart = System.nanoTime();
            WriteResult primaryResult = primaryWriter.addDoc(doc.getPrimaryInput());
            long primaryElapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - primaryStart);
            CompositeShardStats.FormatStats primaryFormatStats = stats.getOrCreateFormatStats(primaryFormat.name());
            switch (primaryResult) {
                case WriteResult.Success s -> {
                    logger.trace("Successfully added document in primary format [{}]", primaryFormat.name());
                    primaryFormatStats.addDocsIndexed(1);
                    primaryFormatStats.addIndexTimeMillis(primaryElapsed);
                }
                case WriteResult.Failure f -> {
                    logger.debug("Failed to add document in primary format [{}]", primaryFormat.name());
                    primaryFormatStats.incIndexFailures();
                    return primaryResult;
                }
            }

            // Then write to each secondary — keyed lookup by DataFormat (equals/hashCode based on name)
            Map<DataFormat, DocumentInput<?>> secondaryInputs = doc.getSecondaryInputs();
            for (Map.Entry<DataFormat, DocumentInput<?>> inputEntry : secondaryInputs.entrySet()) {
                DataFormat format = inputEntry.getKey();
                Writer<DocumentInput<?>> writer = secondaryWritersByFormat.get(format);
                if (writer == null) {
                    logger.warn("No writer found for secondary format [{}], skipping", format.name());
                    continue;
                }
                long secStart = System.nanoTime();
                WriteResult result = writer.addDoc(inputEntry.getValue());
                long secElapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - secStart);
                CompositeShardStats.FormatStats formatStats = stats.getOrCreateFormatStats(format.name());
                switch (result) {
                    case WriteResult.Success s -> {
                        logger.trace("Successfully added document in secondary format [{}]", format.name());
                        formatStats.addDocsIndexed(1);
                        formatStats.addIndexTimeMillis(secElapsed);
                    }
                    case WriteResult.Failure f -> {
                        logger.debug("Failed to add document in secondary format [{}]", format.name());
                        formatStats.incIndexFailures();
                        return result;
                    }
                }
            }

            return primaryResult;
        } finally {
            stats.addDocsIndexed(1);
            stats.addIndexTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }
    }

    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        setFlushPending();
        long startNanos = System.nanoTime();
        try {
            FileInfos.Builder builder = FileInfos.builder();

            // Flush primary (Parquet) first to get the sort permutation
            long primaryStart = System.nanoTime();
            FileInfos primaryFileInfos = primaryWriter.flush(flushInput);
            stats.getOrCreateFormatStats(primaryFormat.name())
                .addFlushTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - primaryStart));
            Optional<WriterFileSet> primaryWfs = primaryFileInfos.getWriterFileSet(primaryFormat);
            primaryWfs.ifPresent(writerFileSet -> builder.putWriterFileSet(primaryFormat, writerFileSet));

            // Capture the row ID mapping from the primary flush
            RowIdMapping rowIdMapping = primaryFileInfos.rowIdMapping();
            if (rowIdMapping != null) {
                builder.rowIdMapping(rowIdMapping);
            }

            // Build FlushInput for secondaries, carrying the row ID mapping from the primary
            FlushInput secondaryFlushInput = rowIdMapping != null ? new FlushInput(rowIdMapping) : FlushInput.EMPTY;

            // Flush secondaries with the sort permutation context
            for (Map.Entry<DataFormat, Writer<DocumentInput<?>>> entry : secondaryWritersByFormat.entrySet()) {
                long secStart = System.nanoTime();
                FileInfos fileInfos = entry.getValue().flush(secondaryFlushInput);
                stats.getOrCreateFormatStats(entry.getKey().name())
                    .addFlushTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - secStart));
                for (Map.Entry<DataFormat, WriterFileSet> fileEntry : fileInfos.writerFilesMap().entrySet()) {
                    // Secondary format's WriterFileSet must also match this writer's generation
                    assert fileEntry.getValue().writerGeneration() == writerGeneration : "secondary WriterFileSet generation ["
                        + fileEntry.getValue().writerGeneration()
                        + "] for format ["
                        + fileEntry.getKey().name()
                        + "] must match composite writer generation ["
                        + writerGeneration
                        + "]";
                    builder.putWriterFileSet(fileEntry.getKey(), fileEntry.getValue());
                }
            }
            return builder.build();
        } finally {
            stats.incFlushTotal();
            stats.addFlushTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }
    }

    @Override
    public void sync() throws IOException {
        long startNanos = System.nanoTime();
        try {
            primaryWriter.sync();
            for (Writer<DocumentInput<?>> writer : secondaryWritersByFormat.values()) {
                writer.sync();
            }
        } finally {
            stats.incSyncTotal();
            stats.addSyncTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }
    }

    @Override
    public long generation() {
        return getWriterGeneration();
    }

    @Override
    public boolean isSchemaMutable() {
        if (primaryWriter.isSchemaMutable() == false) return false;
        for (Writer<?> w : secondaryWritersByFormat.values()) {
            if (w.isSchemaMutable() == false) return false;
        }
        return true;
    }

    @Override
    public long mappingVersion() {
        return mappingVersion;
    }

    @Override
    public void updateMappingVersion(long newVersion) {
        if (newVersion > this.mappingVersion) {
            this.mappingVersion = newVersion;
            primaryWriter.updateMappingVersion(newVersion);
            for (Writer<?> w : secondaryWritersByFormat.values()) {
                w.updateMappingVersion(newVersion);
            }
        }
    }

    @Override
    public void close() throws IOException {
        primaryWriter.close();
        for (Writer<DocumentInput<?>> writer : secondaryWritersByFormat.values()) {
            writer.close();
        }
    }

    /**
     * Searches primary and secondary formats by {@link DataFormat#name()}.
     */
    @Override
    public Optional<Writer<?>> getWriterForFormat(String formatName) {
        if (primaryFormat.name().equals(formatName)) return Optional.of(primaryWriter);
        return secondaryWritersByFormat.entrySet()
            .stream()
            .filter(e -> e.getKey().name().equals(formatName))
            .<Writer<?>>map(Map.Entry::getValue)
            .findFirst();
    }

    /**
     * Returns the writer generation number.
     *
     * @return the writer generation
     */
    long getWriterGeneration() {
        return writerGeneration;
    }

    /**
     * Marks this writer as aborted. Only transitions from {@code ACTIVE}.
     *
     * @throws IllegalStateException if the writer is not in {@code ACTIVE} state
     */
    void abort() {
        if (this.state.compareAndSet(WriterState.ACTIVE, WriterState.ABORTED) == false) {
            throw new IllegalStateException("Cannot abort writer in state " + state.get());
        }
    }

    /**
     * Returns whether this writer has been aborted.
     *
     * @return {@code true} if aborted
     */
    boolean isAborted() {
        return getState() == WriterState.ABORTED;
    }

    /**
     * Marks this writer as having a pending flush. Only transitions from {@code ACTIVE}.
     *
     * @throws IllegalStateException if the writer is not in {@code ACTIVE} state
     */
    void setFlushPending() {
        if (this.state.compareAndSet(WriterState.ACTIVE, WriterState.FLUSH_PENDING) == false) {
            throw new IllegalStateException("Cannot set flush pending on writer in state " + state.get());
        }
    }

    /**
     * Returns whether this writer has a pending flush.
     *
     * @return {@code true} if a flush is pending
     */
    boolean isFlushPending() {
        return getState() == WriterState.FLUSH_PENDING;
    }

    /**
     * Returns the current state of this writer for testing purpose.
     *
     * @return the writer state
     */
    WriterState getState() {
        return state.get();
    }
}
