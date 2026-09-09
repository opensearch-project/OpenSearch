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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.composite.stats.CompositeShardStatsTracker;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    private final FailureHandlerStrategy failureHandler;
    private final CompositeShardStatsTracker statsTracker;
    private volatile boolean closed;
    private long mappingVersion;
    /** Successful addDoc count — every incoming rowId must equal this. */
    private long acceptedRows = 0L;

    /**
     * Constructs a CompositeWriter from the given engine and writer generation.
     * <p>
     * Creates a per-format {@link Writer} for each delegate engine (primary first, then
     * secondaries) and a fresh {@link FailureHandlerStrategy} that lives for the writer's lifetime.
     *
     * @param engine the composite indexing execution engine
     * @param config the writer configuration
     */
    @SuppressWarnings("unchecked")
    CompositeWriter(CompositeIndexingExecutionEngine engine, WriterConfig config) {
        this.writerGeneration = config.writerGeneration();

        IndexingExecutionEngine<?, ?> primaryDelegate = engine.getPrimaryDelegate();
        this.primaryFormat = primaryDelegate.getDataFormat();
        this.primaryWriter = (Writer<DocumentInput<?>>) primaryDelegate.createWriter(config);
        this.mappingVersion = primaryWriter.mappingVersion();

        // Close already-created writers if a secondary createWriter() fails. LuceneWriter
        // acquires a NativeFSLock on construction, tracked in a static LOCK_HELD set. If not
        // closed, the path remains in LOCK_HELD and on engine restart (same JVM) when the
        // generation counter re-produces the same value, a LockObtainFailedException is thrown.
        Map<DataFormat, Writer<DocumentInput<?>>> secondaries = new IdentityHashMap<>();
        try {
            for (IndexingExecutionEngine<?, ?> delegate : engine.getSecondaryDelegates()) {
                Writer<DocumentInput<?>> secondary = (Writer<DocumentInput<?>>) delegate.createWriter(config);
                assert secondary.isSchemaMutable() && secondary.mappingVersion() >= this.mappingVersion
                    : "Secondary writer mapping version ["
                        + secondary.mappingVersion()
                        + "] must match primary ["
                        + this.mappingVersion
                        + "]";
                secondaries.put(delegate.getDataFormat(), secondary);
            }
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(primaryWriter);
            for (Writer<DocumentInput<?>> w : secondaries.values()) {
                IOUtils.closeWhileHandlingException(w);
            }
            throw e;
        }
        this.secondaryWritersByFormat = Collections.unmodifiableMap(secondaries);
        this.failureHandler = new FailureHandlerStrategy();
        this.statsTracker = engine.statsTracker();
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput doc) throws IOException {
        WriterState currentState = state();
        if (currentState != WriterState.ACTIVE) {
            throw new IllegalStateException("Cannot add document to writer in state " + currentState);
        }

        if (doc.getRowId() != acceptedRows) {
            throw new IllegalStateException("rowId [" + doc.getRowId() + "] does not match accepted row count [" + acceptedRows + "]");
        }

        // Count every write attempt so write_*_failures can be read as a rate.
        statsTracker.incWriteTotal();

        // Roll back exactly the writers we've called addDoc on, in order.
        List<Writer<DocumentInput<?>>> touched = new ArrayList<>();
        touched.add(primaryWriter);
        WriteResult primaryResult = primaryWriter.addDoc(doc.getPrimaryInput());
        if (primaryResult instanceof WriteResult.Failure pf) {
            statsTracker.incWritePrimaryFailures();
            logger.warn(
                () -> new ParameterizedMessage("Failed to add document in primary format [{}], rolling back", primaryFormat.name()),
                pf.cause()
            );
            failureHandler.rollback(touched, acceptedRows);
            return new WriteResult.Failure(pf.cause(), -1, -1, -1);
        }

        Map<DataFormat, DocumentInput<?>> secondaryInputs = doc.getSecondaryInputs();
        for (Map.Entry<DataFormat, DocumentInput<?>> inputEntry : secondaryInputs.entrySet()) {
            DataFormat format = inputEntry.getKey();
            Writer<DocumentInput<?>> writer = secondaryWritersByFormat.get(format);
            touched.add(writer);
            WriteResult result = writer.addDoc(inputEntry.getValue());
            if (result instanceof WriteResult.Failure sf) {
                statsTracker.incWriteSecondaryFailures();
                logger.warn(
                    () -> new ParameterizedMessage("Failed to add document in secondary format [{}], rolling back", format.name()),
                    sf.cause()
                );
                failureHandler.rollback(touched, acceptedRows);
                return new WriteResult.Failure(sf.cause(), -1, -1, -1);
            }
        }

        assert touched.size() == 1 + secondaryInputs.size() : "all writers must succeed if we reach here; touched="
            + touched.size()
            + " expected="
            + (1 + secondaryInputs.size());
        acceptedRows++;
        return primaryResult;
    }

    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        FileInfos.Builder builder = FileInfos.builder();

        // Flush primary first to capture the row ID mapping that secondaries need.
        FileInfos primaryFileInfos = primaryWriter.flush(flushInput);
        Optional<WriterFileSet> primaryWfs = primaryFileInfos.getWriterFileSet(primaryFormat);
        primaryWfs.ifPresent(writerFileSet -> builder.putWriterFileSet(primaryFormat, writerFileSet));

        RowIdMapping rowIdMapping = primaryFileInfos.rowIdMapping();
        if (rowIdMapping != null) {
            builder.rowIdMapping(rowIdMapping);
        }
        FlushInput secondaryFlushInput = rowIdMapping != null ? new FlushInput(rowIdMapping) : FlushInput.EMPTY;

        for (Map.Entry<DataFormat, Writer<DocumentInput<?>>> entry : secondaryWritersByFormat.entrySet()) {
            FileInfos fileInfos = entry.getValue().flush(secondaryFlushInput);
            for (Map.Entry<DataFormat, WriterFileSet> fileEntry : fileInfos.writerFilesMap().entrySet()) {
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
        FileInfos result = builder.build();
        assert result.writerFilesMap().isEmpty() || result.writerFilesMap().size() == 1 + secondaryWritersByFormat.size()
            : "flush must produce files for all formats or none; got "
                + result.writerFilesMap().size()
                + " expected 0 or "
                + (1 + secondaryWritersByFormat.size());
        return result;
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
            statsTracker.incMappingUpdateExecutedTotal();
            this.mappingVersion = newVersion;
            primaryWriter.updateMappingVersion(newVersion);
            for (Writer<?> w : secondaryWritersByFormat.values()) {
                w.updateMappingVersion(newVersion);
            }
        }
    }

    // Use IOUtils.close to ensure all writers are closed even if one throws. Each
    // LuceneWriter.close() triggers indexWriter.rollback() → NativeFSLock.close() →
    // LOCK_HELD.remove(path). Without this, a throwing primary.close() would skip
    // secondary close, leaking the lock and causing LockObtainFailedException on restart.
    @Override
    public void close() throws IOException {
        try {
            List<Closeable> allWriters = new ArrayList<>(1 + secondaryWritersByFormat.size());
            allWriters.add(primaryWriter);
            allWriters.addAll(secondaryWritersByFormat.values());
            IOUtils.close(allWriters);
        } finally {
            closed = true;
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
     * Returns the lifecycle state aggregated by the composite's {@link FailureHandlerStrategy} —
     * the worst (most-progressed) state across primary and all secondary writers.
     */
    @Override
    public WriterState state() {
        return failureHandler.aggregateState(closed, primaryWriter, secondaryWritersByFormat.values());
    }

    /**
     * Per-composite failure-handling helper. Owns rollback orchestration and aggregates
     * sub-writer states into the single {@link WriterState} the engine acts on.
     * {@link #rollback} undoes the in-flight doc on each writer the composite touched;
     * a rollback that itself throws propagates to DFAE, which fails the engine so
     * recovery can replay the translog.
     */
    private static final class FailureHandlerStrategy {

        /**
         * Rolls back each writer in {@code touched} to the composite's current
         * {@code acceptedRows}. Each writer either reaches the target or throws.
         */
        void rollback(List<Writer<DocumentInput<?>>> touched, long targetRowCount) throws IOException {
            for (Writer<DocumentInput<?>> w : touched) {
                w.rollbackTo(targetRowCount);
            }
        }

        WriterState aggregateState(
            boolean closed,
            Writer<DocumentInput<?>> primary,
            java.util.Collection<Writer<DocumentInput<?>>> secondaries
        ) {
            if (closed) return WriterState.CLOSED;
            WriterState worst = WriterState.ACTIVE;
            worst = worse(worst, primary.state());
            for (Writer<?> w : secondaries) {
                worst = worse(worst, w.state());
            }
            return worst;
        }

        private static WriterState worse(WriterState a, WriterState b) {
            return rank(a) >= rank(b) ? a : b;
        }

        private static int rank(WriterState s) {
            return switch (s) {
                case ACTIVE -> 0;
                case RETIRED_FLUSHABLE -> 1;
                case PENDING_ROLLBACK -> 2;
                case CLOSED -> 3;
            };
        }
    }
}
