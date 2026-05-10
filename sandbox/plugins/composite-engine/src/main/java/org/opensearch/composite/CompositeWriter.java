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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushAndCloseWriterException;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
     * @param writerGeneration the writer generation number
     */
    @SuppressWarnings("unchecked")
    CompositeWriter(CompositeIndexingExecutionEngine engine, long writerGeneration) {
        this.state = new AtomicReference<>(WriterState.ACTIVE);
        this.writerGeneration = writerGeneration;

        IndexingExecutionEngine<?, ?> primaryDelegate = engine.getPrimaryDelegate();
        this.primaryFormat = primaryDelegate.getDataFormat();
        this.primaryWriter = (Writer<DocumentInput<?>>) primaryDelegate.createWriter(writerGeneration);

        Map<DataFormat, Writer<DocumentInput<?>>> secondaries = new IdentityHashMap<>();
        for (IndexingExecutionEngine<?, ?> delegate : engine.getSecondaryDelegates()) {
            secondaries.put(delegate.getDataFormat(), (Writer<DocumentInput<?>>) delegate.createWriter(writerGeneration));
        }
        this.secondaryWritersByFormat = Collections.unmodifiableMap(secondaries);
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput doc) throws IOException {
        if (state.get() != WriterState.ACTIVE) {
            throw new IllegalStateException("Cannot add document to writer in state " + state.get());
        }

        // Write to primary first
        WriteResult primaryResult = primaryWriter.addDoc(doc.getPrimaryInput());
        switch (primaryResult) {
            case WriteResult.Success s -> logger.trace("Successfully added document in primary format [{}]", primaryFormat.name());
            case WriteResult.Failure f -> {
                logger.debug("Failed to add document in primary format [{}]", primaryFormat.name());
                rowIdGenerator.rollback();
                return primaryResult;
            }
        }

        // Then write to each secondary — keyed lookup by DataFormat (equals/hashCode based on name)
        // Track which secondaries succeeded so we can rollback on failure
        List<Writer<DocumentInput<?>>> succeededSecondaries = new ArrayList<>();
        Map<DataFormat, DocumentInput<?>> secondaryInputs = doc.getSecondaryInputs();
        for (Map.Entry<DataFormat, DocumentInput<?>> inputEntry : secondaryInputs.entrySet()) {
            DataFormat format = inputEntry.getKey();
            Writer<DocumentInput<?>> writer = secondaryWritersByFormat.get(format);
            if (writer == null) {
                logger.warn("No writer found for secondary format [{}], skipping", format.name());
                continue;
            }
            WriteResult result = writer.addDoc(inputEntry.getValue());
            switch (result) {
                case WriteResult.Success s -> {
                    logger.trace("Successfully added document in secondary format [{}]", format.name());
                    succeededSecondaries.add(writer);
                }
                case WriteResult.Failure f -> {
                    logger.warn("Failed to add document in secondary format [{}], rolling back", format.name());
                    return rollbackOnSecondaryFailure(format, f.cause(), succeededSecondaries);
                }
            }
        }

        return primaryResult;
    }

    /**
     * Rolls back the last document from the primary writer and any secondaries that already succeeded,
     * after a secondary format failed. If rollback itself fails, the writer is aborted.
     *
     * @param failedFormat the secondary format that failed
     * @param cause the exception from the failed secondary write
     * @param succeededSecondaries secondaries that accepted the doc before the failure
     * @return a {@link WriteResult.Failure} wrapping a {@link FlushAndCloseWriterException} if rollback succeeds,
     *         or a {@link WriteResult.Failure} with the rollback exception if rollback fails
     */
    private WriteResult rollbackOnSecondaryFailure(
        DataFormat failedFormat,
        Exception cause,
        List<Writer<DocumentInput<?>>> succeededSecondaries
    ) {
        try {
            primaryWriter.rollbackLastDoc();
            for (Writer<DocumentInput<?>> writer : succeededSecondaries) {
                writer.rollbackLastDoc();
            }
            return new WriteResult.Failure(
                new FlushAndCloseWriterException(
                    "Secondary format [" + failedFormat.name() + "] failed, writer should be flushed and closed",
                    cause
                ),
                -1,
                -1,
                -1
            );
        } catch (IOException rollbackEx) {
            logger.error(
                () -> new ParameterizedMessage("Rollback failed after secondary format [{}] failure, aborting writer", failedFormat.name()),
                rollbackEx
            );
            rollbackEx.addSuppressed(cause);
            abort();
            return new WriteResult.Failure(rollbackEx, -1, -1, -1);
        }
    }

    @Override
    public FileInfos flush() throws IOException {
        setFlushPending();
        FileInfos.Builder builder = FileInfos.builder();
        // Flush primary
        Optional<WriterFileSet> primaryWfs = primaryWriter.flush().getWriterFileSet(primaryFormat);
        primaryWfs.ifPresent(writerFileSet -> {
            // Primary format's WriterFileSet must have the same generation as this composite writer
            assert writerFileSet.writerGeneration() == writerGeneration : "primary WriterFileSet generation ["
                + writerFileSet.writerGeneration()
                + "] must match composite writer generation ["
                + writerGeneration
                + "]";
            builder.putWriterFileSet(primaryFormat, writerFileSet);
        });
        // Flush secondaries
        for (Writer<DocumentInput<?>> writer : secondaryWritersByFormat.values()) {
            FileInfos fileInfos = writer.flush();
            // Iterate all format entries in the returned FileInfos
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
    }

    @Override
    public void sync() throws IOException {
        primaryWriter.sync();
        for (Writer<DocumentInput<?>> writer : secondaryWritersByFormat.values()) {
            writer.sync();
        }
    }

    @Override
    public long generation() {
        return getWriterGeneration();
    }

    @Override
    public void close() throws IOException {
        primaryWriter.close();
        for (Writer<DocumentInput<?>> writer : secondaryWritersByFormat.values()) {
            writer.close();
        }
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
    public void abort() {
        if (this.state.compareAndSet(WriterState.ACTIVE, WriterState.ABORTED) == false) {
            throw new IllegalStateException("Cannot abort writer in state " + state.get());
        }
    }

    /**
     * Returns whether this writer has been aborted.
     *
     * @return {@code true} if aborted
     */
    @Override
    public boolean isAborted() {
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
