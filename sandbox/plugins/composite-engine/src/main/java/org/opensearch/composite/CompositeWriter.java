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
import org.opensearch.common.queue.Lockable;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
class CompositeWriter implements Writer<CompositeDocumentInput>, Lockable {

    private static final Logger logger = LogManager.getLogger(CompositeWriter.class);

    private final Map.Entry<DataFormat, Writer<DocumentInput<?>>> primaryWriter;
    private final Map<DataFormat, Writer<DocumentInput<?>>> secondaryWritersByFormat;
    private final ReentrantLock lock;
    private final long writerGeneration;
    private final RowIdGenerator rowIdGenerator;
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
        this.lock = new ReentrantLock();
        this.state = new AtomicReference<>(WriterState.ACTIVE);
        this.writerGeneration = writerGeneration;

        IndexingExecutionEngine<?, ?> primaryDelegate = engine.getPrimaryDelegate();
        this.primaryWriter = new AbstractMap.SimpleImmutableEntry<>(
            primaryDelegate.getDataFormat(),
            (Writer<DocumentInput<?>>) primaryDelegate.createWriter(writerGeneration)
        );

        Map<DataFormat, Writer<DocumentInput<?>>> secondaries = new LinkedHashMap<>();
        for (IndexingExecutionEngine<?, ?> delegate : engine.getSecondaryDelegates()) {
            secondaries.put(delegate.getDataFormat(), (Writer<DocumentInput<?>>) delegate.createWriter(writerGeneration));
        }
        this.secondaryWritersByFormat = Map.copyOf(secondaries);
        this.rowIdGenerator = new RowIdGenerator(CompositeWriter.class.getName());
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput doc) throws IOException {
        if (state.get() != WriterState.ACTIVE) {
            throw new IllegalStateException("Cannot add document to writer in state " + state.get());
        }
        // Write to primary first
        WriteResult primaryResult = primaryWriter.getValue().addDoc(doc.getPrimaryInput());
        switch (primaryResult) {
            case WriteResult.Success s -> logger.trace("Successfully added document in primary format [{}]", primaryWriter.getKey().name());
            case WriteResult.Failure f -> {
                logger.debug("Failed to add document in primary format [{}]", primaryWriter.getKey().name());
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
            WriteResult result = writer.addDoc(inputEntry.getValue());
            switch (result) {
                case WriteResult.Success s -> logger.trace("Successfully added document in secondary format [{}]", format.name());
                case WriteResult.Failure f -> {
                    logger.debug("Failed to add document in secondary format [{}]", format.name());
                    return result;
                }
            }
        }

        return primaryResult;
    }

    @Override
    public FileInfos flush() throws IOException {
        FileInfos.Builder builder = FileInfos.builder();
        // Flush primary
        Optional<WriterFileSet> primaryWfs = primaryWriter.getValue().flush().getWriterFileSet(primaryWriter.getKey());
        primaryWfs.ifPresent(writerFileSet -> builder.putWriterFileSet(primaryWriter.getKey(), writerFileSet));
        // Flush secondaries
        for (Writer<DocumentInput<?>> writer : secondaryWritersByFormat.values()) {
            FileInfos fileInfos = writer.flush();
            // Iterate all format entries in the returned FileInfos
            for (Map.Entry<DataFormat, WriterFileSet> fileEntry : fileInfos.writerFilesMap().entrySet()) {
                builder.putWriterFileSet(fileEntry.getKey(), fileEntry.getValue());
            }
        }
        return builder.build();
    }

    @Override
    public void sync() throws IOException {
        primaryWriter.getValue().sync();
        for (Writer<DocumentInput<?>> writer : secondaryWritersByFormat.values()) {
            writer.sync();
        }
    }

    @Override
    public void close() throws IOException {
        primaryWriter.getValue().close();
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

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }
}
