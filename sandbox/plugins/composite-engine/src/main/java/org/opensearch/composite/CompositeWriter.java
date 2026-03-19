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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
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
public class CompositeWriter implements Writer<CompositeDocumentInput>, Lock {

    private static final Logger logger = LogManager.getLogger(CompositeWriter.class);

    private final Map.Entry<DataFormat, Writer<DocumentInput<?>>> primaryWriter;
    private final List<Map.Entry<DataFormat, Writer<DocumentInput<?>>>> secondaryWriters;
    private final ReentrantLock lock;
    private final long writerGeneration;
    private final RowIdGenerator rowIdGenerator;
    private volatile boolean aborted;
    private volatile boolean flushPending;

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
    public CompositeWriter(CompositeIndexingExecutionEngine engine, long writerGeneration) {
        this.lock = new ReentrantLock();
        this.aborted = false;
        this.flushPending = false;
        this.writerGeneration = writerGeneration;

        IndexingExecutionEngine<?, ?> primaryDelegate = engine.getPrimaryDelegate();
        this.primaryWriter = new AbstractMap.SimpleImmutableEntry<>(
            primaryDelegate.getDataFormat(),
            (Writer<DocumentInput<?>>) primaryDelegate.createWriter(writerGeneration)
        );

        List<Map.Entry<DataFormat, Writer<DocumentInput<?>>>> secondaries = new ArrayList<>();
        for (IndexingExecutionEngine<?, ?> delegate : engine.getSecondaryDelegates()) {
            secondaries.add(
                new AbstractMap.SimpleImmutableEntry<>(
                    delegate.getDataFormat(),
                    (Writer<DocumentInput<?>>) delegate.createWriter(writerGeneration)
                )
            );
        }
        this.secondaryWriters = List.copyOf(secondaries);
        this.rowIdGenerator = new RowIdGenerator(CompositeWriter.class.getName());
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput doc) throws IOException {
        // Write to primary first
        WriteResult primaryResult = primaryWriter.getValue().addDoc(doc.getPrimaryInput());
        switch (primaryResult) {
            case WriteResult.Success s -> logger.trace("Successfully added document in primary format [{}]", primaryWriter.getKey().name());
            case WriteResult.Failure f -> {
                logger.debug("Failed to add document in primary format [{}]", primaryWriter.getKey().name());
                return primaryResult;
            }
        }

        // Then write to each secondary
        List<DocumentInput<?>> secondaryInputs = new ArrayList<>(doc.getSecondaryInputs().values());
        for (int i = 0; i < secondaryWriters.size(); i++) {
            Map.Entry<DataFormat, Writer<DocumentInput<?>>> entry = secondaryWriters.get(i);
            DocumentInput<?> input = secondaryInputs.get(i);
            WriteResult result = entry.getValue().addDoc(input);
            switch (result) {
                case WriteResult.Success s -> logger.trace("Successfully added document in secondary format [{}]", entry.getKey().name());
                case WriteResult.Failure f -> {
                    logger.debug("Failed to add document in secondary format [{}]", entry.getKey().name());
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
        for (Map.Entry<DataFormat, Writer<DocumentInput<?>>> entry : secondaryWriters) {
            Optional<WriterFileSet> wfs = entry.getValue().flush().getWriterFileSet(entry.getKey());
            wfs.ifPresent(writerFileSet -> builder.putWriterFileSet(entry.getKey(), writerFileSet));
        }
        return builder.build();
    }

    @Override
    public void sync() throws IOException {
        primaryWriter.getValue().sync();
        for (Map.Entry<DataFormat, Writer<DocumentInput<?>>> entry : secondaryWriters) {
            entry.getValue().sync();
        }
    }

    @Override
    public void close() throws IOException {
        primaryWriter.getValue().close();
        for (Map.Entry<DataFormat, Writer<DocumentInput<?>>> entry : secondaryWriters) {
            entry.getValue().close();
        }
    }

    /**
     * Returns the writer generation number.
     *
     * @return the writer generation
     */
    public long getWriterGeneration() {
        return writerGeneration;
    }

    /**
     * Marks this writer as aborted.
     */
    public void abort() {
        this.aborted = true;
    }

    /**
     * Returns whether this writer has been aborted.
     *
     * @return {@code true} if aborted
     */
    public boolean isAborted() {
        return aborted;
    }

    /**
     * Marks this writer as having a pending flush.
     */
    public void setFlushPending() {
        this.flushPending = true;
    }

    /**
     * Returns whether this writer has a pending flush.
     *
     * @return {@code true} if a flush is pending
     */
    public boolean isFlushPending() {
        return flushPending;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lock.tryLock(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
