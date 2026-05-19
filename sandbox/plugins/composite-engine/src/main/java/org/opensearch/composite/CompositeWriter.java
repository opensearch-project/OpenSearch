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
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
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
     */
    @SuppressWarnings("unchecked")
    CompositeWriter(CompositeIndexingExecutionEngine engine, WriterConfig config) {
        this.state = new AtomicReference<>(WriterState.ACTIVE);
        this.writerGeneration = config.writerGeneration();

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

        // Write to primary first
        WriteResult primaryResult = primaryWriter.addDoc(doc.getPrimaryInput());
        switch (primaryResult) {
            case WriteResult.Success s -> logger.trace("Successfully added document in primary format [{}]", primaryFormat.name());
            case WriteResult.Failure f -> {
                logger.debug("Failed to add document in primary format [{}]", primaryFormat.name());
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
