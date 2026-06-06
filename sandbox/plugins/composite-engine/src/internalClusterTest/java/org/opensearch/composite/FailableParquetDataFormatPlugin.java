/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.composite.framework.ParquetOnlyDataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.writer.ParquetDocumentInput;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-only plugin that exercises {@link org.opensearch.parquet.writer.ParquetWriter}'s
 * production schema-mismatch failure path end-to-end through the cluster client by
 * suppressing schema reconciliation. When armed, the wrapped writer's
 * {@link Writer#updateMappingVersion} is a no-op, so a doc carrying a newly-mapped field
 * arrives at {@code VSRManager.addDocument} without a corresponding {@code ParquetField}
 * mapping. The real {@link org.opensearch.parquet.writer.MismatchedInputException} is
 * thrown by the production code path and the production catch block (state PENDING_ROLLBACK →
 * inline rollback → state ACTIVE → return Failure) handles it.
 *
 * <p>Mirrors {@link FailableLuceneDataFormatPlugin} but exercises the parquet self-rollback
 * contract instead of Lucene's directory-fault path.
 *
 * <p>Usage:
 * <pre>{@code
 *   FailableParquetDataFormatPlugin.armSchemaSuppression();   // next addDoc on a new field fails
 *   ... index doc with a previously-unseen field type ...
 *   FailableParquetDataFormatPlugin.clearFailure();
 * }</pre>
 */
public class FailableParquetDataFormatPlugin extends ParquetOnlyDataFormatPlugin {

    private static final AtomicBoolean suppressMappingUpdates = new AtomicBoolean(false);

    /** While armed, every wrapped writer's {@code updateMappingVersion} is a no-op. */
    public static void armSchemaSuppression() {
        suppressMappingUpdates.set(true);
    }

    /** Re-enables mapping propagation. */
    public static void clearFailure() {
        suppressMappingUpdates.set(false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig engineConfig) {
        IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput> delegate = (IndexingExecutionEngine<
            ParquetDataFormat,
            ParquetDocumentInput>) super.indexingEngine(engineConfig);
        return new MappingSuppressingEngine(delegate);
    }

    /** Delegating wrapper that intercepts only {@code createWriter}. */
    private static final class MappingSuppressingEngine implements IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput> {

        private final IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput> delegate;

        MappingSuppressingEngine(IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Writer<ParquetDocumentInput> createWriter(WriterConfig config) {
            return new MappingSuppressingWriter(delegate.createWriter(config));
        }

        @Override
        public Merger getMerger() {
            return delegate.getMerger();
        }

        @Override
        public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
            return delegate.refresh(refreshInput);
        }

        @Override
        public long getNextWriterGeneration() {
            return delegate.getNextWriterGeneration();
        }

        @Override
        public ParquetDataFormat getDataFormat() {
            return delegate.getDataFormat();
        }

        @Override
        public long getNativeBytesUsed() {
            return delegate.getNativeBytesUsed();
        }

        @Override
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
            return delegate.deleteFiles(filesToDelete);
        }

        @Override
        public ParquetDocumentInput newDocumentInput() {
            return delegate.newDocumentInput();
        }

        @Override
        public IndexStoreProvider getProvider() {
            return delegate.getProvider();
        }

        @Override
        public FormatChecksumStrategy getChecksumStrategy() {
            return delegate.getChecksumStrategy();
        }

        @Override
        public Map<DataFormat, EngineReaderManager<?>> buildReaderManager(ReaderManagerConfig config) throws IOException {
            return delegate.buildReaderManager(config);
        }

        @Override
        public long getHeapBytesUsed() {
            return delegate.getHeapBytesUsed();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * Delegating writer that drops {@code updateMappingVersion} calls while the static flag
     * is armed, so a doc carrying a field added in a later mapping version reaches the
     * production VSR with an unreconciled schema and triggers
     * {@link org.opensearch.parquet.writer.MismatchedInputException} naturally.
     */
    private static final class MappingSuppressingWriter implements Writer<ParquetDocumentInput> {

        private final Writer<ParquetDocumentInput> delegate;

        MappingSuppressingWriter(Writer<ParquetDocumentInput> delegate) {
            this.delegate = delegate;
        }

        @Override
        public WriteResult addDoc(ParquetDocumentInput input) throws IOException {
            return delegate.addDoc(input);
        }

        @Override
        public void rollbackTo(long rowCount) throws IOException {
            delegate.rollbackTo(rowCount);
        }

        @Override
        public FileInfos flush(FlushInput flushInput) throws IOException {
            return delegate.flush(flushInput);
        }

        @Override
        public void sync() throws IOException {
            delegate.sync();
        }

        @Override
        public long generation() {
            return delegate.generation();
        }

        @Override
        public boolean isSchemaMutable() {
            return delegate.isSchemaMutable();
        }

        @Override
        public long mappingVersion() {
            return delegate.mappingVersion();
        }

        @Override
        public void updateMappingVersion(long newVersion) {
            // The whole point: when armed, the writer is left out of sync with the live
            // mapping so any new field type reaches VSRManager.addDocument unmapped, where
            // production code raises MismatchedInputException.
            if (suppressMappingUpdates.get()) {
                return;
            }
            delegate.updateMappingVersion(newVersion);
        }

        @Override
        public WriterState state() {
            return delegate.state();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
