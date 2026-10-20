/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A writer that persists doc IDs as lines in a text file for file-level consistency
 * verification in tests. Supports failure injection, rollback, and produces real
 * {@link WriterFileSet} on flush. Tracks close state for writer pool lifecycle assertions.
 */
public class FileBackedFailableWriter implements Writer<DocumentInput<?>> {

    private final DataFormat format;
    private final long writerGeneration;
    private final Path directory;
    private final String fileName;
    private final List<String> docs = new ArrayList<>();
    final AtomicInteger addDocCallCount = new AtomicInteger();
    public volatile Supplier<WriteResult> writeResultSupplier;
    volatile IOException flushFailure;
    public volatile IOException rollbackFailure;
    volatile boolean rollbackCalled;
    private volatile WriterState state = WriterState.ACTIVE;
    /**
     * State to transition into when {@link #rollbackTo(long)} succeeds. Defaults to
     * {@link WriterState#RETIRED_FLUSHABLE} — Lucene-style. Parquet-style writers that can
     * cleanly undo the last row return to {@link WriterState#ACTIVE}; tests can flip via
     * {@link #setStateAfterRollback}.
     */
    private volatile WriterState stateAfterRollback = WriterState.RETIRED_FLUSHABLE;

    public FileBackedFailableWriter(DataFormat format, long writerGeneration, Path directory) {
        this.format = format;
        this.writerGeneration = writerGeneration;
        this.directory = directory;
        this.fileName = "data_gen" + writerGeneration + "_" + format.name() + ".txt";
    }

    @Override
    public WriteResult addDoc(DocumentInput<?> d) {
        assert state == WriterState.ACTIVE : "addDoc requires ACTIVE state but was " + state;
        int callNum = addDocCallCount.incrementAndGet();
        if (writeResultSupplier != null) {
            WriteResult result = writeResultSupplier.get();
            // Null result means "fall through to default success behavior" — useful for
            // selective per-call failure injection where most calls should succeed normally.
            if (result != null) {
                if (result instanceof WriteResult.Failure && state == WriterState.ACTIVE) {
                    state = WriterState.PENDING_ROLLBACK;
                }
                return result;
            }
        }
        docs.add("doc_" + callNum);
        return new WriteResult.Success(1L, 1L, callNum);
    }

    @Override
    public FileInfos flush(org.opensearch.index.engine.dataformat.FlushInput flushInput) throws IOException {
        if (flushFailure != null) throw flushFailure;
        Path file = directory.resolve(fileName);
        Files.write(file, docs, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(directory)
            .writerGeneration(writerGeneration)
            .addFile(fileName)
            .addNumRows(docs.size())
            .build();
        return FileInfos.builder().putWriterFileSet(format, fileSet).build();
    }

    @Override
    public void rollbackTo(long rowCount) throws IOException {
        rollbackCalled = true;
        if (rollbackFailure != null) {
            throw rollbackFailure;
        }
        if (rowCount > docs.size()) {
            throw new IllegalStateException("Cannot rollback to " + rowCount + ": only " + docs.size() + " docs");
        }
        while (docs.size() > rowCount) {
            docs.remove(docs.size() - 1);
        }
        state = stateAfterRollback;
    }

    @Override
    public WriterState state() {
        return state;
    }

    /**
     * Test-only: override the post-rollback state (default {@link WriterState#RETIRED_FLUSHABLE}).
     * Pass {@link WriterState#ACTIVE} for Parquet-style "stay ACTIVE after a clean rollback".
     */
    public void setStateAfterRollback(WriterState state) {
        this.stateAfterRollback = state;
    }

    /** Returns the doc IDs currently held in memory (before flush). */
    List<String> getDocs() {
        return List.copyOf(docs);
    }

    int numRows() {
        return docs.size();
    }

    String getFileName() {
        return fileName;
    }

    public Path getFilePath() {
        return directory.resolve(fileName);
    }

    @Override
    public long generation() {
        return writerGeneration;
    }

    @Override
    public boolean isSchemaMutable() {
        return true;
    }

    @Override
    public long mappingVersion() {
        return 0;
    }

    @Override
    public void updateMappingVersion(long newVersion) {}

    private volatile boolean closed;

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        state = WriterState.CLOSED;
    }
}
