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
 * A writer that persists doc IDs to a text file (one per line) for
 * file-level consistency verification in tests. Supports failure
 * injection, rollback, and produces real {@link WriterFileSet} on flush.
 */
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

    public FileBackedFailableWriter(DataFormat format, long writerGeneration, Path directory) {
        this.format = format;
        this.writerGeneration = writerGeneration;
        this.directory = directory;
        this.fileName = "data_gen" + writerGeneration + "_" + format.name() + ".txt";
    }

    @Override
    public WriteResult addDoc(DocumentInput<?> d) {
        int callNum = addDocCallCount.incrementAndGet();
        if (writeResultSupplier != null) {
            return writeResultSupplier.get();
        }
        docs.add("doc_" + callNum);
        return new WriteResult.Success(1L, 1L, callNum);
    }

    @Override
    public FileInfos flush() throws IOException {
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
    public void rollbackLastDoc() throws IOException {
        rollbackCalled = true;
        if (rollbackFailure != null) throw rollbackFailure;
        if (docs.isEmpty() == false) {
            docs.remove(docs.size() - 1);
        }
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
    public void sync() {}

    @Override
    public long generation() {
        return writerGeneration;
    }

    @Override
    public void lock() {}

    @Override
    public boolean tryLock() {
        return true;
    }

    @Override
    public void unlock() {}

    private volatile boolean closed;

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
