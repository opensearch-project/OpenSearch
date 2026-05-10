/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A mock {@link Writer} for testing purposes.
 */
public class MockWriter implements Writer<MockDocumentInput> {
    private final long writerGeneration;
    private final DataFormat dataFormat;
    private final Path directory;
    private final List<MockDocumentInput> docs = new ArrayList<>();
    private final AtomicLong seqNo;
    private Supplier<WriteResult> writeResultSupplier;
    private boolean aborted;
    private final AtomicInteger addDocCallCount = new AtomicInteger();

    public void setWriteResultSupplier(Supplier<WriteResult> supplier) {
        this.writeResultSupplier = supplier;
    }

    public void setAborted(boolean aborted) {
        this.aborted = aborted;
    }

    @Override
    public boolean isAborted() {
        return aborted;
    }

    public int getAddDocCallCount() {
        return addDocCallCount.get();
    }

    public MockWriter(long writerGeneration, DataFormat dataFormat, Path directory, AtomicLong seqNo) {
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.directory = directory;
        this.seqNo = seqNo;
    }

    @Override
    public WriteResult addDoc(MockDocumentInput d) {
        addDocCallCount.incrementAndGet();
        if (writeResultSupplier != null) {
            return writeResultSupplier.get();
        }
        docs.add(d);
        long seq = seqNo.getAndIncrement();
        return new WriteResult.Success(1L, 1L, seq);
    }

    @Override
    public FileInfos flush() {
        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(directory)
            .writerGeneration(writerGeneration)
            .addFile("data_gen" + writerGeneration + ".parquet")
            .addNumRows(docs.size())
            .build();
        return FileInfos.builder().putWriterFileSet(dataFormat, fileSet).build();
    }

    @Override
    public void sync() {}

    @Override
    public long generation() {
        return writerGeneration;
    }

    @Override
    public void close() {}
}
