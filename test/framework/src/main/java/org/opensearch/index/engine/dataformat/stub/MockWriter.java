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
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A mock {@link Writer} for testing purposes.
 */
public class MockWriter implements Writer<MockDocumentInput> {
    private final long writerGeneration;
    private final DataFormat dataFormat;
    private final Path directory;
    private final List<MockDocumentInput> docs = new ArrayList<>();
    private final AtomicLong seqNo;

    public MockWriter(long writerGeneration, DataFormat dataFormat, Path directory, AtomicLong seqNo) {
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.directory = directory;
        this.seqNo = seqNo;
    }

    @Override
    public WriteResult addDoc(MockDocumentInput d) {
        docs.add(d);
        long seq = seqNo.getAndIncrement();
        return new WriteResult.Success(1L, 1L, seq);
    }

    @Override
    public FileInfos flush(FlushInput flushInput) {
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
    public void lock() {}

    @Override
    public boolean tryLock() {
        return true;
    }

    @Override
    public void unlock() {}

    @Override
    public void close() {}
}
