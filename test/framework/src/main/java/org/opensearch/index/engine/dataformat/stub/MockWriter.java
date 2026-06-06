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
import org.opensearch.index.engine.dataformat.WriterState;
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
    /**
     * If set, addDoc admits the doc (counter++) and then throws this exception. Models a
     * Parquet-style writer where a row is committed before a downstream operation fails.
     */
    private volatile Supplier<RuntimeException> throwAfterAdmit;
    private volatile WriterState state = WriterState.ACTIVE;
    private final AtomicInteger addDocCallCount = new AtomicInteger();

    public void setWriteResultSupplier(Supplier<WriteResult> supplier) {
        this.writeResultSupplier = supplier;
    }

    /**
     * Configure addDoc to admit the doc (counter advances) and then throw. Used to model
     * the Parquet rotation-after-admit failure mode.
     */
    public void setThrowAfterAdmit(Supplier<RuntimeException> throwSupplier) {
        this.throwAfterAdmit = throwSupplier;
    }

    /** Test-only: directly inject a state to simulate post-failure conditions. */
    public void setState(WriterState state) {
        this.state = state;
    }

    @Override
    public WriterState state() {
        return state;
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
        assert state == WriterState.ACTIVE : "addDoc requires ACTIVE state but was " + state;
        addDocCallCount.incrementAndGet();
        if (writeResultSupplier != null) {
            WriteResult result = writeResultSupplier.get();
            if (result instanceof WriteResult.Failure && state == WriterState.ACTIVE) {
                state = WriterState.PENDING_ROLLBACK;
            }
            return result;
        }
        if (throwAfterAdmit != null) {
            // Admit the doc (mirroring Parquet's setRowCount + acceptedRows++), then throw.
            // State remains ACTIVE so the caller can call rollbackLastDoc to undo, just as
            // ParquetWriter.addDoc does on top of VSRManager.
            docs.add(d);
            seqNo.getAndIncrement();
            throw throwAfterAdmit.get();
        }
        docs.add(d);
        long seq = seqNo.getAndIncrement();
        return new WriteResult.Success(1L, 1L, seq);
    }

    /**
     * If true, rollbackTo keeps the writer ACTIVE (Parquet-style). Default false
     * means rollback always retires (Lucene-style).
     */
    private volatile boolean rollbackKeepsActive = false;

    /** Test-only: switch rollback behavior to "stay ACTIVE" (Parquet-style). */
    public void setRollbackKeepsActive(boolean keepActive) {
        this.rollbackKeepsActive = keepActive;
    }

    @Override
    public void rollbackTo(long rowCount) {
        if (rowCount > docs.size()) {
            throw new IllegalStateException("Cannot rollback to " + rowCount + ": only " + docs.size() + " docs");
        }
        while (docs.size() > rowCount) {
            docs.remove(docs.size() - 1);
        }
        state = rollbackKeepsActive ? WriterState.ACTIVE : WriterState.RETIRED_FLUSHABLE;
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

    @Override
    public void close() {
        state = WriterState.CLOSED;
    }
}
