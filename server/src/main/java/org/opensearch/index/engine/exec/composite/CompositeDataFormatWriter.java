/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.lucene.util.SetOnce;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileInfos;
//import org.opensearch.index.engine.exec.RowIdGenerator;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class CompositeDataFormatWriter implements Writer<CompositeDataFormatWriter.CompositeDocumentInput>, Lock {

    private final List<ImmutablePair<DataFormat, Writer<? extends DocumentInput<?>>>> writers;
    private final Runnable postWrite;
    private final ReentrantLock lock;
    private final SetOnce<Boolean> flushPending = new SetOnce<>();
    private final SetOnce<Boolean> hasFlushed = new SetOnce<>();
    private final long writerGeneration;
    private boolean aborted;
    //private final RowIdGenerator rowIdGenerator;
    public static final String ROW_ID = "_row_id";

    public CompositeDataFormatWriter(CompositeIndexingExecutionEngine engine,
        long writerGeneration) {
        this.writers = new ArrayList<>();
        this.lock = new ReentrantLock();
        this.aborted = false;
        this.writerGeneration = writerGeneration;
        engine.getDelegates().forEach(delegate -> {
            try {
                writers.add(ImmutablePair.of(delegate.getDataFormat(), delegate.createWriter(writerGeneration)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        this.postWrite = () -> {
            engine.getDataFormatWriterPool().releaseAndUnlock(this);
        };
        //this.rowIdGenerator = new RowIdGenerator(CompositeDataFormatWriter.class.getName());
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput d) throws IOException {
        return d.addToWriter();
    }

    @Override
    public FileInfos flush(FlushIn flushIn) throws IOException {
        FileInfos fileInfos = new FileInfos();
        for (ImmutablePair<DataFormat, Writer<? extends DocumentInput<?>>> writerPair : writers) {
            Optional<WriterFileSet> fileMetadataOptional = writerPair.getRight().flush(flushIn)
                .getWriterFileSet(writerPair.getLeft());
            fileMetadataOptional.ifPresent(
                fileMetadata -> fileInfos.putWriterFileSet(writerPair.getLeft(), fileMetadata));
        }
        hasFlushed.set(true);
        return fileInfos;
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() {

    }

    @Override
    public CompositeDocumentInput newDocumentInput() {

        CompositeDocumentInput compositeDocumentInput = new CompositeDocumentInput(
            writers.stream().map(ImmutablePair::getRight).map(Writer::newDocumentInput).collect(Collectors.toList()),
            this, postWrite);

        //compositeDocumentInput.addRowIdField(ROW_ID, rowIdGenerator.getAndIncrementRowId());

        return compositeDocumentInput;
    }

    void abort() throws IOException {
        aborted = true;
    }

    public void setFlushPending() {
        flushPending.set(Boolean.TRUE);
    }

    public boolean hasFlushed() {
        return hasFlushed.get() == Boolean.TRUE;
    }

    public boolean isFlushPending() {
        return flushPending.get() == Boolean.TRUE;
    }

    public boolean isAborted() {
        return aborted;
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

    boolean isHeldByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    public static class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {

        List<? extends DocumentInput<?>> inputs;
        CompositeDataFormatWriter writer;
        Runnable onClose;

        public CompositeDocumentInput(List<? extends DocumentInput<?>> inputs, CompositeDataFormatWriter writer,
            Runnable onClose) {
            this.inputs = inputs;
            this.writer = writer;
            this.onClose = onClose;
        }

        @Override
        public void addRowIdField(String fieldName, long rowId) {
            for (DocumentInput<?> input : inputs) {
                input.addRowIdField(fieldName, rowId);
            }
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            for (DocumentInput<?> input : inputs) {
                input.addField(fieldType, value);
            }
        }

        @Override
        public List<? extends DocumentInput<?>> getFinalInput() {
            return null;
        }

        @Override
        public WriteResult addToWriter() throws IOException {
            WriteResult writeResult = null;
            for (DocumentInput<?> input : inputs) {
                writeResult = input.addToWriter();
            }
            return writeResult;
        }

        @Override
        public void close() throws Exception {
            onClose.run();
        }
    }
}
