/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FieldAssignments;
import org.opensearch.index.engine.exec.AssignedFieldType;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.RowIdGenerator;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

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

public class CompositeDataFormatWriter implements Writer<CompositeDataFormatWriter.CompositeDocumentInput>, Lock {

    private static final Logger logger = LogManager.getLogger(CompositeDataFormatWriter.class);
    private final List<Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>>> writers;
    private final Runnable postWrite;
    private final ReentrantLock lock;
    private final SetOnce<Boolean> flushPending = new SetOnce<>();
    private final SetOnce<Boolean> hasFlushed = new SetOnce<>();
    private final long writerGeneration;
    private boolean aborted;
    private final RowIdGenerator rowIdGenerator;
    private final Map<DataFormat, FieldAssignments> fieldAssignmentsMap;
    public static final String ROW_ID = "___row_id";

    public CompositeDataFormatWriter(CompositeIndexingExecutionEngine engine, long writerGeneration) {
        this.writers = new ArrayList<>();
        this.lock = new ReentrantLock();
        this.aborted = false;
        this.writerGeneration = writerGeneration;
        this.fieldAssignmentsMap = engine.getFieldAssignmentsMap();
        engine.getDelegates().forEach(delegate -> {
            try {
                writers.add(new AbstractMap.SimpleImmutableEntry<>(delegate.getDataFormat(), delegate.createWriter(writerGeneration)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        this.postWrite = () -> {
            engine.getDataFormatWriterPool().releaseAndUnlock(this);
        };
        this.rowIdGenerator = new RowIdGenerator(CompositeDataFormatWriter.class.getName());
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput d) throws IOException {
        return d.addToWriter();
    }

    @Override
    public FileInfos flush(FlushIn flushIn) throws IOException {
        FileInfos.Builder builder = FileInfos.builder();
        for (Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>> writerPair : writers) {
            Optional<WriterFileSet> writerFileSetOptional = writerPair.getValue().flush(flushIn).getWriterFileSet(writerPair.getKey());
            writerFileSetOptional.ifPresent(fileMetadata -> builder.putWriterFileSet(writerPair.getKey(), fileMetadata));
        }
        hasFlushed.set(true);
        return builder.build();
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>> writerPair : writers) {
            writerPair.getValue().close();
        }
    }

    @Override
    public CompositeDocumentInput newDocumentInput() {
        List<DocumentInput<?>> inputs = new ArrayList<>();
        for (Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>> writerEntry : writers) {
            inputs.add(writerEntry.getValue().newDocumentInput());
        }

        CompositeDocumentInput compositeDocumentInput =
            new CompositeDocumentInput(
                inputs,
                fieldAssignmentsMap,
                this,
                postWrite
            );

        compositeDocumentInput.addRowIdField(ROW_ID, rowIdGenerator.getAndIncrementRowId());

        return compositeDocumentInput;
    }

    void abort() throws IOException {
        aborted = true;
    }

    public void setFlushPending() {
        flushPending.set(Boolean.TRUE);
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

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    public long getWriterGeneration() {
        return writerGeneration;
    }

    public static class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {

        List<? extends DocumentInput<?>> inputs;
        private final Map<DataFormat, FieldAssignments> fieldAssignmentsMap;
        CompositeDataFormatWriter writer;
        Runnable onClose;
        private long version = -1;
        private long seqNo = -2L;
        private long primaryTerm = 0;

        public CompositeDocumentInput(
            List<? extends DocumentInput<?>> inputs,
            Map<DataFormat, FieldAssignments> fieldAssignmentsMap,
            CompositeDataFormatWriter writer,
            Runnable onClose
        ) {
            this.inputs = inputs;
            this.fieldAssignmentsMap = fieldAssignmentsMap;
            this.writer = writer;
            this.onClose = onClose;
        }

        @Override
        public void addRowIdField(String fieldName, long rowId) {
            for (DocumentInput<?> input : inputs) {
                input.addRowIdField(fieldName, rowId);
            }
        }

        /**
         * Entry point from the mapper layer. Resolves per-format {@link MappedFieldType}
         * using each delegate's {@link FieldAssignments}, then delegates to the format-specific
         * {@link DocumentInput#addField(MappedFieldType, Object)}.
         * Skips delegation if no field type exists for the field name in that format.
         */
        public void addField(MappedFieldType fieldType, Object value) {
            logger.debug("[COMPOSITE_DEBUG] addField: field=[{}] type=[{}] value=[{}] — resolving per-format field types for {} inputs",
                fieldType.name(), fieldType.typeName(), value, inputs.size());
            for (DocumentInput<?> input : inputs) {
                FieldAssignments assignments = fieldAssignmentsMap.get(input.getDataFormat());
                if (assignments == null) {
                    continue;
                }
                MappedFieldType perFormatType = assignments.getFieldType(fieldType.name());
                if (perFormatType == null) {
                    continue;
                }
                input.addField(perFormatType, value);
            }
        }

        @Override
        public void setVersion(long version) {
            this.version = version;
            MappedFieldType versionType = new AssignedFieldType(
                VersionFieldMapper.NAME,
                VersionFieldMapper.CONTENT_TYPE,
                false,
                false,
                true
            );
            for (DocumentInput<?> input : inputs) {
                input.addField(versionType, version);
            }
        }

        @Override
        public void setSeqNo(long seqNo) {
            this.seqNo = seqNo;
            MappedFieldType seqNoType = new AssignedFieldType(
                SeqNoFieldMapper.NAME,
                SeqNoFieldMapper.CONTENT_TYPE,
                true,
                false,
                true
            );
            for (DocumentInput<?> input : inputs) {
                input.addField(seqNoType, seqNo);
            }
        }

        @Override
        public void setPrimaryTerm(String fieldName, long primaryTerm) {
            this.primaryTerm = primaryTerm;
            for (DocumentInput<?> input : inputs) {
                input.setPrimaryTerm(fieldName, primaryTerm);
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
