/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/** One independent sequential Lucene query/doc-values cursor. */
@SuppressForbidden(reason = "reference counting keeps the reader alive for the source lifetime")
final class DocValuesBatchSource implements ArrowBatchSource {

    static final int BATCH_SIZE = 65_536;
    private static final Logger LOGGER = LogManager.getLogger(DocValuesBatchSource.class);

    private final IndexSearcher searcher;
    private final List<InputColumn> columns;
    private final BufferAllocator allocator;
    private final Task task;
    private final Weight weight;
    private final Schema schema;
    private final int[] docs = new int[BATCH_SIZE];
    private final long[] fallbackScratch = new long[BATCH_SIZE];
    private final int[] ordScratch = new int[BATCH_SIZE];
    private final long[] ordRowScratch = new long[BATCH_SIZE];
    private final LongAdder directBatches;
    private final LongAdder fallbackBatches;
    private final LongAdder batches;
    private final LongAdder rows;
    private final LongAdder nullValues;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private int leafIndex = -1;
    private LeafReaderContext leaf;
    private DocIdSetIterator iterator;
    private Bits liveDocs;
    private ColumnReader[] readers;
    private boolean eof;

    DocValuesBatchSource(
        IndexSearcher searcher,
        Weight weight,
        List<InputColumn> columns,
        BufferAllocator allocator,
        Task task,
        LongAdder directBatches,
        LongAdder fallbackBatches,
        LongAdder batches,
        LongAdder rows,
        LongAdder nullValues
    ) throws IOException {
        this.searcher = searcher;
        this.columns = List.copyOf(columns);
        this.allocator = allocator;
        this.task = task;
        this.directBatches = directBatches;
        this.fallbackBatches = fallbackBatches;
        this.batches = batches;
        this.rows = rows;
        this.nullValues = nullValues;
        this.weight = weight;
        this.schema = ArrowBatchSourcePlan.schemaFor(columns);
    }

    @Override
    public BufferAllocator allocator() {
        return allocator;
    }

    @Override
    public synchronized VectorSchemaRoot nextBatch() throws Exception {
        ensureOpen();
        checkCancelled();
        while (eof == false) {
            if (iterator == null && advanceLeaf() == false) {
                eof = true;
                return null;
            }
            int size = 0;
            while (size < BATCH_SIZE) {
                int doc = iterator.nextDoc();
                if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                    iterator = null;
                    break;
                }
                if (liveDocs == null || liveDocs.get(doc)) {
                    docs[size++] = doc;
                }
                if ((size & 0xFFF) == 0) {
                    checkCancelled();
                }
            }
            if (size == 0) {
                continue;
            }
            VectorSchemaRoot root = decodeBatch(size);
            try {
                checkCancelled();
                batches.increment();
                rows.add(size);
                return root;
            } catch (RuntimeException | Error e) {
                root.close();
                throw e;
            }
        }
        return null;
    }

    private boolean advanceLeaf() throws IOException {
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        while (++leafIndex < leaves.size()) {
            leaf = leaves.get(leafIndex);
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            readers = new ColumnReader[columns.size()];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = openColumn(leaf, columns.get(i));
            }
            liveDocs = leaf.reader().getLiveDocs();
            TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
            iterator = twoPhase == null ? scorer.iterator() : TwoPhaseIterator.asDocIdSetIterator(twoPhase);
            return true;
        }
        leaf = null;
        readers = null;
        liveDocs = null;
        iterator = null;
        return false;
    }

    private VectorSchemaRoot decodeBatch(int size) throws IOException {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        boolean success = false;
        try {
            for (int i = 0; i < readers.length; i++) {
                FieldVector vector = root.getVector(i);
                switch (readers[i]) {
                    case LongColumn longColumn -> decodeLong(longColumn, (BaseFixedWidthVector) vector, size);
                    case KeywordColumn keywordColumn -> decodeKeyword(keywordColumn.values(), (ViewVarCharVector) vector, size);
                    case MissingColumn ignored -> decodeMissing(vector, size);
                }
            }
            root.setRowCount(size);
            success = true;
            return root;
        } finally {
            if (success == false) {
                root.close();
            }
        }
    }

    private void decodeMissing(FieldVector vector, int size) {
        switch (vector) {
            case BaseFixedWidthVector fixedWidth -> fixedWidth.allocateNew(size);
            case ViewVarCharVector view -> view.allocateNew((long) size * 16, size);
            default -> throw new IllegalStateException("unsupported missing-column vector type " + vector.getClass().getName());
        }
        vector.getValidityBuffer().setZero(0, (size + 7) / 8);
        vector.setValueCount(size);
        nullValues.add(size);
    }

    private void decodeLong(LongColumn column, BaseFixedWidthVector vector, int size) throws IOException {
        vector.allocateNew(size);
        long byteLength = (long) size * Long.BYTES;
        MemorySegment destination = MemorySegment.ofAddress(vector.getDataBuffer().memoryAddress()).reinterpret(byteLength);
        fallbackBatches.increment();
        column.values().longValues(size, docs, 0, fallbackScratch, 0, 0L);
        destination.copyFrom(MemorySegment.ofArray(fallbackScratch).asSlice(0, byteLength));

        int validityBytes = (size + 7) / 8;
        vector.getValidityBuffer().setZero(0, validityBytes);
        for (int i = 0; i < size; i++) {
            if (column.validity().advanceExact(docs[i])) {
                BitVectorHelper.setBit(vector.getValidityBuffer(), i);
            } else {
                nullValues.increment();
            }
        }
        vector.setValueCount(size);
    }

    private void decodeKeyword(SortedDocValues values, ViewVarCharVector vector, int size) throws IOException {
        for (int i = 0; i < size; i++) {
            ordScratch[i] = values.advanceExact(docs[i]) ? values.ordValue() : -1;
        }
        vector.allocateNew((long) size * 16, size);
        for (int i = 0; i < size; i++) {
            ordRowScratch[i] = ((long) (ordScratch[i] + 1) << 20) | i;
        }
        Arrays.sort(ordRowScratch, 0, size);

        int lastOrd = Integer.MIN_VALUE;
        BytesRef term = null;
        for (int i = 0; i < size; i++) {
            int row = (int) (ordRowScratch[i] & 0xFFFFF);
            int ord = (int) (ordRowScratch[i] >>> 20) - 1;
            if (ord < 0) {
                vector.setNull(row);
                nullValues.increment();
            } else {
                if (ord != lastOrd) {
                    term = values.lookupOrd(ord);
                    lastOrd = ord;
                }
                vector.set(row, term.bytes, term.offset, term.length);
            }
        }
        vector.setValueCount(size);
    }

    private static ColumnReader openColumn(LeafReaderContext leaf, InputColumn column) throws IOException {
        FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(column.name());
        if (fieldInfo == null || fieldInfo.getDocValuesType() == DocValuesType.NONE) {
            return MissingColumn.INSTANCE;
        }
        if (column.kind() == ColumnKind.KEYWORD) {
            return new KeywordColumn(keywordValues(leaf, column.name(), fieldInfo.getDocValuesType()));
        }

        NumericDocValues values = numericValues(leaf, column.name(), fieldInfo.getDocValuesType());
        NumericDocValues validity = numericValues(leaf, column.name(), fieldInfo.getDocValuesType());
        return new LongColumn(values, validity);
    }

    private static NumericDocValues numericValues(LeafReaderContext leaf, String name, DocValuesType type) throws IOException {
        if (type == DocValuesType.NUMERIC) {
            return leaf.reader().getNumericDocValues(name);
        }
        if (type == DocValuesType.SORTED_NUMERIC) {
            SortedNumericDocValues sorted = leaf.reader().getSortedNumericDocValues(name);
            NumericDocValues singleton = DocValues.unwrapSingleton(sorted);
            if (singleton == null) {
                throw new IllegalArgumentException("multi-valued numeric doc values are not supported for field [" + name + "]");
            }
            return singleton;
        }
        throw incompatibleType(name, type);
    }

    private static SortedDocValues keywordValues(LeafReaderContext leaf, String name, DocValuesType type) throws IOException {
        if (type == DocValuesType.SORTED) {
            return leaf.reader().getSortedDocValues(name);
        }
        if (type == DocValuesType.SORTED_SET) {
            SortedSetDocValues sortedSet = leaf.reader().getSortedSetDocValues(name);
            SortedDocValues singleton = DocValues.unwrapSingleton(sortedSet);
            if (singleton == null) {
                throw new IllegalArgumentException("multi-valued keyword doc values are not supported for field [" + name + "]");
            }
            return singleton;
        }
        throw incompatibleType(name, type);
    }

    private static IllegalArgumentException incompatibleType(String name, DocValuesType type) {
        return new IllegalArgumentException("field [" + name + "] has incompatible doc values type [" + type + "]");
    }

    private void checkCancelled() {
        if (cancelled.get() || (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled())) {
            throw new TaskCancelledException("doc-values scan cancelled");
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("doc-values source is closed");
        }
    }

    @Override
    public void cancel() {
        cancelled.set(true);
    }

    @Override
    public void close() {
        cancel();
        if (closed.compareAndSet(false, true)) {
            try {
                searcher.getIndexReader().decRef();
            } catch (IOException e) {
                LOGGER.warn("failed to release doc-values source reader", e);
            }
        }
    }

    private sealed interface ColumnReader permits LongColumn, KeywordColumn, MissingColumn {}

    private enum MissingColumn implements ColumnReader {
        INSTANCE
    }

    private record LongColumn(NumericDocValues values, NumericDocValues validity) implements ColumnReader {
    }

    private record KeywordColumn(SortedDocValues values) implements ColumnReader {
    }
}
