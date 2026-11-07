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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.BinaryDocValues;
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
import org.apache.lucene.util.NumericUtils;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final Text textScratch = new Text();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private int leafIndex = -1;
    private LeafReaderContext leaf;
    private DocIdSetIterator iterator;
    private Bits liveDocs;
    private ColumnReader[] readers;
    private boolean eof;

    DocValuesBatchSource(IndexSearcher searcher, Weight weight, List<InputColumn> columns, BufferAllocator allocator, Task task)
        throws IOException {
        this.searcher = searcher;
        this.columns = List.copyOf(columns);
        this.allocator = allocator;
        this.task = task;
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
                    case NumericColumn numeric -> decodeNumeric(numeric, vector, size);
                    case SortedNumericColumn numeric -> decodeSortedNumeric(numeric, (ListVector) vector, size);
                    case BinaryColumn binary -> decodeBinary(binary.values(), (VarBinaryVector) vector, size);
                    case SortedColumn sorted -> decodeSorted(sorted, vector, size);
                    case SortedSetColumn sortedSet -> decodeSortedSet(sortedSet, (ListVector) vector, size);
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
            case VarBinaryVector binary -> binary.allocateNew((long) size * 16, size);
            case ListVector list -> {
                UnionListWriter writer = list.getWriter();
                for (int i = 0; i < size; i++) {
                    writer.setPosition(i);
                    writer.writeNull();
                }
                writer.setValueCount(size);
                return;
            }
            default -> throw new IllegalStateException("unsupported missing-column vector type " + vector.getClass().getName());
        }
        vector.getValidityBuffer().setZero(0, (size + 7) / 8);
        vector.setValueCount(size);
    }

    private void decodeNumeric(NumericColumn column, FieldVector vector, int size) throws IOException {
        column.values().longValues(size, docs, 0, fallbackScratch, 0, 0L);
        switch (column.kind()) {
            case LONG, TIMESTAMP -> decodeLong(column.validity(), (BaseFixedWidthVector) vector, size);
            case BOOLEAN -> {
                BitVector booleans = (BitVector) vector;
                booleans.allocateNew(size);
                for (int i = 0; i < size; i++) {
                    if (column.validity().advanceExact(docs[i])) {
                        booleans.set(i, fallbackScratch[i] == 0L ? 0 : 1);
                    } else {
                        booleans.setNull(i);
                    }
                }
                booleans.setValueCount(size);
            }
            case FLOAT -> {
                Float4Vector floats = (Float4Vector) vector;
                floats.allocateNew(size);
                for (int i = 0; i < size; i++) {
                    if (column.validity().advanceExact(docs[i])) {
                        floats.set(i, NumericUtils.sortableIntToFloat((int) fallbackScratch[i]));
                    } else {
                        floats.setNull(i);
                    }
                }
                floats.setValueCount(size);
            }
            case DOUBLE -> {
                Float8Vector doubles = (Float8Vector) vector;
                doubles.allocateNew(size);
                for (int i = 0; i < size; i++) {
                    if (column.validity().advanceExact(docs[i])) {
                        doubles.set(i, NumericUtils.sortableLongToDouble(fallbackScratch[i]));
                    } else {
                        doubles.setNull(i);
                    }
                }
                doubles.setValueCount(size);
            }
            case KEYWORD, BINARY, IP -> throw new IllegalStateException("non-numeric column kind " + column.kind());
        }
    }

    private void decodeLong(NumericDocValues validity, BaseFixedWidthVector vector, int size) throws IOException {
        vector.allocateNew(size);
        vector.getValidityBuffer().setZero(0, (size + 7) / 8);
        for (int i = 0; i < size; i++) {
            vector.getDataBuffer().setLong((long) i * Long.BYTES, fallbackScratch[i]);
            if (validity.advanceExact(docs[i])) {
                BitVectorHelper.setBit(vector.getValidityBuffer(), i);
            }
        }
        vector.setValueCount(size);
    }

    private void decodeSorted(SortedColumn column, FieldVector vector, int size) throws IOException {
        SortedDocValues values = column.values();
        for (int i = 0; i < size; i++) {
            ordScratch[i] = values.advanceExact(docs[i]) ? values.ordValue() : -1;
        }
        allocateBytes(vector, size);
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
            } else {
                if (ord != lastOrd) {
                    term = values.lookupOrd(ord);
                    lastOrd = ord;
                }
                setBytes(vector, row, term);
            }
        }
        vector.setValueCount(size);
    }

    private void decodeBinary(BinaryDocValues values, VarBinaryVector vector, int size) throws IOException {
        vector.allocateNew((long) size * 16, size);
        for (int i = 0; i < size; i++) {
            if (values.advanceExact(docs[i])) {
                BytesRef value = values.binaryValue();
                vector.setSafe(i, value.bytes, value.offset, value.length);
            } else {
                vector.setNull(i);
            }
        }
        vector.setValueCount(size);
    }

    private void decodeSortedNumeric(SortedNumericColumn column, ListVector vector, int size) throws IOException {
        SortedNumericDocValues values = column.values();
        UnionListWriter writer = vector.getWriter();
        int valueIndex = 0;
        for (int i = 0; i < size; i++) {
            writer.setPosition(i);
            if (values.advanceExact(docs[i]) == false) {
                writer.writeNull();
                continue;
            }
            writer.startList();
            int count = values.docValueCount();
            for (int j = 0; j < count; j++) {
                writeNumeric(writer, column.kind(), values.nextValue());
                if ((++valueIndex & 0xFFF) == 0) {
                    checkCancelled();
                }
            }
            writer.endList();
        }
        writer.setValueCount(size);
    }

    private void decodeSortedSet(SortedSetColumn column, ListVector vector, int size) throws IOException {
        SortedSetDocValues values = column.values();
        UnionListWriter writer = vector.getWriter();
        int valueIndex = 0;
        for (int i = 0; i < size; i++) {
            writer.setPosition(i);
            if (values.advanceExact(docs[i]) == false) {
                writer.writeNull();
                continue;
            }
            writer.startList();
            int count = values.docValueCount();
            for (int j = 0; j < count; j++) {
                writeBytes(writer, column.kind(), values.lookupOrd(values.nextOrd()));
                if ((++valueIndex & 0xFFF) == 0) {
                    checkCancelled();
                }
            }
            writer.endList();
        }
        writer.setValueCount(size);
    }

    private static void writeNumeric(UnionListWriter writer, ColumnKind kind, long value) {
        switch (kind) {
            case LONG -> writer.bigInt().writeBigInt(value);
            case TIMESTAMP -> writer.timeStampMilli().writeTimeStampMilli(value);
            case BOOLEAN -> writer.bit().writeBit(value == 0L ? 0 : 1);
            case FLOAT -> writer.float4().writeFloat4(NumericUtils.sortableIntToFloat((int) value));
            case DOUBLE -> writer.float8().writeFloat8(NumericUtils.sortableLongToDouble(value));
            case KEYWORD, BINARY, IP -> throw new IllegalStateException("non-numeric column kind " + kind);
        }
    }

    private void writeBytes(UnionListWriter writer, ColumnKind kind, BytesRef value) {
        switch (kind) {
            case KEYWORD -> {
                textScratch.set(value.bytes, value.offset, value.length);
                writer.writeViewVarChar(textScratch);
            }
            case BINARY, IP -> writer.writeVarBinary(value.bytes, value.offset, value.length);
            case LONG, TIMESTAMP, BOOLEAN, FLOAT, DOUBLE -> throw new IllegalStateException("non-binary column kind " + kind);
        }
    }

    private static void allocateBytes(FieldVector vector, int size) {
        switch (vector) {
            case ViewVarCharVector text -> text.allocateNew((long) size * 16, size);
            case VarBinaryVector binary -> binary.allocateNew((long) size * 16, size);
            default -> throw new IllegalStateException("unsupported byte vector type " + vector.getClass().getName());
        }
    }

    private static void setBytes(FieldVector vector, int row, BytesRef value) {
        switch (vector) {
            case ViewVarCharVector text -> text.setSafe(row, value.bytes, value.offset, value.length);
            case VarBinaryVector binary -> binary.setSafe(row, value.bytes, value.offset, value.length);
            default -> throw new IllegalStateException("unsupported byte vector type " + vector.getClass().getName());
        }
    }

    private static ColumnReader openColumn(LeafReaderContext leaf, InputColumn column) throws IOException {
        FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(column.name());
        if (fieldInfo == null || fieldInfo.getDocValuesType() == DocValuesType.NONE) {
            return MissingColumn.INSTANCE;
        }
        DocValuesType type = fieldInfo.getDocValuesType();
        if (column.multiValued()) {
            return switch (column.kind()) {
                case KEYWORD, IP -> new SortedSetColumn(sortedSetValues(leaf, column.name(), type), column.kind());
                case BINARY -> throw incompatibleType(column.name(), type);
                case LONG, TIMESTAMP, BOOLEAN, FLOAT, DOUBLE -> new SortedNumericColumn(
                    sortedNumericValues(leaf, column.name(), type),
                    column.kind()
                );
            };
        }
        return switch (column.kind()) {
            case KEYWORD, IP -> new SortedColumn(sortedValues(leaf, column, type), column.kind());
            case BINARY -> type == DocValuesType.BINARY
                ? new BinaryColumn(leaf.reader().getBinaryDocValues(column.name()))
                : new SortedColumn(sortedValues(leaf, column, type), column.kind());
            case LONG, TIMESTAMP, BOOLEAN, FLOAT, DOUBLE -> new NumericColumn(
                numericValues(leaf, column.name(), type),
                numericValues(leaf, column.name(), type),
                column.kind()
            );
        };
    }

    private static NumericDocValues numericValues(LeafReaderContext leaf, String name, DocValuesType type) throws IOException {
        if (type == DocValuesType.NUMERIC) {
            return leaf.reader().getNumericDocValues(name);
        }
        if (type == DocValuesType.SORTED_NUMERIC) {
            NumericDocValues singleton = DocValues.unwrapSingleton(leaf.reader().getSortedNumericDocValues(name));
            if (singleton == null) {
                throw new IllegalArgumentException("multi-valued numeric doc values are not supported for scalar field [" + name + "]");
            }
            return singleton;
        }
        throw incompatibleType(name, type);
    }

    private static SortedNumericDocValues sortedNumericValues(LeafReaderContext leaf, String name, DocValuesType type) throws IOException {
        if (type == DocValuesType.NUMERIC) {
            return DocValues.singleton(leaf.reader().getNumericDocValues(name));
        }
        if (type == DocValuesType.SORTED_NUMERIC) {
            return leaf.reader().getSortedNumericDocValues(name);
        }
        throw incompatibleType(name, type);
    }

    private static SortedDocValues sortedValues(LeafReaderContext leaf, InputColumn column, DocValuesType type) throws IOException {
        if (type == DocValuesType.SORTED) {
            return leaf.reader().getSortedDocValues(column.name());
        }
        if (type == DocValuesType.SORTED_SET) {
            SortedDocValues singleton = DocValues.unwrapSingleton(leaf.reader().getSortedSetDocValues(column.name()));
            if (singleton == null) {
                String kind = column.kind().name().toLowerCase(Locale.ROOT);
                throw new IllegalArgumentException(
                    "multi-valued " + kind + " doc values are not supported for scalar field [" + column.name() + "]"
                );
            }
            return singleton;
        }
        throw incompatibleType(column.name(), type);
    }

    private static SortedSetDocValues sortedSetValues(LeafReaderContext leaf, String name, DocValuesType type) throws IOException {
        if (type == DocValuesType.SORTED) {
            return DocValues.singleton(leaf.reader().getSortedDocValues(name));
        }
        if (type == DocValuesType.SORTED_SET) {
            return leaf.reader().getSortedSetDocValues(name);
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

    private sealed interface ColumnReader permits NumericColumn, SortedNumericColumn, BinaryColumn, SortedColumn, SortedSetColumn,
        MissingColumn {}

    private enum MissingColumn implements ColumnReader {
        INSTANCE
    }

    private record NumericColumn(NumericDocValues values, NumericDocValues validity, ColumnKind kind) implements ColumnReader {
    }

    private record SortedNumericColumn(SortedNumericDocValues values, ColumnKind kind) implements ColumnReader {
    }

    private record BinaryColumn(BinaryDocValues values) implements ColumnReader {
    }

    private record SortedColumn(SortedDocValues values, ColumnKind kind) implements ColumnReader {
    }

    private record SortedSetColumn(SortedSetDocValues values, ColumnKind kind) implements ColumnReader {
    }
}
