/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.io.IOException;

/**
 * {@link DocValuesProducer} that intercepts the {@code ___row_id} field and returns
 * remapped row ID values from a {@link RowIdMapping}. All other fields are delegated
 * unchanged to the wrapped producer.
 *
 * <p>This ensures the merged segment's {@code ___row_id} doc values contain the new
 * global row IDs (0..n-1) rather than the original per-segment local values.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class RowIdRemappingDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer delegate;
    private final RowIdMapping rowIdMapping;
    private final long generation;
    private final int maxDoc;
    private final int rowIdOffset;

    /**
     * @param delegate     the original doc values producer
     * @param rowIdMapping the mapping from old to new row IDs, or null for sequential assignment
     * @param generation   the writer generation of the source segment
     * @param maxDoc       the maximum document count in the source segment
     * @param rowIdOffset  the starting row ID offset for sequential assignment (used when rowIdMapping is null)
     */
    RowIdRemappingDocValuesProducer(DocValuesProducer delegate, RowIdMapping rowIdMapping, long generation, int maxDoc, int rowIdOffset) {
        this.delegate = delegate;
        this.rowIdMapping = rowIdMapping;
        this.generation = generation;
        this.maxDoc = maxDoc;
        this.rowIdOffset = rowIdOffset;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        if (LuceneMerger.ROW_ID_FIELD.equals(field.name)) {
            if (rowIdMapping != null) {
                return new MappedRowIdDocValues(delegate.getSortedNumeric(field), rowIdMapping, generation);
            }
            return new SequentialRowIdDocValues(maxDoc, rowIdOffset);
        }
        return delegate.getSortedNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return delegate.getSkipper(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Reads the original {@code ___row_id} and maps it through the {@link RowIdMapping}.
     */
    private static class MappedRowIdDocValues extends SortedNumericDocValues {

        private final SortedNumericDocValues delegate;
        private final RowIdMapping rowIdMapping;
        private final long generation;

        MappedRowIdDocValues(SortedNumericDocValues delegate, RowIdMapping rowIdMapping, long generation) {
            this.delegate = delegate;
            this.rowIdMapping = rowIdMapping;
            this.generation = generation;
        }

        @Override
        public long nextValue() throws IOException {
            long oldRowId = delegate.nextValue();
            return rowIdMapping.getNewRowId(oldRowId, generation);
        }

        @Override
        public int docValueCount() {
            return delegate.docValueCount();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return delegate.advanceExact(target);
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return delegate.advance(target);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }

    /**
     * Assigns sequential {@code ___row_id} = {@code rowIdOffset + docID}.
     * Used when no RowIdMapping is provided.
     */
    private static class SequentialRowIdDocValues extends SortedNumericDocValues {

        private final int maxDoc;
        private final int rowIdOffset;
        private int docID = -1;

        SequentialRowIdDocValues(int maxDoc, int rowIdOffset) {
            this.maxDoc = maxDoc;
            this.rowIdOffset = rowIdOffset;
        }

        @Override
        public long nextValue() {
            return rowIdOffset + docID;
        }

        @Override
        public int docValueCount() {
            return 1;
        }

        @Override
        public boolean advanceExact(int target) {
            docID = target;
            return true;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int nextDoc() {
            return ++docID < maxDoc ? docID : NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            docID = target;
            return docID < maxDoc ? docID : NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }
}
