/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * A {@link DocValuesFormat} used by {@link LuceneWriterCodec} that intercepts writes to the
 * {@code ___row_id} field and replaces the values with sequential 0..N when row ID rewriting
 * is enabled. All other fields are delegated unchanged to the underlying format.
 * <p>
 * This allows the reorder merge and the row ID rewrite to happen in a single pass during
 * {@link LuceneWriter#flush}, ensuring that after a sorted merge the Lucene doc IDs align
 * with the new sequential row IDs.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneWriterDocValuesFormat extends DocValuesFormat {

    private static final String ROW_ID = LuceneDocumentInput.ROW_ID_FIELD;

    private final DocValuesFormat delegate;

    /**
     * Creates a new doc values format that wraps the given delegate and rewrites
     * the {@code ___row_id} field with sequential values.
     *
     * @param delegate the underlying doc values format to delegate operations to
     */
    public LuceneWriterDocValuesFormat(DocValuesFormat delegate) {
        super(delegate.getName());
        this.delegate = delegate;
    }

    /**
     * Returns the delegate format.
     */
    DocValuesFormat getDelegate() {
        return delegate;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        DocValuesConsumer delegateConsumer = delegate.fieldsConsumer(state);
        return new RowIdRewritingDocValuesConsumer(delegateConsumer, state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }

    /**
     * A {@link DocValuesConsumer} that intercepts {@code addSortedNumericField} for the
     * {@code ___row_id} field and replaces the values with sequential 0..maxDoc.
     * All other fields are passed through to the delegate consumer unchanged.
     */
    static class RowIdRewritingDocValuesConsumer extends DocValuesConsumer {

        private final DocValuesConsumer delegate;
        private final SegmentWriteState state;

        RowIdRewritingDocValuesConsumer(DocValuesConsumer delegate, SegmentWriteState state) {
            this.delegate = delegate;
            this.state = state;
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            if (ROW_ID.equals(field.name)) {
                delegate.addSortedNumericField(field, new SequentialRowIdProducer(state.segmentInfo.maxDoc()));
            } else {
                delegate.addSortedNumericField(field, valuesProducer);
            }
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addNumericField(field, valuesProducer);
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addBinaryField(field, valuesProducer);
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addSortedField(field, valuesProducer);
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addSortedSetField(field, valuesProducer);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * A {@link DocValuesProducer} that produces sequential row IDs (0, 1, 2, ..., maxDoc-1)
     * for the {@code ___row_id} sorted numeric field.
     */
    static class SequentialRowIdProducer extends DocValuesProducer {

        private final int maxDoc;

        SequentialRowIdProducer(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
            return new SequentialRowIdDocValues(maxDoc);
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) {
            return null;
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) {
            return null;
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) {
            return null;
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) {
            return null;
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo field) {
            return null;
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public void close() {}
    }

    /**
     * {@link SortedNumericDocValues} that returns sequential values where each doc's
     * row ID equals its doc ID (0, 1, 2, ...).
     */
    static class SequentialRowIdDocValues extends SortedNumericDocValues {

        private final int maxDoc;
        private int docID = -1;

        SequentialRowIdDocValues(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public long nextValue() {
            return docID;
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
