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
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.io.IOException;

/**
 * {@link DocValuesProducer} that intercepts the Engine-4 element index's
 * {@link DocumentInput#NESTED_PARENT_ROW_FIELD} field and returns parent row ids remapped through the
 * document merge's {@link RowIdMapping}. All other fields are delegated unchanged.
 *
 * <p>On a merge the parquet primary renumbers parent rows and produces a {@link RowIdMapping} keyed by
 * {@code (oldParentRow, parentGeneration)}. Each source element segment came from one parent
 * generation, so its elements' {@code __parent_row__} values (parent rows local to that generation)
 * are remapped with {@code getNewRowId(oldParentRow, parentGeneration)} to point at the merged rows.
 * This is the {@code __parent_row__} analogue of {@link RowIdRemappingDocValuesProducer} (which does the
 * same for the main index's own {@code __row_id__}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class NestedParentRowRemappingDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer delegate;
    private final RowIdMapping documentMapping;
    private final long parentGeneration;

    /**
     * @param delegate         the original doc values producer for the source element segment
     * @param documentMapping  the parent-row mapping the document merge produced (must be non-null)
     * @param parentGeneration the parent document generation this element segment belongs to, used to
     *                         key {@code documentMapping}
     */
    NestedParentRowRemappingDocValuesProducer(DocValuesProducer delegate, RowIdMapping documentMapping, long parentGeneration) {
        this.delegate = delegate;
        this.documentMapping = documentMapping;
        this.parentGeneration = parentGeneration;
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        if (DocumentInput.NESTED_PARENT_ROW_FIELD.equals(field.name)) {
            return new MappedParentRowDocValues(delegate.getSortedNumeric(field), documentMapping, parentGeneration);
        }
        return delegate.getSortedNumeric(field);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
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

    /** Reads the original {@code __parent_row__} and maps it through the document {@link RowIdMapping}. */
    private static class MappedParentRowDocValues extends SortedNumericDocValues {

        private final SortedNumericDocValues delegate;
        private final RowIdMapping documentMapping;
        private final long parentGeneration;

        MappedParentRowDocValues(SortedNumericDocValues delegate, RowIdMapping documentMapping, long parentGeneration) {
            this.delegate = delegate;
            this.documentMapping = documentMapping;
            this.parentGeneration = parentGeneration;
        }

        @Override
        public long nextValue() throws IOException {
            long oldParentRow = delegate.nextValue();
            long newParentRow = documentMapping.getNewRowId(oldParentRow, parentGeneration);
            if (newParentRow < 0L) {
                throw new IllegalStateException(
                    "Document merge RowIdMapping has no entry for parent row ["
                        + oldParentRow
                        + "] in generation ["
                        + parentGeneration
                        + "]; refusing to write a stale __parent_row__ into the merged element index"
                );
            }
            return newParentRow;
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
}
