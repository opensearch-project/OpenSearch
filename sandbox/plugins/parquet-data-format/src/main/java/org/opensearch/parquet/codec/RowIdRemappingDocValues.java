/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.codec.cache.RowIdStats;

import java.io.IOException;

/**
 * Decorators that adapt the Parquet DocValues iterators (which address values by Parquet
 * <b>row position</b>) to Lucene's {@code docId}-space contract, translating each {@code docId} to
 * its Parquet row via a {@link RowIdResolver} before delegating.
 *
 * <p>The wrapped (delegate) iterator produced by {@link ParquetDocValuesProducer} treats the integer
 * passed to {@code advanceExact} as the Parquet row position. These decorators therefore:
 * <ul>
 *   <li>accept {@code advanceExact(docId)} in Lucene doc-id space,</li>
 *   <li>resolve {@code row = resolver.toRowId(docId)},</li>
 *   <li>delegate {@code advanceExact((int) row)} to read the value at that Parquet row, and</li>
 *   <li>expose {@code docID()} / {@code nextDoc()} / {@code advance()} in doc-id space.</li>
 * </ul>
 *
 * <p>When the resolver is {@link RowIdResolver#IDENTITY} the translation is a no-op and behavior is
 * identical to using the delegate directly (the un-merged, single-generation case).
 *
 * <p>Value accessors ({@code longValue}, {@code binaryValue}, ordinal lookups, etc.) delegate
 * straight through, since after {@code advanceExact} the delegate is positioned on the correct row.
 */
final class RowIdRemappingDocValues {

    private RowIdRemappingDocValues() {}

    static NumericDocValues numeric(NumericDocValues delegate, RowIdResolver resolver, int maxDoc) {
        return new NumericDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                return delegate.advanceExact((int) resolver.toRowId(target));
            }

            @Override
            public long longValue() throws IOException {
                return delegate.longValue();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                for (int d = target; d < maxDoc; d++) {
                    if (advanceExact(d)) {
                        return d;
                    }
                }
                doc = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return delegate.cost();
            }
        };
    }

    static SortedNumericDocValues sortedNumeric(SortedNumericDocValues delegate, RowIdResolver resolver, int maxDoc) {
        return new SortedNumericDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                return delegate.advanceExact((int) resolver.toRowId(target));
            }

            @Override
            public long nextValue() throws IOException {
                return delegate.nextValue();
            }

            @Override
            public int docValueCount() {
                return delegate.docValueCount();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                for (int d = target; d < maxDoc; d++) {
                    if (advanceExact(d)) {
                        return d;
                    }
                }
                doc = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return delegate.cost();
            }
        };
    }

    static BinaryDocValues binary(BinaryDocValues delegate, RowIdResolver resolver, int maxDoc) {
        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                return delegate.advanceExact((int) resolver.toRowId(target));
            }

            @Override
            public BytesRef binaryValue() throws IOException {
                return delegate.binaryValue();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                for (int d = target; d < maxDoc; d++) {
                    if (advanceExact(d)) {
                        return d;
                    }
                }
                doc = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return delegate.cost();
            }
        };
    }

    static SortedDocValues sorted(SortedDocValues delegate, RowIdResolver resolver, int maxDoc) {
        return new SortedDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                return delegate.advanceExact((int) resolver.toRowId(target));
            }

            @Override
            public int ordValue() throws IOException {
                return delegate.ordValue();
            }

            @Override
            public BytesRef lookupOrd(int ord) throws IOException {
                return delegate.lookupOrd(ord);
            }

            @Override
            public int getValueCount() {
                return delegate.getValueCount();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                for (int d = target; d < maxDoc; d++) {
                    if (advanceExact(d)) {
                        return d;
                    }
                }
                doc = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return delegate.cost();
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
                return delegate.termsEnum();
            }
        };
    }

    static SortedSetDocValues sortedSet(SortedSetDocValues delegate, RowIdResolver resolver, int maxDoc) {
        return new SortedSetDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                return delegate.advanceExact((int) resolver.toRowId(target));
            }

            @Override
            public long nextOrd() throws IOException {
                return delegate.nextOrd();
            }

            @Override
            public int docValueCount() {
                return delegate.docValueCount();
            }

            @Override
            public BytesRef lookupOrd(long ord) throws IOException {
                return delegate.lookupOrd(ord);
            }

            @Override
            public long getValueCount() {
                return delegate.getValueCount();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                for (int d = target; d < maxDoc; d++) {
                    if (advanceExact(d)) {
                        return d;
                    }
                }
                doc = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return delegate.cost();
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
                return delegate.termsEnum();
            }
        };
    }

    /**
     * Builds a {@link RowIdResolver} from a {@code __row_id__} {@link SortedNumericDocValues} iterator
     * over the same segment ({@code __row_id__} is written as a single-valued
     * {@code SortedNumericDocValuesField}). The returned resolver reads the row-id value for each
     * {@code docId}; it is forward-only (callers must pass non-decreasing doc ids), matching the codec
     * iterators' access pattern. If {@code rowIdDocValues} is {@code null} (no row-id field in the
     * segment) this returns {@link RowIdResolver#IDENTITY}.
     *
     * <p>Each codec DocValues iterator advances independently, so callers must build a <b>separate</b>
     * resolver (backed by its own {@code __row_id__} iterator) per codec iterator.
     */
    static RowIdResolver resolverFrom(SortedNumericDocValues rowIdDocValues) {
        return resolverFrom(rowIdDocValues, null);
    }

    /**
     * As {@link #resolverFrom(SortedNumericDocValues)}, but records the lookup <b>count</b> into
     * {@code stats} (and marks IDENTITY when there is no row-id field). When {@code stats} is null,
     * no instrumentation is added (the resolver is the plain hot path).
     *
     * <p>Only the lookup count is recorded — never per-call timing. The per-document work is on the
     * order of tens of nanoseconds, comparable to {@code System.nanoTime()} itself, so timing each
     * call cannot produce a trustworthy figure; the actual wall-clock spent here is measured with a
     * CPU flamegraph instead (see {@link RowIdStats}).
     */
    static RowIdResolver resolverFrom(SortedNumericDocValues rowIdDocValues, RowIdStats stats) {
        if (rowIdDocValues == null) {
            if (stats != null) {
                stats.markIdentity();
            }
            return RowIdResolver.IDENTITY;
        }
        if (stats == null) {
            return docId -> {
                if (rowIdDocValues.advanceExact(docId) == false) {
                    throw new IllegalStateException(
                        "missing __row_id__ doc value for docId=" + docId + "; cannot translate to Parquet row position"
                    );
                }
                return rowIdDocValues.nextValue();
            };
        }
        return docId -> {
            if (rowIdDocValues.advanceExact(docId) == false) {
                throw new IllegalStateException(
                    "missing __row_id__ doc value for docId=" + docId + "; cannot translate to Parquet row position"
                );
            }
            // __row_id__ is single-valued; take the first (and only) value.
            long rowId = rowIdDocValues.nextValue();
            stats.recordLookup();
            return rowId;
        };
    }
}
