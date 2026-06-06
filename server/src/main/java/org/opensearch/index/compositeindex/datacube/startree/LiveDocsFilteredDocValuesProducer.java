/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Arrays;

/**
 * Wraps a {@link DocValuesProducer} and filters out soft-deleted documents using a
 * {@link Bits} liveDocs bitset.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>Remap mode</b> (3-arg constructor): Remaps document IDs to contiguous space
 *       (0 to numLiveDocs-1). Uses {@code advanceExact()} on the delegate. Used during
 *       star tree construction in the upgrade path.</li>
 *   <li><b>Skip-only mode</b> (2-arg constructor): No remapping. Skips deleted docs during
 *       sequential {@code nextDoc()} iteration. Never calls {@code advanceExact()} on the
 *       delegate. Used during star tree construction in the merge fallback path where
 *       merge producers only support sequential access.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LiveDocsFilteredDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer delegate;
    private final Bits liveDocs;
    private final int[] remappedToOriginal; // null = skip-only mode
    private final int numLiveDocs;

    /**
     * Remap mode constructor — used in upgrade build path.
     * Remaps doc IDs to contiguous space. Requires delegate to support advanceExact().
     *
     * @param delegate the underlying producer to wrap
     * @param liveDocs bitset where set bits indicate live (non-deleted) documents
     * @param maxDoc   total number of documents in the segment (including deleted)
     */
    public LiveDocsFilteredDocValuesProducer(DocValuesProducer delegate, Bits liveDocs, int maxDoc) {
        this.delegate = delegate;
        this.liveDocs = liveDocs;
        if (liveDocs == null) {
            // No deletes — identity mapping
            this.remappedToOriginal = new int[maxDoc];
            for (int i = 0; i < maxDoc; i++) {
                remappedToOriginal[i] = i;
            }
            this.numLiveDocs = maxDoc;
        } else {
            int[] temp = new int[maxDoc];
            int count = 0;
            for (int i = 0; i < maxDoc; i++) {
                if (liveDocs.get(i)) {
                    temp[count++] = i;
                }
            }
            this.remappedToOriginal = Arrays.copyOf(temp, count);
            this.numLiveDocs = count;
        }
    }

    /**
     * Skip-only mode constructor — used in merge fallback path.
     * No remapping. Skips deleted docs during sequential nextDoc() iteration.
     * Does NOT call advanceExact() on the delegate.
     *
     * @param delegate the underlying producer to wrap
     * @param liveDocs bitset where set bits indicate live (non-deleted) documents
     */
    public LiveDocsFilteredDocValuesProducer(DocValuesProducer delegate, Bits liveDocs) {
        this.delegate = delegate;
        this.liveDocs = liveDocs;
        this.remappedToOriginal = null; // null signals skip-only mode
        this.numLiveDocs = -1;
    }

    public int getNumLiveDocs() {
        return numLiveDocs;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        NumericDocValues inner = delegate.getNumeric(field);
        if (inner == null) return null;
        return new NumericDocValues() {
            int currentRemappedId = -1;

            @Override
            public boolean advanceExact(int remappedId) throws IOException {
                currentRemappedId = remappedId;
                if (remappedId < 0 || remappedId >= remappedToOriginal.length) {
                    return false;
                }
                return inner.advanceExact(remappedToOriginal[remappedId]);
            }

            @Override
            public int nextDoc() throws IOException {
                currentRemappedId++;
                while (currentRemappedId < numLiveDocs) {
                    if (inner.advanceExact(remappedToOriginal[currentRemappedId])) {
                        return currentRemappedId;
                    }
                    currentRemappedId++;
                }
                currentRemappedId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                currentRemappedId = target;
                return nextDoc();
            }

            @Override
            public long longValue() throws IOException {
                return inner.longValue();
            }

            @Override
            public int docID() {
                return currentRemappedId;
            }

            @Override
            public long cost() {
                return numLiveDocs;
            }
        };
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        SortedNumericDocValues inner = delegate.getSortedNumeric(field);
        if (inner == null) return null;

        if (remappedToOriginal == null) {
            // Skip-only mode: sequential iteration, no remapping
            return new SortedNumericDocValues() {
                @Override
                public int nextDoc() throws IOException {
                    int doc;
                    while ((doc = inner.nextDoc()) != NO_MORE_DOCS) {
                        if (liveDocs.get(doc)) return doc;
                    }
                    return NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) throws IOException {
                    int doc = inner.advance(target);
                    if (doc == NO_MORE_DOCS) return NO_MORE_DOCS;
                    if (liveDocs.get(doc)) return doc;
                    return nextDoc();
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    throw new UnsupportedOperationException("advanceExact not supported in skip-only mode (merge path)");
                }

                @Override
                public long nextValue() throws IOException { return inner.nextValue(); }

                @Override
                public int docValueCount() { return inner.docValueCount(); }

                @Override
                public int docID() { return inner.docID(); }

                @Override
                public long cost() { return inner.cost(); }
            };
        }

        // Remap mode: contiguous doc ID space
        return new SortedNumericDocValues() {
            int currentRemappedId = -1;

            @Override
            public boolean advanceExact(int remappedId) throws IOException {
                currentRemappedId = remappedId;
                if (remappedId < 0 || remappedId >= remappedToOriginal.length) {
                    return false;
                }
                return inner.advanceExact(remappedToOriginal[remappedId]);
            }

            @Override
            public int nextDoc() throws IOException {
                currentRemappedId++;
                while (currentRemappedId < numLiveDocs) {
                    if (inner.advanceExact(remappedToOriginal[currentRemappedId])) {
                        return currentRemappedId;
                    }
                    currentRemappedId++;
                }
                currentRemappedId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                currentRemappedId = target;
                return nextDoc();
            }

            @Override
            public long nextValue() throws IOException {
                return inner.nextValue();
            }

            @Override
            public int docValueCount() {
                return inner.docValueCount();
            }

            @Override
            public int docID() {
                return currentRemappedId;
            }

            @Override
            public long cost() {
                return numLiveDocs;
            }
        };
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        SortedDocValues inner = delegate.getSorted(field);
        if (inner == null) return null;
        return new SortedDocValues() {
            int currentRemappedId = -1;

            @Override
            public boolean advanceExact(int remappedId) throws IOException {
                currentRemappedId = remappedId;
                if (remappedId < 0 || remappedId >= remappedToOriginal.length) {
                    return false;
                }
                return inner.advanceExact(remappedToOriginal[remappedId]);
            }

            @Override
            public int ordValue() throws IOException {
                return inner.ordValue();
            }

            @Override
            public org.apache.lucene.util.BytesRef lookupOrd(int ord) throws IOException {
                return inner.lookupOrd(ord);
            }

            @Override
            public int getValueCount() {
                return inner.getValueCount();
            }

            @Override
            public int docID() {
                return currentRemappedId;
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException("Sequential access not supported in filtered view");
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException("Random advance not supported in filtered view");
            }

            @Override
            public long cost() {
                return numLiveDocs;
            }
        };
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetDocValues inner = delegate.getSortedSet(field);
        if (inner == null) return null;

        if (remappedToOriginal == null) {
            // Skip-only mode: sequential iteration, no remapping
            return new SortedSetDocValues() {
                @Override
                public int nextDoc() throws IOException {
                    int doc;
                    while ((doc = inner.nextDoc()) != NO_MORE_DOCS) {
                        if (liveDocs.get(doc)) return doc;
                    }
                    return NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) throws IOException {
                    int doc = inner.advance(target);
                    if (doc == NO_MORE_DOCS) return NO_MORE_DOCS;
                    if (liveDocs.get(doc)) return doc;
                    return nextDoc();
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    throw new UnsupportedOperationException("advanceExact not supported in skip-only mode (merge path)");
                }

                @Override
                public long nextOrd() throws IOException { return inner.nextOrd(); }

                @Override
                public int docValueCount() { return inner.docValueCount(); }

                @Override
                public org.apache.lucene.util.BytesRef lookupOrd(long ord) throws IOException { return inner.lookupOrd(ord); }

                @Override
                public long getValueCount() { return inner.getValueCount(); }

                @Override
                public int docID() { return inner.docID(); }

                @Override
                public long cost() { return inner.cost(); }
            };
        }

        // Remap mode: contiguous doc ID space
        return new SortedSetDocValues() {
            int currentRemappedId = -1;

            @Override
            public boolean advanceExact(int remappedId) throws IOException {
                currentRemappedId = remappedId;
                if (remappedId < 0 || remappedId >= remappedToOriginal.length) {
                    return false;
                }
                return inner.advanceExact(remappedToOriginal[remappedId]);
            }

            @Override
            public int nextDoc() throws IOException {
                currentRemappedId++;
                while (currentRemappedId < numLiveDocs) {
                    if (inner.advanceExact(remappedToOriginal[currentRemappedId])) {
                        return currentRemappedId;
                    }
                    currentRemappedId++;
                }
                currentRemappedId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                currentRemappedId = target;
                return nextDoc();
            }

            @Override
            public long nextOrd() throws IOException {
                return inner.nextOrd();
            }

            @Override
            public int docValueCount() {
                return inner.docValueCount();
            }

            @Override
            public org.apache.lucene.util.BytesRef lookupOrd(long ord) throws IOException {
                return inner.lookupOrd(ord);
            }

            @Override
            public long getValueCount() {
                return inner.getValueCount();
            }

            @Override
            public int docID() {
                return currentRemappedId;
            }

            @Override
            public long cost() {
                return numLiveDocs;
            }
        };
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        BinaryDocValues inner = delegate.getBinary(field);
        if (inner == null) return null;
        return new BinaryDocValues() {
            int currentRemappedId = -1;

            @Override
            public boolean advanceExact(int remappedId) throws IOException {
                currentRemappedId = remappedId;
                if (remappedId < 0 || remappedId >= remappedToOriginal.length) {
                    return false;
                }
                return inner.advanceExact(remappedToOriginal[remappedId]);
            }

            @Override
            public org.apache.lucene.util.BytesRef binaryValue() throws IOException {
                return inner.binaryValue();
            }

            @Override
            public int docID() {
                return currentRemappedId;
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException("Sequential access not supported in filtered view");
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException("Random advance not supported in filtered view");
            }

            @Override
            public long cost() {
                return numLiveDocs;
            }
        };
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
}
