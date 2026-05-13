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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Wraps merge-path DocValuesProducers to skip soft-deleted docs during
 * sequential iteration. Unlike LiveDocsFilteredDocValuesProducer, this does
 * NOT remap doc IDs — it skips deleted docs in-place using nextDoc().
 * Required because merge producers only support sequential access (no advanceExact).
 *
 * @opensearch.experimental
 */
public class SequentialLiveDocsFilteredDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer delegate;
    private final Bits liveDocs;
    private final int maxDoc;

    public SequentialLiveDocsFilteredDocValuesProducer(DocValuesProducer delegate, Bits liveDocs, int maxDoc) {
        this.delegate = delegate;
        this.liveDocs = liveDocs;
        this.maxDoc = maxDoc;
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        SortedNumericDocValues inner = delegate.getSortedNumeric(field);
        return new SortedNumericDocValues() {
            int currentDoc = -1;

            @Override
            public int nextDoc() throws IOException {
                int doc;
                while ((doc = inner.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (liveDocs.get(doc)) {
                        currentDoc = doc;
                        return doc;
                    }
                }
                currentDoc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                int doc = inner.advance(target);
                if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                    currentDoc = DocIdSetIterator.NO_MORE_DOCS;
                    return DocIdSetIterator.NO_MORE_DOCS;
                }
                if (liveDocs.get(doc)) {
                    currentDoc = doc;
                    return doc;
                }
                return nextDoc();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException("advanceExact not supported on merge-path sequential producers");
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
                return currentDoc;
            }

            @Override
            public long cost() {
                return inner.cost();
            }
        };
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetDocValues inner = delegate.getSortedSet(field);
        return new SortedSetDocValues() {
            int currentDoc = -1;

            @Override
            public int nextDoc() throws IOException {
                int doc;
                while ((doc = inner.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (liveDocs.get(doc)) {
                        currentDoc = doc;
                        return doc;
                    }
                }
                currentDoc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                int doc = inner.advance(target);
                if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                    currentDoc = DocIdSetIterator.NO_MORE_DOCS;
                    return DocIdSetIterator.NO_MORE_DOCS;
                }
                if (liveDocs.get(doc)) {
                    currentDoc = doc;
                    return doc;
                }
                return nextDoc();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException("advanceExact not supported on merge-path sequential producers");
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
            public BytesRef lookupOrd(long ord) throws IOException {
                return inner.lookupOrd(ord);
            }

            @Override
            public long getValueCount() {
                return inner.getValueCount();
            }

            @Override
            public int docID() {
                return currentDoc;
            }

            @Override
            public long cost() {
                return inner.cost();
            }
        };
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
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return delegate.getSkipper(field);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }
}
