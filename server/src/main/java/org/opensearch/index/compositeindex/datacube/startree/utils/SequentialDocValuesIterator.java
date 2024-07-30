
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Coordinates the reading of documents across multiple DocIdSetIterators.
 * It encapsulates a single DocIdSetIterator and maintains the latest document ID and its associated value.
 * @opensearch.experimental
 */
@ExperimentalApi
public class SequentialDocValuesIterator {

    /**
     * The doc id set iterator associated for each field.
     */
    private final DocIdSetIterator docIdSetIterator;

    /**
     * The id of the latest document.
     */
    private int docId = -1;

    /**
     * Constructs a new SequentialDocValuesIterator instance with the given DocIdSetIterator.
     *
     * @param docIdSetIterator the DocIdSetIterator to be associated with this instance
     */
    public SequentialDocValuesIterator(DocIdSetIterator docIdSetIterator) {
        this.docIdSetIterator = docIdSetIterator;
    }

    /**
     * Returns the id of the latest document.
     *
     * @return the id of the latest document
     */
    int getDocId() {
        return docId;
    }

    /**
     * Returns the DocIdSetIterator associated with this instance.
     *
     * @return the DocIdSetIterator associated with this instance
     */
    public DocIdSetIterator getDocIdSetIterator() {
        return docIdSetIterator;
    }

    public int nextDoc(int currentDocId) throws IOException {
        // if doc id stored is less than or equal to the requested doc id , return the stored doc id
        if (docId >= currentDocId) {
            return docId;
        }
        docId = this.docIdSetIterator.nextDoc();
        return docId;
    }

    public Long value(int currentDocId) throws IOException {
        if (this.getDocIdSetIterator() instanceof SortedNumericDocValues) {
            SortedNumericDocValues sortedNumericDocValues = (SortedNumericDocValues) this.getDocIdSetIterator();
            if (currentDocId < 0) {
                throw new IllegalStateException("invalid doc id to fetch the next value");
            }
            if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                throw new IllegalStateException("DocValuesIterator is already exhausted");
            }
            if (docId == DocIdSetIterator.NO_MORE_DOCS || docId != currentDocId) {
                return null;
            }
            return sortedNumericDocValues.nextValue();

        } else {
            throw new IllegalStateException("Unsupported Iterator requested for SequentialDocValuesIterator");
        }
    }
}
