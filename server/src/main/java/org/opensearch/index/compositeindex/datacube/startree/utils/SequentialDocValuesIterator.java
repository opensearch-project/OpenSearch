/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;

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
     * The value associated with the latest document.
     */
    private Long docValue;

    /**
     * The id of the latest document.
     */
    private int docId;

    /**
     * Constructs a new SequentialDocValuesIterator instance with the given DocIdSetIterator.
     *
     * @param docIdSetIterator the DocIdSetIterator to be associated with this instance
     */
    public SequentialDocValuesIterator(DocIdSetIterator docIdSetIterator) {
        this.docIdSetIterator = docIdSetIterator;
    }

    /**
     * Returns the value associated with the latest document.
     *
     * @return the value associated with the latest document
     */
    public Long getDocValue() {
        return docValue;
    }

    /**
     * Sets the value associated with the latest document.
     *
     * @param docValue the value to be associated with the latest document
     */
    public void setDocValue(Long docValue) {
        this.docValue = docValue;
    }

    /**
     * Returns the id of the latest document.
     *
     * @return the id of the latest document
     */
    public int getDocId() {
        return docId;
    }

    /**
     * Sets the id of the latest document.
     *
     * @param docId the ID of the latest document
     */
    public void setDocId(int docId) {
        this.docId = docId;
    }

    /**
     * Returns the DocIdSetIterator associated with this instance.
     *
     * @return the DocIdSetIterator associated with this instance
     */
    public DocIdSetIterator getDocIdSetIterator() {
        return docIdSetIterator;
    }
}
