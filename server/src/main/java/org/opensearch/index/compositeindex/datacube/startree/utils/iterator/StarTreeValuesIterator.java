/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils.iterator;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Wrapper iterator class for StarTree index in place of DocIdSetIterator to read / traverse the docValues formats.
 * This is needed since star tree values are different from segment documents and number of star tree values
 * can even exceed segment docs in the worst cases.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class StarTreeValuesIterator {

    public static final int NO_MORE_ENTRIES = Integer.MAX_VALUE;
    protected final DocIdSetIterator docIdSetIterator;

    public StarTreeValuesIterator(DocIdSetIterator docIdSetIterator) {
        this.docIdSetIterator = docIdSetIterator;
    }

    public int entryId() {
        return docIdSetIterator.docID();
    }

    public int nextEntry() throws IOException {
        return docIdSetIterator.nextDoc();
    }

    public int advance(int target) throws IOException {
        return docIdSetIterator.advance(target);
    }

    public long cost() {
        return docIdSetIterator.cost();
    }
}
