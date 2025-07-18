/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

public class ApproximateConjunctionScorer extends Scorer {
    /**
     * Returns the doc ID that is currently being scored.
     */
    @Override
    public int docID() {
        return 0;
    }

    /**
     * Return a {@link DocIdSetIterator} over matching documents.
     *
     * <p>The returned iterator will either be positioned on {@code -1} if no documents have been
     * scored yet, {@link DocIdSetIterator#NO_MORE_DOCS} if all documents have been scored already, or
     * the last document id that has been scored otherwise.
     *
     * <p>The returned iterator is a view: calling this method several times will return iterators
     * that have the same state.
     */
    @Override
    public DocIdSetIterator iterator() {
        return null;
    }

    /**
     * Return the maximum score that documents between the last {@code target} that this iterator was
     * {@link #advanceShallow(int) shallow-advanced} to included and {@code upTo} included.
     *
     * @param upTo
     */
    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    /**
     * Returns the score of the current document matching the query.
     */
    @Override
    public float score() throws IOException {
        return 0;
    }
}
