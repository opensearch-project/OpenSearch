/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;

import java.io.IOException;
import java.util.List;

/**
 * A custom Scorer that manages an ApproximateConjunctionDISI.
 * This class creates and manages an ApproximateConjunctionDISI to score documents
 * that match all clauses in a boolean query.
 */
public class ApproximateConjunctionScorer extends Scorer {
    private final ApproximateConjunctionDISI approximateConjunctionDISI;
    private final float score;

    /**
     * Creates a new ApproximateConjunctionScorer.
     *
     * @param boost The boost factor
     * @param scoreMode The score mode
     * @param iterators The iterators to coordinate
     */
    public ApproximateConjunctionScorer(float boost, ScoreMode scoreMode, List<ResumableDISI> iterators) {
        // Scorer doesn't have a constructor that takes arguments
        this.approximateConjunctionDISI = new ApproximateConjunctionDISI(iterators);
        this.score = boost;
    }

    @Override
    public DocIdSetIterator iterator() {
        return approximateConjunctionDISI;
    }

    @Override
    public float score() throws IOException {
        return score;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return score;
    }

    @Override
    public int docID() {
        return approximateConjunctionDISI.docID();
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        return null; // No two-phase iteration needed for conjunction
    }
}
