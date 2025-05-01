/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;

/**
 * This class functions as a utility for propagating block boundaries within disjunctions.
 * In disjunctions, where a match occurs if any subclause matches, a common approach might involve returning
 * the minimum block boundary across all clauses. However, this method can introduce performance challenges,
 * particularly when dealing with high minimum competitive scores and clauses with low scores that no longer
 * significantly contribute to the iteration process. Therefore, this class computes block boundaries solely for clauses
 * with a maximum score equal to or exceeding the minimum competitive score, or for the clause with the maximum
 * score if such a clause is absent.
 */
public class HybridScoreBlockBoundaryPropagator {

    private static final Comparator<Scorer> MAX_SCORE_COMPARATOR = Comparator.comparing((Scorer s) -> {
        try {
            return s.getMaxScore(DocIdSetIterator.NO_MORE_DOCS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }).thenComparing(s -> s.iterator().cost());

    private final Scorer[] scorers;
    private final float[] maxScores;
    private int leadIndex = 0;

    HybridScoreBlockBoundaryPropagator(final Collection<Scorer> scorers) throws IOException {
        this.scorers = scorers.stream().filter(Objects::nonNull).toArray(Scorer[]::new);
        for (Scorer scorer : this.scorers) {
            scorer.advanceShallow(0);
        }
        Arrays.sort(this.scorers, MAX_SCORE_COMPARATOR);

        maxScores = new float[this.scorers.length];
        for (int i = 0; i < this.scorers.length; ++i) {
            maxScores[i] = this.scorers[i].getMaxScore(DocIdSetIterator.NO_MORE_DOCS);
        }
    }

    /** See {@link Scorer#advanceShallow(int)}. */
    int advanceShallow(int target) throws IOException {
        // For scorers that are below the lead index, just propagate.
        for (int i = 0; i < leadIndex; ++i) {
            Scorer s = scorers[i];
            if (s.docID() < target) {
                s.advanceShallow(target);
            }
        }

        // For scorers above the lead index, we take the minimum
        // boundary.
        Scorer leadScorer = scorers[leadIndex];
        int upTo = leadScorer.advanceShallow(Math.max(leadScorer.docID(), target));

        for (int i = leadIndex + 1; i < scorers.length; ++i) {
            Scorer scorer = scorers[i];
            if (scorer.docID() <= target) {
                upTo = Math.min(scorer.advanceShallow(target), upTo);
            }
        }

        // If the maximum scoring clauses are beyond `target`, then we use their
        // docID as a boundary. It helps not consider them when computing the
        // maximum score and get a lower score upper bound.
        for (int i = scorers.length - 1; i > leadIndex; --i) {
            Scorer scorer = scorers[i];
            if (scorer.docID() > target) {
                upTo = Math.min(upTo, scorer.docID() - 1);
            } else {
                break;
            }
        }
        return upTo;
    }

    /**
     * Set the minimum competitive score to filter out clauses that score less than this threshold.
     *
     * @see Scorer#setMinCompetitiveScore
     */
    void setMinCompetitiveScore(float minScore) throws IOException {
        // Update the lead index if necessary
        while (leadIndex < maxScores.length - 1 && minScore > maxScores[leadIndex]) {
            leadIndex++;
        }
    }
}
