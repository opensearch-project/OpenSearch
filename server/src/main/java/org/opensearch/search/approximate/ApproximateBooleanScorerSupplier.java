/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A ScorerSupplier implementation for ApproximateBooleanQuery that creates resumable DocIdSetIterators
 * for each clause and coordinates their usage in the boolean query context.
 */
public class ApproximateBooleanScorerSupplier extends ScorerSupplier {
    private final List<ScorerSupplier> clauseScorerSuppliers;
    private final ScoreMode scoreMode;
    private final float boost;
    private final int threshold;
    private final LeafReaderContext context;
    private long cost = -1;

    /**
     * Creates a new ApproximateBooleanScorerSupplier.
     *
     * @param clauseWeights The weights for each clause in the boolean query
     * @param scoreMode The score mode
     * @param boost The boost factor
     * @param threshold The threshold for early termination
     * @param context The leaf reader context
     * @throws IOException If there's an error creating scorer suppliers
     */
    public ApproximateBooleanScorerSupplier(
        List<Weight> clauseWeights,
        ScoreMode scoreMode,
        float boost,
        int threshold,
        LeafReaderContext context
    ) throws IOException {
        this.clauseScorerSuppliers = new ArrayList<>(clauseWeights.size());
        this.scoreMode = scoreMode;
        this.boost = boost;
        this.threshold = threshold;
        this.context = context;

        // Create scorer suppliers for each clause
        for (Weight clauseWeight : clauseWeights) {
            ScorerSupplier supplier = clauseWeight.scorerSupplier(context);
            if (supplier != null) {
                clauseScorerSuppliers.add(supplier);
            }
        }
    }

    /**
     * Get the {@link Scorer}. This may not return {@code null} and must be called at most once.
     *
     * @param leadCost Cost of the scorer that will be used in order to lead iteration.
     */
    @Override
    public Scorer get(long leadCost) throws IOException {
        if (clauseScorerSuppliers.isEmpty()) {
            return null;
        }

        // Create ResumableDISIs for each clause
        List<ResumableDISI> clauseIterators = new ArrayList<>(clauseScorerSuppliers.size());
        for (ScorerSupplier supplier : clauseScorerSuppliers) {
            ResumableDISI disi = new ResumableDISI(supplier);
            clauseIterators.add(disi);
        }

        // Create an ApproximateConjunctionScorer with the clause iterators
        return new ApproximateConjunctionScorer(boost, scoreMode, clauseIterators);
    }

    /**
     * Get a scorer that is optimized for bulk-scoring.
     */
    @Override
    public BulkScorer bulkScorer() throws IOException {
        if (clauseScorerSuppliers.isEmpty()) {
            return null;
        }

        // Create ResumableDISIs for each clause
        List<ResumableDISI> clauseIterators = new ArrayList<>(clauseScorerSuppliers.size());
        for (ScorerSupplier supplier : clauseScorerSuppliers) {
            ResumableDISI disi = new ResumableDISI(supplier);
            clauseIterators.add(disi);
        }

        // Create an ApproximateBooleanBulkScorer with the clause iterators
        return new ApproximateBooleanBulkScorer(clauseIterators, threshold);
    }

    /**
     * Get an estimate of the {@link Scorer} that would be returned by {@link #get}.
     */
    @Override
    public long cost() {
        if (cost == -1) {
            // Estimate cost as the minimum cost of all clauses (conjunction)
            if (!clauseScorerSuppliers.isEmpty()) {
                cost = Long.MAX_VALUE;
                for (ScorerSupplier supplier : clauseScorerSuppliers) {
                    cost = Math.min(cost, supplier.cost());
                }
            } else {
                cost = 0;
            }
        }
        return cost;
    }

    /**
     * A BulkScorer implementation that coordinates multiple ResumableDISIs to implement
     * the circular scoring process described in the blog.
     */
    private static class ApproximateBooleanBulkScorer extends BulkScorer {
        private final List<ResumableDISI> clauseIterators;
        private final int threshold;

        public ApproximateBooleanBulkScorer(List<ResumableDISI> clauseIterators, int threshold) {
            this.clauseIterators = clauseIterators;
            this.threshold = threshold;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            // Create an ApproximateConjunctionDISI to coordinate the clause iterators
            ApproximateConjunctionDISI conjunctionDISI = new ApproximateConjunctionDISI(clauseIterators);

            // Create a scorer for the collector
            ApproximateConjunctionScorer scorer = new ApproximateConjunctionScorer(1.0f, ScoreMode.COMPLETE, clauseIterators);

            // Set the scorer on the collector
            collector.setScorer(scorer);

            // Track how many documents we've collected
            int collected = 0;
            int docID;

            // Collect documents until we reach the threshold or exhaust the iterator
            while (collected < threshold && (docID = conjunctionDISI.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (docID >= max) {
                    // We've reached the end of the range
                    return docID;
                }

                if (docID >= min && (acceptDocs == null || acceptDocs.get(docID))) {
                    // Collect the document
                    collector.collect(docID);
                    collected++;
                }
            }

            // If we haven't collected enough documents and the iterator isn't exhausted,
            // we need to rescore the clauses and continue
            if (collected < threshold && !conjunctionDISI.isExhausted()) {
                // Reset each clause iterator for the next batch
                for (ResumableDISI disi : clauseIterators) {
                    disi.resetForNextBatch();
                }

                // Create a new conjunction DISI with the reset iterators
                conjunctionDISI = new ApproximateConjunctionDISI(clauseIterators);

                // Continue collecting documents
                while (collected < threshold && (docID = conjunctionDISI.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (docID >= max) {
                        // We've reached the end of the range
                        return docID;
                    }

                    if (docID >= min && (acceptDocs == null || acceptDocs.get(docID))) {
                        // Collect the document
                        collector.collect(docID);
                        collected++;
                    }
                }
            }

            // We've either collected enough documents or exhausted the iterator
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        /**
         * Same as {@link DocIdSetIterator#cost()} for bulk scorers.
         */
        @Override
        public long cost() {
            return 0;
        }
    }
}
