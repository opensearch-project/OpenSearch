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
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
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
 * for each clause and uses Lucene's ConjunctionUtils to coordinate them.
 */
public class ApproximateBooleanScorerSupplier extends ScorerSupplier {
    private final List<Weight> clauseWeights;
    private final List<ScorerSupplier> cachedSuppliers; // Cache suppliers to avoid repeated calls
    private final ScoreMode scoreMode;
    private final float boost;
    private final int size;
    private long cost = -1;

    /**
     * Creates a new ApproximateBooleanScorerSupplier.
     *
     * @param clauseWeights The weights for each clause in the boolean query
     * @param scoreMode The score mode
     * @param boost The boost factor
     * @param size The threshold for early termination
     * @param context The leaf reader context
     * @throws IOException If there's an error creating scorer suppliers
     */
    public ApproximateBooleanScorerSupplier(
        List<Weight> clauseWeights,
        ScoreMode scoreMode,
        float boost,
        int size,
        LeafReaderContext context
    ) throws IOException {
        this.clauseWeights = new ArrayList<>();
        this.cachedSuppliers = new ArrayList<>();
        this.scoreMode = scoreMode;
        this.boost = boost;
        this.size = size;

        // Store weights and cache their suppliers
        for (Weight clauseWeight : clauseWeights) {
            ScorerSupplier supplier = clauseWeight.scorerSupplier(context);
            if (supplier != null) {
                this.clauseWeights.add(clauseWeight);
                this.cachedSuppliers.add(supplier);
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
        if (clauseWeights.isEmpty()) {
            return null;
        }

        // Create appropriate iterators for each clause - ResumableDISI only for approximatable queries
        List<DocIdSetIterator> clauseIterators = new ArrayList<>(clauseWeights.size());
        for (int i = 0; i < clauseWeights.size(); i++) {
            // Use regular DocIdSetIterator for non-approximatable queries
            clauseIterators.add(cachedSuppliers.get(i).get(leadCost).iterator());
        }

        // Use Lucene's ConjunctionUtils to create the conjunction
        DocIdSetIterator conjunctionDISI = ConjunctionUtils.intersectIterators(clauseIterators);

        // Create a simple scorer that wraps the conjunction
        return new Scorer() {
            @Override
            public DocIdSetIterator iterator() {
                return conjunctionDISI;
            }

            @Override
            public float score() throws IOException {
                return 0.0f;
            }

            @Override
            public float getMaxScore(int upTo) throws IOException {
                return 0.0f;
            }

            @Override
            public int docID() {
                return conjunctionDISI.docID();
            }
        };
    }

    /**
     * Get an estimate of the {@link Scorer} that would be returned by {@link #get}.
     */
    @Override
    public long cost() {
        if (cost == -1) {
            // Estimate cost as the minimum cost of all clauses (conjunction)
            if (!cachedSuppliers.isEmpty()) {
                cost = Long.MAX_VALUE;
                for (ScorerSupplier supplier : cachedSuppliers) {
                    cost = Math.min(cost, supplier.cost());
                }
            } else {
                cost = 0;
            }
        }
        return cost;
    }

    /**
     * Get a scorer that is optimized for bulk-scoring.
     */
    @Override
    public BulkScorer bulkScorer() throws IOException {
        if (clauseWeights.isEmpty()) {
            return null;
        }

        // Calculate window size heuristic using cached suppliers
        long minCost = Long.MAX_VALUE;
        long maxCost = 0;
        for (ScorerSupplier supplier : cachedSuppliers) {
            long cost = supplier.cost();
            minCost = Math.min(minCost, cost);
            maxCost = Math.max(maxCost, cost);
        }
        final int initialWindowSize = Math.max((1 << 15), (int) Math.min(minCost, maxCost / (1 << 7))); // Ensure minimum 10k

        // Create a simple scorer for the collector (will be used by windowed approach)
        Scorer scorer = new Scorer() {
            @Override
            public DocIdSetIterator iterator() {
                // This won't be used in windowed approach
                return DocIdSetIterator.empty();
            }

            @Override
            public float score() throws IOException {
                return 0.0f;
            }

            @Override
            public float getMaxScore(int upTo) throws IOException {
                return 0.0f;
            }

            @Override
            public int docID() {
                return -1;
            }
        };

        // Create a simple bulk scorer that wraps the conjunction
        return new BulkScorer() {
            private int totalCollected = 0;

            // Windowed approach state
            private int currentWindowSize = initialWindowSize;
            private DocIdSetIterator globalConjunction = null;

            private List<DocIdSetIterator> rebuildIteratorsWithWindowSize(int windowSize) throws IOException {
                List<DocIdSetIterator> newIterators = new ArrayList<>();
                for (int i = 0; i < clauseWeights.size(); i++) {
                    Weight weight = clauseWeights.get(i);
                    ScorerSupplier supplier = cachedSuppliers.get(i); // Use cached supplier
                    Query query = weight.getQuery();

                    if (query instanceof ApproximatePointRangeQuery approxQuery) {
                        // For approximatable queries, try to use the window size
                        // Temporarily set the size
                        int originalSize = approxQuery.getSize();
                        approxQuery.setSize(windowSize);
                        try {
                            Scorer scorer = supplier.get(windowSize);
                            newIterators.add(scorer.iterator());
                        } finally {
                            // Restore original size
                            approxQuery.setSize(originalSize);
                        }
                    } else {
                        // Regular queries use full cost
                        Scorer scorer = supplier.get(supplier.cost());
                        newIterators.add(scorer.iterator());
                    }
                }
                return newIterators;
            }

            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                collector.setScorer(scorer);

                // Check if we need to expand window
                if (totalCollected < size && (globalConjunction == null || globalConjunction.docID() == DocIdSetIterator.NO_MORE_DOCS)) {
                    currentWindowSize *= 3;

                    // Rebuild iterators with new window size
                    List<DocIdSetIterator> newIterators = rebuildIteratorsWithWindowSize(currentWindowSize);
                    globalConjunction = ConjunctionUtils.intersectIterators(newIterators);

                    // Return first docID from new conjunction (could be < min)
                    int firstDoc = globalConjunction.nextDoc();
                    if (firstDoc != DocIdSetIterator.NO_MORE_DOCS) {
                        return firstDoc;  // CancellableBulkScorer will use this as new min
                    } else {}
                }

                // Score existing conjunction within [min, max) range
                int result = scoreExistingConjunction(collector, acceptDocs, min, max);

                return result;
            }

            private int scoreExistingConjunction(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                if (globalConjunction == null) {
                    return DocIdSetIterator.NO_MORE_DOCS;
                }

                // Position the iterator correctly
                if (globalConjunction.docID() < min) {
                    if (globalConjunction.docID() == min - 1) {
                        globalConjunction.nextDoc();
                    } else {
                        globalConjunction.advance(min);
                    }
                }

                int collected = 0;
                int doc = -1;

                // Score documents in the range [min, max)
                for (doc = globalConjunction.docID(); doc < max; doc = globalConjunction.nextDoc()) {
                    if (totalCollected >= size) {
                        return DocIdSetIterator.NO_MORE_DOCS; // Early termination
                    }

                    if (acceptDocs == null || acceptDocs.get(doc)) {
                        collector.collect(doc);
                        collected++;
                        totalCollected++;
                    }
                }

                // Check if conjunction exhausted
                if (globalConjunction.docID() == DocIdSetIterator.NO_MORE_DOCS) {

                    // If we need more hits, expand immediately
                    if (totalCollected < size) {
                        currentWindowSize *= 3;

                        try {
                            List<DocIdSetIterator> newIterators = rebuildIteratorsWithWindowSize(currentWindowSize);
                            globalConjunction = ConjunctionUtils.intersectIterators(newIterators);

                            int firstDoc = globalConjunction.nextDoc();
                            if (firstDoc != DocIdSetIterator.NO_MORE_DOCS) {
                                return firstDoc; // Return new starting point
                            }
                        } catch (IOException e) {}
                    }

                }

                return globalConjunction.docID();
            }

            @Override
            public long cost() {
                return ApproximateBooleanScorerSupplier.this.cost();
            }
        };

    }
}
