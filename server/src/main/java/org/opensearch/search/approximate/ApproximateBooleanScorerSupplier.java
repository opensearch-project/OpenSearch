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
        this.clauseWeights = new ArrayList<>();
        this.scoreMode = scoreMode;
        this.boost = boost;
        this.threshold = threshold;
        this.context = context;

        // Store weights that have valid scorer suppliers
        for (Weight clauseWeight : clauseWeights) {
            ScorerSupplier supplier = clauseWeight.scorerSupplier(context);
            if (supplier != null) {
                this.clauseWeights.add(clauseWeight);
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
        for (Weight weight : clauseWeights) {
            Query query = weight.getQuery();
            ScorerSupplier supplier = weight.scorerSupplier(context);

            if (query instanceof ApproximateQuery) {
                // Use ResumableDISI for approximatable queries
                ResumableDISI disi = new ResumableDISI(supplier);
                clauseIterators.add(disi);
            } else {
                // Use regular DocIdSetIterator for non-approximatable queries
                Scorer scorer = supplier.get(leadCost);
                clauseIterators.add(scorer.iterator());
            }
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
                return boost;
            }

            @Override
            public float getMaxScore(int upTo) throws IOException {
                return boost;
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
            if (!clauseWeights.isEmpty()) {
                cost = Long.MAX_VALUE;
                for (Weight weight : clauseWeights) {
                    try {
                        ScorerSupplier supplier = weight.scorerSupplier(context);
                        if (supplier != null) {
                            cost = Math.min(cost, supplier.cost());
                        }
                    } catch (IOException e) {
                        // If we can't get the cost, use a default
                        cost = Math.min(cost, 1000);
                    }
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

        // Create appropriate iterators for each clause - ResumableDISI only for approximatable queries
        List<DocIdSetIterator> clauseIterators = new ArrayList<>(clauseWeights.size());
        System.out.println("DEBUG: Creating iterators for " + clauseWeights.size() + " clauses");

        for (Weight weight : clauseWeights) {
            Query query = weight.getQuery();
            ScorerSupplier supplier = weight.scorerSupplier(context);
            System.out.println("DEBUG: Processing query: " + query.getClass().getSimpleName() + " - " + query);

            if (query instanceof ApproximateQuery) {
                // Use ResumableDISI for approximatable queries
                System.out.println("DEBUG: Using ResumableDISI for ApproximateQuery");
                ResumableDISI disi = new ResumableDISI(supplier);
                clauseIterators.add(disi);
            } else {
                // Use regular DocIdSetIterator for non-approximatable queries
                System.out.println("DEBUG: Using regular DISI for non-approximatable query");
                Scorer scorer = supplier.get(supplier.cost());
                DocIdSetIterator iterator = scorer.iterator();
                System.out.println("DEBUG: Regular iterator cost: " + iterator.cost());
                clauseIterators.add(iterator);
            }
        }

        // Use Lucene's ConjunctionUtils to create the conjunction ONCE (outside the BulkScorer)
        final DocIdSetIterator conjunctionDISI = ConjunctionUtils.intersectIterators(clauseIterators);
        // Create a simple scorer for the collector
        Scorer scorer = new Scorer() {
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

        // Create a simple bulk scorer that wraps the conjunction
        return new BulkScorer() {
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {

                collector.setScorer(scorer);

                // Position the iterator correctly (following Lucene's DefaultBulkScorer pattern)
                if (conjunctionDISI.docID() < min) {
                    if (conjunctionDISI.docID() == min - 1) {
                        conjunctionDISI.nextDoc();
                    }
                    else {
                        conjunctionDISI.advance(min);
                    }
                }
                int collected = 0;
                int doc = -1;

                // Score documents in the range [min, max) following Lucene's pattern
                // Note: No threshold limit here - that's handled by individual ResumableDISI clauses
                for (doc = conjunctionDISI.docID(); doc < max; doc = conjunctionDISI.nextDoc()) {
                    if (acceptDocs == null || acceptDocs.get(doc)) {
                        System.out.println("Conjunction Hit: "+doc);
                        collector.collect(doc);
                        collected++;
                    }
                }

                // Return the current iterator position (standard Lucene pattern)
                return conjunctionDISI.docID();
            }


            @Override
            public long cost() {
                return ApproximateBooleanScorerSupplier.this.cost();
            }
        };

    }
}




