/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.search.internal.HybridQueryScorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Collects the TopDocs after executing hybrid query. Uses HybridQueryTopDocs as DTO to handle each sub query results
 */
public class HybridTopScoreDocCollector implements HybridSearchCollector {
    private static final Logger logger = LogManager.getLogger(HybridTopScoreDocCollector.class);

    private static final TopDocs EMPTY_TOPDOCS = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    private int docBase;
    private final HitsThresholdChecker hitsThresholdChecker;
    private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private int totalHits;
    private int[] collectedHitsPerSubQuery;
    private final int numOfHits;
    private PriorityQueue<ScoreDoc>[] compoundScores;
    private float maxScore = 0.0f;

    public HybridTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker) {
        numOfHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
        docBase = context.docBase;

        return new LeafCollector() {
            HybridQueryScorer compoundQueryScorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                if (scorer instanceof HybridQueryScorer) {
                    logger.debug("passed scorer is of type HybridQueryScorer, saving it for collecting documents and scores");
                    compoundQueryScorer = (HybridQueryScorer) scorer;
                } else {
                    compoundQueryScorer = getHybridQueryScorer(scorer);
                    if (Objects.isNull(compoundQueryScorer)) {
                        logger.error(
                            String.format(Locale.ROOT, "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s", scorer)
                        );
                    }
                }
            }

            private HybridQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
                if (scorer == null) {
                    return null;
                }
                if (scorer instanceof HybridQueryScorer) {
                    return (HybridQueryScorer) scorer;
                }
                for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
                    HybridQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child());
                    if (Objects.nonNull(hybridQueryScorer)) {
                        logger.debug(
                            String.format(
                                Locale.ROOT,
                                "found hybrid query scorer, it's child of scorer %s",
                                childScorable.child().getClass().getSimpleName()
                            )
                        );
                        return hybridQueryScorer;
                    }
                }
                return null;
            }

            @Override
            public void collect(int doc) throws IOException {
                if (Objects.isNull(compoundQueryScorer)) {
                    throw new IllegalArgumentException("scorers are null for all sub-queries in hybrid query");
                }
                float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                // iterate over results for each query
                if (compoundScores == null) {
                    compoundScores = new PriorityQueue[subScoresByQuery.length];
                    for (int i = 0; i < subScoresByQuery.length; i++) {
                        compoundScores[i] = new HitQueue(numOfHits, false);
                    }
                    collectedHitsPerSubQuery = new int[subScoresByQuery.length];
                }
                // Increment total hit count which represents unique doc found on the shard
                totalHits++;
                hitsThresholdChecker.incrementHitCount();
                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    // if score is 0.0 there is no hits for that sub-query
                    if (score == 0) {
                        continue;
                    }
                    if (hitsThresholdChecker.isThresholdReached() && totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                        logger.info(
                            "hit count threshold reached: total hits={}, threshold={}, action=updating_results",
                            totalHits,
                            hitsThresholdChecker.getTotalHitsThreshold()
                        );
                        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                    }
                    collectedHitsPerSubQuery[i]++;
                    PriorityQueue<ScoreDoc> pq = compoundScores[i];
                    ScoreDoc currentDoc = new ScoreDoc(doc + docBase, score);
                    maxScore = Math.max(currentDoc.score, maxScore);
                    // this way we're inserting into heap and do nothing else unless we reach the capacity
                    // after that we pull out the lowest score element on each insert
                    pq.insertWithOverflow(currentDoc);
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    /**
     * Get resulting collection of TopDocs for hybrid query after we ran search for each of its sub query
     * @return
     */
    public List<TopDocs> topDocs() {
        if (compoundScores == null) {
            return new ArrayList<>();
        }
        final List<TopDocs> topDocs = new ArrayList<>();
        for (int i = 0; i < compoundScores.length; i++) {
            topDocs.add(
                topDocsPerQuery(
                    0,
                    Math.min(collectedHitsPerSubQuery[i], compoundScores[i].size()),
                    compoundScores[i],
                    collectedHitsPerSubQuery[i]
                )
            );
        }
        return topDocs;
    }

    private TopDocs topDocsPerQuery(int start, int howMany, PriorityQueue<ScoreDoc> pq, int totalHits) {
        if (howMany < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Number of hits requested must be greater than 0 but value was %d", howMany)
            );
        }

        if (start < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Expected value of starting position is between 0 and %d, got %d", howMany, start)
            );
        }

        if (start >= howMany || howMany == 0) {
            return EMPTY_TOPDOCS;
        }

        int size = howMany - start;
        ScoreDoc[] results = new ScoreDoc[size];

        // Get the requested results from pq.
        populateResults(results, size, pq);

        return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }

    protected void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<ScoreDoc> pq) {
        for (int i = howMany - 1; i >= 0 && pq.size() > 0; i--) {
            // adding to array if index is within [0..array_length - 1]
            if (i < results.length) {
                results[i] = pq.pop();
            }
        }
    }

    @Override
    public int getTotalHits() {
        return totalHits;
    }

    @Override
    public float getMaxScore() {
        return maxScore;
    }

}
