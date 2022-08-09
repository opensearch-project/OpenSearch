/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;

import java.io.IOException;
import java.util.Collection;

/**
 * CollectorManager for the TotalHitCountCollector
 *
 * @opensearch.internal
 */
public class TotalHitCountCollectorManager
    implements
        CollectorManager<TotalHitCountCollector, ReduceableSearchResult>,
        EarlyTerminatingListener {

    private static final TotalHitCountCollector EMPTY_COLLECTOR = new TotalHitCountCollector() {
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {}
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    };

    private final Sort sort;
    private Integer terminatedAfter;

    public TotalHitCountCollectorManager(final Sort sort) {
        this.sort = sort;
    }

    @Override
    public void onEarlyTermination(int maxCountHits, boolean forcedTermination) {
        terminatedAfter = maxCountHits;
    }

    @Override
    public TotalHitCountCollector newCollector() throws IOException {
        return new TotalHitCountCollector();
    }

    @Override
    public ReduceableSearchResult reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
        return (QuerySearchResult result) -> {
            final TotalHits.Relation relation = (terminatedAfter != null)
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO;

            int totalHits = collectors.stream().mapToInt(TotalHitCountCollector::getTotalHits).sum();
            if (terminatedAfter != null && totalHits > terminatedAfter) {
                totalHits = terminatedAfter;
            }

            final TotalHits totalHitCount = new TotalHits(totalHits, relation);
            final TopDocs topDocs = (sort != null)
                ? new TopFieldDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS, sort.getSort())
                : new TopDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS);

            result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), null);
        };
    }

    static class Empty implements CollectorManager<TotalHitCountCollector, ReduceableSearchResult> {
        private final TotalHits totalHits;
        private final Sort sort;

        Empty(final TotalHits totalHits, final Sort sort) {
            this.totalHits = totalHits;
            this.sort = sort;
        }

        @Override
        public TotalHitCountCollector newCollector() throws IOException {
            return EMPTY_COLLECTOR;
        }

        @Override
        public ReduceableSearchResult reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
            return (QuerySearchResult result) -> {
                final TopDocs topDocs;

                if (sort != null) {
                    topDocs = new TopFieldDocs(totalHits, Lucene.EMPTY_SCORE_DOCS, sort.getSort());
                } else {
                    topDocs = new TopDocs(totalHits, Lucene.EMPTY_SCORE_DOCS);
                }

                result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), null);
            };
        }
    }
}
