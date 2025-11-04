/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collection;

/**
 * Minimal no-hits streaming collector context for size=0 aggregation-only requests.
 * Preserves streaming aggregation path without collecting hits or setting topDocs.
 * 
 * @opensearch.internal
 */
public class StreamingAggregationOnlyCollectorContext extends TopDocsCollectorContext {

    private final SearchContext searchContext;

    public StreamingAggregationOnlyCollectorContext(SearchContext searchContext) {
        super("streaming_aggregation_only", 0);
        this.searchContext = searchContext;
    }

    @Override
    Collector create(Collector in) {
        // Return a no-op collector
        return new NoOpCollector(in);
    }

    @Override
    CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new NoOpCollectorManager(in);
    }

    @Override
    void postProcess(QuerySearchResult result) throws IOException {
        // Ensure topDocs is present so serialization never NPEs
        if (!result.hasTopDocs()) {
            ScoreDoc[] scoreDocs = new ScoreDoc[0];
            TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
            result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    /**
     * No-op collector that doesn't collect documents for hits
     */
    private static class NoOpCollector implements Collector {
        private final Collector in;

        NoOpCollector(Collector in) {
            this.in = in;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            // Get the inner collector's leaf collector (for aggregations)
            final LeafCollector inLeafCollector = (in != null) ? in.getLeafCollector(context) : null;
            
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    if (inLeafCollector != null) {
                        inLeafCollector.setScorer(scorer);
                    }
                }

                @Override
                public void collect(int doc) throws IOException {
                    // Only collect for the inner collector (aggregations), not for hits
                    if (inLeafCollector != null) {
                        inLeafCollector.collect(doc);
                    }
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            // Use COMPLETE_NO_SCORES as we're not collecting hits
            return (in != null) ? in.scoreMode() : ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    /**
     * No-op collector manager for concurrent search
     */
    private static class NoOpCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
        private final CollectorManager<?, ReduceableSearchResult> in;

        NoOpCollectorManager(CollectorManager<?, ReduceableSearchResult> in) {
            this.in = in;
        }

        @Override
        public Collector newCollector() throws IOException {
            Collector innerCollector = (in != null) ? in.newCollector() : null;
            return new NoOpCollector(innerCollector);
        }

        @Override
        @SuppressWarnings("unchecked")
        public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
            // Return a ReduceableSearchResult that does not modify topDocs
            if (in != null) {
                return ((CollectorManager<Collector, ReduceableSearchResult>) in).reduce(collectors);
            } else {
                return (QuerySearchResult result) -> {
                    // No-op - don't modify topDocs
                };
            }
        }
    }
}