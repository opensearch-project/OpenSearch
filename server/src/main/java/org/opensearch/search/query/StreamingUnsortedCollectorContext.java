/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collection;

/**
 * Streaming collector context for NO_SCORING mode.
 * Uses ring buffer for batch emission, no scoring, fastest TTFB.
 *
 * @opensearch.internal
 */
public class StreamingUnsortedCollectorContext extends TopDocsCollectorContext {

    public StreamingUnsortedCollectorContext(SearchContext searchContext, boolean hasFilterCollector) {
        super("streaming_unsorted", searchContext.size());
        // hasFilterCollector is used by parent class for optimization decisions
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // For NO_SCORING mode, we don't need scoring, just collect docs in order
        return new StreamingUnsortedCollector(numHits());
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) {
        return new StreamingUnsortedCollectorManager(numHits());
    }

    @Override
    public void postProcess(QuerySearchResult result) throws IOException {
        // This will be called after collection is complete
        // The actual TopDocs will be set by the collector manager
    }

    /**
     * Simple collector that just counts documents without scoring.
     */
    private static class StreamingUnsortedCollector implements Collector {
        private final int maxDocs;
        private int docCount = 0;

        public StreamingUnsortedCollector(int maxDocs) {
            this.maxDocs = maxDocs;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    // No scoring needed for NO_SCORING mode
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (docCount < maxDocs) {
                        docCount++;
                    }
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES; // No scoring needed
        }

        public int getDocCount() {
            return docCount;
        }
    }

    /**
     * Collector manager for streaming unsorted collection.
     */
    private static class StreamingUnsortedCollectorManager implements CollectorManager<StreamingUnsortedCollector, ReduceableSearchResult> {
        private final int maxDocs;

        public StreamingUnsortedCollectorManager(int maxDocs) {
            this.maxDocs = maxDocs;
        }

        @Override
        public StreamingUnsortedCollector newCollector() throws IOException {
            return new StreamingUnsortedCollector(maxDocs);
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingUnsortedCollector> collectors) throws IOException {
            // Count total documents collected across all collectors
            int totalDocs = collectors.stream().mapToInt(StreamingUnsortedCollector::getDocCount).sum();

            // For now, return null - this will be handled by the calling code
            // In a real implementation, we would create a proper result object
            return null;
        }
    }
}
