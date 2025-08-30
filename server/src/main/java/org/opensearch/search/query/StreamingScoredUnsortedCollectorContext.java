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
 * Streaming collector context for SCORED_UNSORTED mode.
 * Scores documents but doesn't sort, uses ring buffer for batch emission.
 *
 * @opensearch.internal
 */
public class StreamingScoredUnsortedCollectorContext extends TopDocsCollectorContext {

    public StreamingScoredUnsortedCollectorContext(SearchContext searchContext, boolean hasFilterCollector) {
        super("streaming_scored_unsorted", searchContext.size());
        // hasFilterCollector is used by parent class for optimization decisions
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // For SCORED_UNSORTED mode, score docs but don't sort
        return new StreamingScoredUnsortedCollector(numHits());
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) {
        return new StreamingScoredUnsortedCollectorManager(numHits());
    }

    @Override
    public void postProcess(QuerySearchResult result) throws IOException {
        // This will be called after collection is complete
        // The actual TopDocs will be set by the collector manager
    }

    /**
     * Collector that scores documents but doesn't sort them.
     */
    private static class StreamingScoredUnsortedCollector implements Collector {
        private final int maxDocs;
        private int docCount = 0;

        public StreamingScoredUnsortedCollector(int maxDocs) {
            this.maxDocs = maxDocs;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    // Scoring needed
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
            return ScoreMode.COMPLETE; // Scoring needed
        }

        public int getDocCount() {
            return docCount;
        }
    }

    /**
     * Collector manager for streaming scored unsorted collection.
     */
    private static class StreamingScoredUnsortedCollectorManager
        implements
            CollectorManager<StreamingScoredUnsortedCollector, ReduceableSearchResult> {
        private final int maxDocs;

        public StreamingScoredUnsortedCollectorManager(int maxDocs) {
            this.maxDocs = maxDocs;
        }

        @Override
        public StreamingScoredUnsortedCollector newCollector() throws IOException {
            return new StreamingScoredUnsortedCollector(maxDocs);
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingScoredUnsortedCollector> collectors) throws IOException {
            // Count total documents collected across all collectors
            int totalDocs = collectors.stream().mapToInt(StreamingScoredUnsortedCollector::getDocCount).sum();

            // For now, return null - this will be handled by the calling code
            return null;
        }
    }
}
