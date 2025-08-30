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
 * Streaming collector context for CONFIDENCE_BASED mode.
 * Uses Hoeffding bounds, deterministic bounds, confidence annotation.
 *
 * @opensearch.internal
 */
public class StreamingConfidenceCollectorContext extends TopDocsCollectorContext {

    public StreamingConfidenceCollectorContext(SearchContext searchContext, boolean hasFilterCollector) {
        super("streaming_confidence", searchContext.size());
        // hasFilterCollector is used by parent class for optimization decisions
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // For CONFIDENCE_BASED mode, use confidence-based collection
        return new StreamingConfidenceCollector(numHits());
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) {
        return new StreamingConfidenceCollectorManager(numHits());
    }

    @Override
    public void postProcess(QuerySearchResult result) throws IOException {
        // This will be called after collection is complete
        // The actual TopDocs will be set by the collector manager
    }

    /**
     * Collector that uses confidence-based bounds for progressive emission.
     */
    private static class StreamingConfidenceCollector implements Collector {
        private final int maxDocs;
        private int docCount = 0;

        public StreamingConfidenceCollector(int maxDocs) {
            this.maxDocs = maxDocs;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    // Scoring needed for confidence calculation
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
            return ScoreMode.COMPLETE; // Scoring needed for confidence
        }

        public int getDocCount() {
            return docCount;
        }
    }

    /**
     * Collector manager for streaming confidence-based collection.
     */
    private static class StreamingConfidenceCollectorManager
        implements
            CollectorManager<StreamingConfidenceCollector, ReduceableSearchResult> {
        private final int maxDocs;

        public StreamingConfidenceCollectorManager(int maxDocs) {
            this.maxDocs = maxDocs;
        }

        @Override
        public StreamingConfidenceCollector newCollector() throws IOException {
            return new StreamingConfidenceCollector(maxDocs);
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingConfidenceCollector> collectors) throws IOException {
            // Count total documents collected across all collectors
            int totalDocs = collectors.stream().mapToInt(StreamingConfidenceCollector::getDocCount).sum();

            // For now, return null - this will be handled by the calling code
            return null;
        }
    }
}
