/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Streams unsorted TopDocs from a shard.
 *
 * @opensearch.internal
 */
public class UnsortedShardTopDocsStreamer extends AbstractShardTopDocsStreamer {

    private final int numHits;
    private final boolean trackMaxScore;
    private TopScoreDocCollector collector;

    public UnsortedShardTopDocsStreamer(
        int numHits,
        boolean trackMaxScore,
        int batchDocThreshold,
        TimeValue timeInterval,
        boolean firstHitImmediate,
        boolean enableCoalescing
    ) {
        super(batchDocThreshold, timeInterval, firstHitImmediate, enableCoalescing);
        this.numHits = numHits;
        this.trackMaxScore = trackMaxScore;
    }

    @Override
    public void onStart(org.opensearch.search.internal.SearchContext context) {
        super.onStart(context);

        // Initialize the TopScoreDocCollector for this shard
        this.collector = new org.apache.lucene.search.TopScoreDocCollectorManager(numHits, null, Integer.MAX_VALUE).newCollector();

        if (logger.isDebugEnabled()) {
            logger.debug("Initialized unsorted streaming aggregator with numHits [{}]", numHits);
        }
    }

    @Override
    public LeafCollector newLeafCollector(LeafReaderContext leafContext, Scorable scorable) throws IOException {
        if (collector == null) {
            throw new IllegalStateException("Aggregator not started - call onStart() first");
        }

        // Get the leaf collector from our TopScoreDocCollector
        LeafCollector leafCollector = collector.getLeafCollector(leafContext);

        // Return a delegating collector that also calls our onDoc method
        return new StreamingLeafCollector(leafCollector, leafContext.docBase);
    }

    @Override
    protected void processDocument(int globalDocId, float score, Object[] sortValues) throws IOException {
        // The actual document processing is handled by the TopScoreDocCollector
        // through the delegating leaf collector
    }

    @Override
    protected TopDocs buildCurrentTopDocs() throws IOException {
        if (collector == null) {
            return null;
        }

        TopDocs topDocs = collector.topDocs();

        // Let coordinator set shardIndex during reduction
        return topDocs;
    }

    @Override
    public TopDocsAndMaxScore buildFinalTopDocs() throws IOException {
        if (collector == null) {
            return new TopDocsAndMaxScore(new TopDocs(null, new org.apache.lucene.search.ScoreDoc[0]), Float.NaN);
        }

        TopDocs topDocs = buildCurrentTopDocs();
        // TODO: Get max score from collector when API is available
        float maxScore = Float.NaN;

        return new TopDocsAndMaxScore(topDocs, maxScore);
    }

    @Override
    protected long estimateMemoryUsage() {
        if (collector == null) {
            return 0;
        }

        // Estimate based on collected docs and internal state
        // TopScoreDocCollector maintains a priority queue of ScoreDoc objects
        int collectedDocs = getCollectedCount();
        int storedDocs = Math.min(collectedDocs, numHits);

        // Each ScoreDoc: ~24 bytes (int doc + float score + int shardIndex + object overhead)
        long scoreDocsBytes = storedDocs * 24L;

        // Priority queue overhead and other internal state
        long internalBytes = numHits * 8L + 1024L;

        return scoreDocsBytes + internalBytes;
    }

    /**
     * Delegating leaf collector that forwards to the TopScoreDocCollector
     * and also triggers our aggregator's processing logic.
     */
    private class StreamingLeafCollector implements LeafCollector {
        private final LeafCollector delegate;
        private final int docBase;

        StreamingLeafCollector(LeafCollector delegate, int docBase) {
            this.delegate = delegate;
            this.docBase = docBase;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            delegate.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            // Forward to the actual collector first
            delegate.collect(doc);

            // Calculate global doc ID and get score
            int globalDocId = docBase + doc;
            float score = Float.NaN;

            try {
                // Try to get the score if available
                // TODO: Access score when collector API provides it
            } catch (Exception e) {
                // Score not available or error occurred
            }

            // Trigger our aggregator's processing
            UnsortedShardTopDocsStreamer.this.onDoc(globalDocId, score, null);
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            return delegate.competitiveIterator();
        }
    }
}
