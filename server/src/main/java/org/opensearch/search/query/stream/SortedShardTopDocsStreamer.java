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
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Streams sorted TopDocs from a shard.
 *
 * @opensearch.internal
 */
public class SortedShardTopDocsStreamer extends AbstractShardTopDocsStreamer {

    private final int numHits;
    private final Sort sort;
    private final boolean trackMaxScore;
    private TopFieldCollector collector;

    public SortedShardTopDocsStreamer(
        int numHits,
        Sort sort,
        boolean trackMaxScore,
        int batchDocThreshold,
        TimeValue timeInterval,
        boolean firstHitImmediate,
        boolean enableCoalescing
    ) {
        super(batchDocThreshold, timeInterval, firstHitImmediate, enableCoalescing);
        this.numHits = numHits;
        this.sort = sort;
        this.trackMaxScore = trackMaxScore;
    }

    @Override
    public void onStart(org.opensearch.search.internal.SearchContext context) {
        super.onStart(context);

        // Initialize the TopFieldCollector for this shard
        this.collector = new org.apache.lucene.search.TopFieldCollectorManager(sort, numHits, null, Integer.MAX_VALUE).newCollector();

        if (logger.isDebugEnabled()) {
            logger.debug("Initialized sorted streaming aggregator with numHits [{}] and sort [{}]", numHits, sort);
        }
    }

    @Override
    public LeafCollector newLeafCollector(LeafReaderContext leafContext, Scorable scorable) throws IOException {
        if (collector == null) {
            throw new IllegalStateException("Aggregator not started - call onStart() first");
        }

        // Get the leaf collector from our TopFieldCollector
        LeafCollector leafCollector = collector.getLeafCollector(leafContext);

        // Return a delegating collector that also calls our onDoc method
        return new StreamingLeafCollector(leafCollector, leafContext.docBase);
    }

    @Override
    protected void processDocument(int globalDocId, float score, Object[] sortValues) throws IOException {
        // The actual document processing is handled by the TopFieldCollector
        // through the delegating leaf collector
    }

    @Override
    protected TopDocs buildCurrentTopDocs() throws IOException {
        if (collector == null) {
            return null;
        }

        TopFieldDocs topFieldDocs = collector.topDocs();

        // Let coordinator set shardIndex during reduction
        return topFieldDocs;
    }

    @Override
    public TopDocsAndMaxScore buildFinalTopDocs() throws IOException {
        if (collector == null) {
            return new TopDocsAndMaxScore(new TopFieldDocs(null, new FieldDoc[0], sort.getSort()), Float.NaN);
        }

        TopFieldDocs topFieldDocs = (TopFieldDocs) buildCurrentTopDocs();
        // TODO: Get max score from collector when API is available
        float maxScore = Float.NaN;

        return new TopDocsAndMaxScore(topFieldDocs, maxScore);
    }

    @Override
    protected long estimateMemoryUsage() {
        if (collector == null) {
            return 0;
        }

        // Estimate based on collected docs and internal state
        // TopFieldCollector maintains a priority queue of FieldDoc objects
        int collectedDocs = getCollectedCount();
        int storedDocs = Math.min(collectedDocs, numHits);

        // Each FieldDoc: ~32 bytes base + (sortValues.length * 8) for references
        int sortFieldCount = sort.getSort().length;
        long fieldDocBytes = storedDocs * (32L + (sortFieldCount * 8L));

        // Sort values themselves (estimate 16 bytes per value)
        long sortValueBytes = storedDocs * sortFieldCount * 16L;

        // Priority queue overhead and other internal state
        long internalBytes = numHits * 8L + 1024L;

        return fieldDocBytes + sortValueBytes + internalBytes;
    }

    /**
     * Get the sort used by this aggregator.
     *
     * @return the sort configuration
     */
    public Sort getSort() {
        return sort;
    }

    /**
     * Delegating leaf collector that forwards to the TopFieldCollector
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

            // Calculate global doc ID
            int globalDocId = docBase + doc;

            // For sorted collection, we don't typically need the individual score/sort values
            // as the TopFieldCollector handles the sorting internally
            // The sorting values would be extracted by the collector itself

            // Trigger our aggregator's processing for emission logic
            SortedShardTopDocsStreamer.this.onDoc(globalDocId, Float.NaN, null);
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            return delegate.competitiveIterator();
        }
    }
}
