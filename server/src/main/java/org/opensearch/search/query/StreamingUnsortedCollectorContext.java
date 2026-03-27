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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.FlushMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Streaming collector context for NO_SCORING mode.
 */
public class StreamingUnsortedCollectorContext extends TopDocsCollectorContext {

    private static final Logger logger = LogManager.getLogger(StreamingUnsortedCollectorContext.class);

    private final SearchContext searchContext;

    private StreamingUnsortedCollector activeCollector;

    public StreamingUnsortedCollectorContext(String profilerName, int numHits, SearchContext searchContext) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
    }

    @Override
    public Collector create(Collector in) throws IOException {
        this.activeCollector = new StreamingUnsortedCollector();
        return this.activeCollector;
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingUnsortedCollectorManager();
    }

    @Override
    public void postProcess(org.opensearch.search.query.QuerySearchResult result) throws IOException {
        if (activeCollector != null) {
            activeCollector.emitSegmentBatch(false);
        }
        if (result.hasTopDocs()) {
            return;
        }

        if (activeCollector != null) {
            List<ScoreDoc> firstK = activeCollector.getFirstKDocs();
            int totalHitsCount = activeCollector.getTotalHitsCount();

            ScoreDoc[] scoreDocs = firstK.toArray(new ScoreDoc[0]);
            TotalHits totalHits = new TotalHits(totalHitsCount, TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
            result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
            return;
        }

        ScoreDoc[] scoreDocs = new ScoreDoc[0];
        TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
        TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
        result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
    }

    /**
     * Collector manager for streaming unsorted collection
     */
    private class StreamingUnsortedCollectorManager implements CollectorManager<StreamingUnsortedCollector, ReduceableSearchResult> {

        @Override
        public StreamingUnsortedCollector newCollector() throws IOException {
            return new StreamingUnsortedCollector();
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingUnsortedCollector> collectors) throws IOException {
            List<ScoreDoc> mergedFirstK = new ArrayList<>();
            int totalHits = 0;

            for (StreamingUnsortedCollector collector : collectors) {
                mergedFirstK.addAll(collector.getFirstKDocs());
                totalHits += collector.getTotalHitsCount();
            }

            if (mergedFirstK.size() > numHits()) {
                mergedFirstK = mergedFirstK.subList(0, numHits());
            }

            ScoreDoc[] scoreDocs = mergedFirstK.toArray(new ScoreDoc[0]);
            TopDocs topDocs = new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);

            return result -> result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    /**
     * Collector that actually collects documents without scoring
     */
    private class StreamingUnsortedCollector implements Collector {

        private final List<ScoreDoc> currentSegmentBatch = new ArrayList<>();
        private final List<ScoreDoc> firstK = new ArrayList<>(numHits());
        private int totalHitsCount = 0;

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public LeafCollector getLeafCollector(org.apache.lucene.index.LeafReaderContext context) throws IOException {
            emitSegmentBatch(false);
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                    totalHitsCount++;
                    ScoreDoc scoreDoc = new ScoreDoc(doc + context.docBase, Float.NaN);

                    currentSegmentBatch.add(scoreDoc);

                    if (firstK.size() < numHits()) {
                        firstK.add(scoreDoc);
                    }
                }
            };
        }

        public List<ScoreDoc> getFirstKDocs() {
            return firstK;
        }

        public int getTotalHitsCount() {
            return totalHitsCount;
        }

        void emitSegmentBatch(boolean isFinal) {
            if (currentSegmentBatch.isEmpty()) {
                return;
            }

            try {
                if (searchContext == null
                    || searchContext.getFlushMode() != FlushMode.PER_SEGMENT
                    || searchContext.getStreamChannelListener() == null) {
                    return;
                }
                QuerySearchResult partial = new QuerySearchResult();
                TopDocs topDocs = new TopDocs(
                    new TotalHits(currentSegmentBatch.size(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                    currentSegmentBatch.toArray(new ScoreDoc[0])
                );
                partial.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
                searchContext.getStreamChannelListener().onStreamResponse(partial, isFinal);
            } catch (Exception e) {
                logger.trace("Failed to emit streaming batch", e);
            } finally {
                currentSegmentBatch.clear();
            }
        }
    }

}
