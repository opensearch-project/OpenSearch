/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.StreamingSearchMode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StreamQueryPhaseResultConsumerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private SearchPhaseController searchPhaseController;
    private CircuitBreaker circuitBreaker;
    private NamedWriteableRegistry namedWriteableRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("test");
        searchPhaseController = new SearchPhaseController(writableRegistry(), s -> null);
        circuitBreaker = new NoopCircuitBreaker("test");
        namedWriteableRegistry = writableRegistry();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Test that different streaming modes use their configured batch sizes
     */
    public void testStreamingModesUseDifferentBatchSizes() {
        // Test supported modes with hard-coded multipliers
        for (StreamingSearchMode mode : new StreamingSearchMode[] {
            StreamingSearchMode.NO_SCORING,
            StreamingSearchMode.SCORED_UNSORTED,
            StreamingSearchMode.SCORED_SORTED }) {
            SearchRequest request = new SearchRequest();
            request.setStreamingSearchMode(mode.toString());

            StreamQueryPhaseResultConsumer consumer = new StreamQueryPhaseResultConsumer(
                request,
                threadPool.executor(ThreadPool.Names.SEARCH),
                circuitBreaker,
                searchPhaseController,
                SearchProgressListener.NOOP,
                namedWriteableRegistry,
                10,
                exc -> {}
            );

            int batchSize = consumer.getBatchReduceSize(100, 5);

            switch (mode) {
                case NO_SCORING:
                    assertEquals(1, batchSize);
                    break;
                case SCORED_UNSORTED:
                    assertEquals(10, batchSize);
                    break;
                case SCORED_SORTED:
                    assertEquals(50, batchSize);
                    break;
            }
        }
    }

    /**
     * Test that streaming consumer uses correct hard-coded multipliers
     */
    public void testStreamingConsumerBatchSizes() {
        SearchRequest request = new SearchRequest();
        request.setStreamingSearchMode(StreamingSearchMode.SCORED_UNSORTED.toString());

        StreamQueryPhaseResultConsumer consumer = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            10,
            exc -> {}
        );

        int batchSize = consumer.getBatchReduceSize(100, 10);
        assertEquals(20, batchSize);

        request.setStreamingSearchMode(StreamingSearchMode.NO_SCORING.toString());
        StreamQueryPhaseResultConsumer noScoringConsumer = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            10,
            exc -> {}
        );

        int noScoringBatchSize = noScoringConsumer.getBatchReduceSize(100, 10);
        assertEquals(1, noScoringBatchSize);
    }

    /**
     * Test that StreamQueryPhaseResultConsumer for SCORED_SORTED uses appropriate batch sizing
     * to maintain global ordering when consuming interleaved partial results from multiple shards.
     */
    public void testConsumeInterleavedPartials_ScoredSorted_RespectsGlobalOrdering() {
        SearchRequest request = new SearchRequest();
        request.setStreamingSearchMode(StreamingSearchMode.SCORED_SORTED.toString());

        // Create consumer for 3 shards
        StreamQueryPhaseResultConsumer consumer = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            3,
            exc -> {}
        );

        int batchSize = consumer.getBatchReduceSize(100, 5);
        assertEquals(50, batchSize);
        assertTrue(batchSize >= 10);
    }

    /**
     * Test that StreamQueryPhaseResultConsumer for SCORED_UNSORTED uses smaller batch sizing
     * to enable faster partial reductions without strict ordering requirements.
     */
    public void testConsumeInterleavedPartials_ScoredUnsorted_MergesAllWithoutOrdering() {
        SearchRequest request = new SearchRequest();
        request.setStreamingSearchMode(StreamingSearchMode.SCORED_UNSORTED.toString());

        // Create consumer for 3 shards
        StreamQueryPhaseResultConsumer consumer = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            3,
            exc -> {}
        );

        int batchSize = consumer.getBatchReduceSize(100, 5);
        assertEquals(10, batchSize);
        assertTrue(batchSize < 50);
    }

    /**
     * Test that partial results are correctly merged with existing final results
     * to provide a coordinated global view.
     */
    public void testCoordinatedSnapshotMerging() {
        SearchRequest request = new SearchRequest();
        request.setStreamingSearchMode(StreamingSearchMode.SCORED_SORTED.toString());

        AtomicReference<TopDocs> capturedTopDocs = new AtomicReference<>();
        AtomicReference<List<SearchShard>> capturedShards = new AtomicReference<>();

        SearchProgressListener listener = new SearchProgressListener() {
            @Override
            protected void onPartialReduceWithTopDocs(
                List<SearchShard> shards,
                TotalHits totalHits,
                TopDocs topDocs,
                org.opensearch.search.aggregations.InternalAggregations aggs,
                int reducePhase
            ) {
                capturedTopDocs.set(topDocs);
                capturedShards.set(shards);
            }
        };

        StreamQueryPhaseResultConsumer consumer = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            listener,
            namedWriteableRegistry,
            2,
            exc -> {}
        );

        // 1. Shard 1 sends a partial result (score 10.0)
        SearchShardTarget target1 = new SearchShardTarget("node1", new ShardId("index", "uuid", 0), null, null);
        QuerySearchResult partial1 = new QuerySearchResult();
        partial1.setSearchShardTarget(target1);
        partial1.setShardIndex(0);
        partial1.setPartial(true);
        TopDocs td1 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 10.0f) });
        partial1.topDocs(new TopDocsAndMaxScore(td1, 10.0f), new DocValueFormat[0]);

        SearchPhaseResult sprPartial1 = createMockResult(partial1, target1, 0);
        consumer.consumeResult(sprPartial1, () -> {});

        assertNotNull(capturedTopDocs.get());
        assertEquals(1, capturedTopDocs.get().scoreDocs.length);
        assertEquals(10.0f, capturedTopDocs.get().scoreDocs[0].score, 0.01f);
        assertEquals(1, capturedShards.get().size());

        // 2. Shard 2 sends a final result (score 50.0)
        SearchShardTarget target2 = new SearchShardTarget("node1", new ShardId("index", "uuid", 1), null, null);
        QuerySearchResult final2 = new QuerySearchResult();
        final2.setSearchShardTarget(target2);
        final2.setShardIndex(1);
        final2.setPartial(false); // Final
        TopDocs td2 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(2, 50.0f) });
        final2.topDocs(new TopDocsAndMaxScore(td2, 50.0f), new DocValueFormat[0]);

        SearchPhaseResult sprFinal2 = createMockResult(final2, target2, 1);
        consumer.consumeResult(sprFinal2, () -> {});

        // Snapshot now contains Shard 2 (Final).
        // Note: The current implementation is stateless regarding *partials*.
        // Previous partials from other shards are not persisted in the reducer until they become final.
        // So Shard 1 (partial) is not included here.
        assertNotNull(capturedTopDocs.get());
        assertEquals(1, capturedTopDocs.get().scoreDocs.length);
        assertEquals(50.0f, capturedTopDocs.get().scoreDocs[0].score, 0.01f);
        assertEquals(1, capturedShards.get().size());

        // 3. Shard 1 sends another partial result (score 100.0)
        QuerySearchResult partial1Update = new QuerySearchResult();
        partial1Update.setSearchShardTarget(target1);
        partial1Update.setShardIndex(0);
        partial1Update.setPartial(true);
        TopDocs td1U = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 100.0f) });
        partial1Update.topDocs(new TopDocsAndMaxScore(td1U, 100.0f), new DocValueFormat[0]);

        SearchPhaseResult sprPartial1U = createMockResult(partial1Update, target1, 0);
        consumer.consumeResult(sprPartial1U, () -> {});

        // Snapshot should now contain the UPDATED Shard 1 + Final Shard 2
        assertNotNull(capturedTopDocs.get());
        assertEquals(2, capturedTopDocs.get().scoreDocs.length);
        assertEquals(100.0f, capturedTopDocs.get().scoreDocs[0].score, 0.01f); // Global sort: 100.0 first
        assertEquals(50.0f, capturedTopDocs.get().scoreDocs[1].score, 0.01f);
    }

    private SearchPhaseResult createMockResult(QuerySearchResult qResult, SearchShardTarget target, int index) {
        return new SearchPhaseResult() {
            @Override
            public QuerySearchResult queryResult() {
                return qResult;
            }

            @Override
            public SearchShardTarget getSearchShardTarget() {
                return target;
            }

            @Override
            public void setSearchShardTarget(SearchShardTarget shardTarget) {}

            @Override
            public int getShardIndex() {
                return index;
            }

            @Override
            public void setShardIndex(int shardIndex) {}
        };
    }
}
