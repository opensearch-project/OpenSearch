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
     * Test that streaming consumer uses correct hard-coded multipliers
     */
    public void testStreamingConsumerBatchSizes() {
        SearchRequest request = new SearchRequest();
        request.setStreamingSearchMode(StreamingSearchMode.NO_SCORING.toString());

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
        assertEquals(1, batchSize);
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
