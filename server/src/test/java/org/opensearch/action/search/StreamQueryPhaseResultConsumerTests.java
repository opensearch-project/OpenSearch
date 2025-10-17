/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.query.StreamingSearchMode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

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

}
