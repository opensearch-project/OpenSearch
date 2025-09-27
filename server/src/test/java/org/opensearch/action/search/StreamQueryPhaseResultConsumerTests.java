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
        // Test each mode with hard-coded multipliers
        for (StreamingSearchMode mode : StreamingSearchMode.values()) {
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
                    assertEquals("NO_SCORING should always use batch size 1", 1, batchSize);
                    break;
                case SCORED_UNSORTED:
                    assertEquals("SCORED_UNSORTED should use 5 * 2", 10, batchSize);
                    break;
                case CONFIDENCE_BASED:
                    assertEquals("CONFIDENCE_BASED should use 5 * 3", 15, batchSize);
                    break;
                case SCORED_SORTED:
                    assertEquals("SCORED_SORTED should use 5 * 10", 50, batchSize);
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

        // Verify getBatchReduceSize returns expected value for SCORED_UNSORTED (minBatchSize * 2)
        int batchSize = consumer.getBatchReduceSize(100, 10);
        assertEquals("SCORED_UNSORTED should use hard-coded multiplier of 2", 20, batchSize);

        // Test NO_SCORING mode
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
        assertEquals("NO_SCORING should always use batch size 1", 1, noScoringBatchSize);
    }

}
