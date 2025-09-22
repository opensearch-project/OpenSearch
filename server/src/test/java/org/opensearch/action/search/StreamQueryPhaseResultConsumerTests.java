/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.query.StreamingSearchMode;
import org.opensearch.search.streaming.StreamingSearchSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashSet;
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
        Settings settings = Settings.builder()
            .put(StreamingSearchSettings.STREAMING_NO_SCORING_BATCH_MULTIPLIER.getKey(), 1)
            .put(StreamingSearchSettings.STREAMING_SCORED_UNSORTED_BATCH_MULTIPLIER.getKey(), 2)
            .put(StreamingSearchSettings.STREAMING_CONFIDENCE_BATCH_MULTIPLIER.getKey(), 3)
            .put(StreamingSearchSettings.STREAMING_SCORED_SORTED_BATCH_MULTIPLIER.getKey(), 10)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(StreamingSearchSettings.getAllSettings()));

        // Test each mode
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
                exc -> {},
                clusterSettings
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
     * Test that non-default multipliers affect partial-reduce frequency
     */
    public void testScoredUnsortedMultiplierAffectsReductionFrequency() {
        // Setup: Configure ClusterSettings with non-default multiplier (5 instead of 2)
        Settings settings = Settings.builder().put(StreamingSearchSettings.STREAMING_SCORED_UNSORTED_BATCH_MULTIPLIER.getKey(), 5).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(StreamingSearchSettings.getAllSettings()));

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
            exc -> {},
            clusterSettings
        );

        // Verify getBatchReduceSize returns expected value (minBatchSize * 5)
        int batchSize = consumer.getBatchReduceSize(100, 10);
        assertEquals("Should use multiplier of 5", 50, batchSize);

        // Test with default multiplier
        Settings defaultSettings = Settings.builder()
            .put(StreamingSearchSettings.STREAMING_SCORED_UNSORTED_BATCH_MULTIPLIER.getKey(), 2)
            .build();
        ClusterSettings defaultClusterSettings = new ClusterSettings(
            defaultSettings,
            new HashSet<>(StreamingSearchSettings.getAllSettings())
        );

        StreamQueryPhaseResultConsumer defaultConsumer = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            10,
            exc -> {},
            defaultClusterSettings
        );

        int defaultBatchSize = defaultConsumer.getBatchReduceSize(100, 10);
        assertEquals("Should use default multiplier of 2", 20, defaultBatchSize);

        // Verify multiplier actually affects reduction frequency
        assertTrue("Higher multiplier should result in larger batch size", batchSize > defaultBatchSize);
    }

    /**
     * Test that null ClusterSettings throws appropriate exception
     */
    public void testNullClusterSettingsThrowsException() {
        SearchRequest request = new SearchRequest();
        request.setStreamingSearchMode(StreamingSearchMode.SCORED_UNSORTED.toString());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new StreamQueryPhaseResultConsumer(
                request,
                threadPool.executor(ThreadPool.Names.SEARCH),
                circuitBreaker,
                searchPhaseController,
                SearchProgressListener.NOOP,
                namedWriteableRegistry,
                10,
                exc -> {},
                null // Should throw IllegalArgumentException
            );
        });

        assertEquals("ClusterSettings must not be null for StreamQueryPhaseResultConsumer", exception.getMessage());
    }

    /**
     * Test dynamic settings update affects new consumers
     */
    public void testDynamicSettingsUpdateAffectsNewConsumers() {
        Settings initialSettings = Settings.builder()
            .put(StreamingSearchSettings.STREAMING_SCORED_UNSORTED_BATCH_MULTIPLIER.getKey(), 2)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(initialSettings, new HashSet<>(StreamingSearchSettings.getAllSettings()));

        SearchRequest request = new SearchRequest();
        request.setStreamingSearchMode(StreamingSearchMode.SCORED_UNSORTED.toString());

        // Create first consumer with initial settings
        StreamQueryPhaseResultConsumer consumer1 = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            10,
            exc -> {},
            clusterSettings
        );

        int batchSize1 = consumer1.getBatchReduceSize(100, 10);
        assertEquals("Should use initial multiplier of 2", 20, batchSize1);

        // Update the settings dynamically
        Settings updatedSettings = Settings.builder()
            .put(StreamingSearchSettings.STREAMING_SCORED_UNSORTED_BATCH_MULTIPLIER.getKey(), 5)
            .build();
        clusterSettings.applySettings(updatedSettings);

        // Create second consumer after settings update
        StreamQueryPhaseResultConsumer consumer2 = new StreamQueryPhaseResultConsumer(
            request,
            threadPool.executor(ThreadPool.Names.SEARCH),
            circuitBreaker,
            searchPhaseController,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            10,
            exc -> {},
            clusterSettings
        );

        int batchSize2 = consumer2.getBatchReduceSize(100, 10);
        assertEquals("Should use updated multiplier of 5", 50, batchSize2);

        // First consumer should still use original settings (settings are read once)
        int batchSize1Again = consumer1.getBatchReduceSize(100, 10);
        assertEquals("First consumer should still use original multiplier", 20, batchSize1Again);
    }
}
