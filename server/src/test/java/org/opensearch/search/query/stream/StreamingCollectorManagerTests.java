/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Streaming collector manager tests
public class StreamingCollectorManagerTests extends OpenSearchTestCase {

    private SearchContext searchContext;
    private StreamSearchChannelListener channelListener;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // Mock search context
        searchContext = mock(SearchContext.class);
        channelListener = mock(StreamSearchChannelListener.class);

        when(searchContext.getStreamChannelListener()).thenReturn(channelListener);
        when(searchContext.getStreamingBatchSize()).thenReturn(10);
        when(searchContext.getStreamingTimeInterval()).thenReturn(TimeValue.timeValueMillis(100));
        when(searchContext.getStreamingFirstHitImmediate()).thenReturn(true);
        when(searchContext.getStreamingEnableCoalescing()).thenReturn(true);
    }

    @After
    public void tearDown() throws Exception {
        // Clean up the fallback scheduler to prevent thread leaks
        AbstractShardTopDocsStreamer.shutdownFallbackScheduler();
        super.tearDown();
    }

    public void testScoreManager() throws IOException {
        StreamingScoreCollectorManager manager = new StreamingScoreCollectorManager(
            searchContext,
            10, // numHits
            false, // trackMaxScore
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        // Test collector creation
        Collector collector = manager.newCollector();
        assertNotNull(collector);

        // Test that second collector creation fails (single collector enforcement)
        expectThrows(IllegalStateException.class, () -> { manager.newCollector(); });

        // Test reduction with single collector
        ReduceableSearchResult result = manager.reduce(Collections.singletonList(collector));
        assertNotNull(result);

        // Test that result can reduce a QuerySearchResult
        QuerySearchResult queryResult = new QuerySearchResult();
        result.reduce(queryResult);
        assertNotNull(queryResult.topDocs());
    }

    public void testSortManager() throws IOException {
        Sort sort = new Sort(org.apache.lucene.search.SortField.FIELD_SCORE);

        StreamingSortCollectorManager manager = new StreamingSortCollectorManager(
            searchContext,
            10, // numHits
            sort, // sort
            true, // trackMaxScore
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        // Test collector creation
        Collector collector = manager.newCollector();
        assertNotNull(collector);

        // Test sort configuration
        assertEquals(sort, manager.getSort());

        // Test that second collector creation fails (single collector enforcement)
        expectThrows(IllegalStateException.class, () -> { manager.newCollector(); });

        // Test reduction with single collector
        ReduceableSearchResult result = manager.reduce(Collections.singletonList(collector));
        assertNotNull(result);

        // Test that result can reduce a QuerySearchResult
        QuerySearchResult queryResult = new QuerySearchResult();
        result.reduce(queryResult);
        assertNotNull(queryResult.topDocs());
    }

    public void testReductionErrors() throws IOException {
        StreamingScoreCollectorManager manager = new StreamingScoreCollectorManager(
            searchContext,
            10, // numHits
            false, // trackMaxScore
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        Collector collector1 = manager.newCollector();

        // Create second manager for second collector to test multi-collector error
        StreamingScoreCollectorManager manager2 = new StreamingScoreCollectorManager(
            searchContext,
            10,
            false,
            5,
            TimeValue.timeValueMillis(100),
            true,
            true
        );
        Collector collector2 = manager2.newCollector();

        // Test that reduction with multiple collectors fails
        expectThrows(IllegalStateException.class, () -> { manager.reduce(java.util.Arrays.asList(collector1, collector2)); });
    }

    public void testAggregator() throws IOException {
        StreamingScoreCollectorManager manager = new StreamingScoreCollectorManager(
            searchContext,
            10,
            false,
            5,
            TimeValue.timeValueMillis(100),
            true,
            true
        );

        // Test aggregator access
        UnsortedShardTopDocsStreamer aggregator = manager.getAggregator();
        assertNotNull(aggregator);
        assertFalse(aggregator.isCancelled());
        assertEquals(0, aggregator.getCollectedCount());
        assertEquals(0, aggregator.getEmissionCount());
    }

    public void testScoreMode() throws IOException {
        // Test score mode for unsorted search
        StreamingScoreCollectorManager scoreManager = new StreamingScoreCollectorManager(
            searchContext,
            10,
            true,
            5,
            TimeValue.timeValueMillis(100),
            true,
            true
        );

        Collector scoreCollector = scoreManager.newCollector();
        assertEquals(org.apache.lucene.search.ScoreMode.COMPLETE, scoreCollector.scoreMode());

        // Test score mode for sorted search with score tracking
        Sort scoreSort = new Sort(org.apache.lucene.search.SortField.FIELD_SCORE);
        StreamingSortCollectorManager sortManager = new StreamingSortCollectorManager(
            searchContext,
            10,
            scoreSort,
            true,
            5,
            TimeValue.timeValueMillis(100),
            true,
            true
        );

        Collector sortCollector = sortManager.newCollector();
        assertEquals(org.apache.lucene.search.ScoreMode.COMPLETE, sortCollector.scoreMode());

        // Test score mode for sorted search without score tracking
        Sort fieldSort = new Sort(new org.apache.lucene.search.SortField("field", org.apache.lucene.search.SortField.Type.STRING));
        StreamingSortCollectorManager fieldSortManager = new StreamingSortCollectorManager(
            searchContext,
            10,
            fieldSort,
            false,
            5,
            TimeValue.timeValueMillis(100),
            true,
            true
        );

        Collector fieldCollector = fieldSortManager.newCollector();
        assertEquals(org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, fieldCollector.scoreMode());
    }
}
