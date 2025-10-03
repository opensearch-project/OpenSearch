/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.opensearch.common.CheckedFunction;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.FlushMode;
import org.opensearch.search.streaming.Streamable;
import org.opensearch.search.streaming.StreamingCostMetrics;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AggregatorTreeEvaluatorTests extends OpenSearchTestCase {

    interface TestStreamableCollector extends Collector, Streamable {}

    public void testNonStreamSearch() throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.isStreamSearch()).thenReturn(false);

        Collector collector = mock(Collector.class);
        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider = mock(CheckedFunction.class);

        Collector result = AggregatorTreeEvaluator.evaluateAndRecreateIfNeeded(collector, searchContext, aggProvider);

        assertSame(collector, result);
        verify(aggProvider, never()).apply(any());
    }

    public void testStreamSearchWithCachedFlushMode() throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.isStreamSearch()).thenReturn(true);
        when(searchContext.getFlushMode()).thenReturn(FlushMode.PER_SEGMENT);

        Collector collector = mock(Collector.class);
        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider = mock(CheckedFunction.class);

        Collector result = AggregatorTreeEvaluator.evaluateAndRecreateIfNeeded(collector, searchContext, aggProvider);

        assertSame(collector, result);
        verify(aggProvider, never()).apply(any());
    }

    public void testStreamSearchDecidesToStream() throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.isStreamSearch()).thenReturn(true);
        when(searchContext.getFlushMode()).thenReturn(null);
        when(searchContext.getStreamingMaxEstimatedBucketCount()).thenReturn(100000L);
        when(searchContext.getStreamingMinCardinalityRatio()).thenReturn(0.01);
        when(searchContext.getStreamingMinEstimatedBucketCount()).thenReturn(1000L);
        when(searchContext.setFlushModeIfAbsent(FlushMode.PER_SEGMENT)).thenReturn(true);

        TestStreamableCollector streamableCollector = mock(TestStreamableCollector.class);
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 5000, 3, 100000);
        when(streamableCollector.getStreamingCostMetrics()).thenReturn(metrics);

        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider = mock(CheckedFunction.class);

        Collector result = AggregatorTreeEvaluator.evaluateAndRecreateIfNeeded(streamableCollector, searchContext, aggProvider);

        assertSame(streamableCollector, result);
        verify(aggProvider, never()).apply(any());
    }

    public void testStreamSearchDecidesToUseTraditional() throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.isStreamSearch()).thenReturn(true);
        when(searchContext.getFlushMode()).thenReturn(null);
        when(searchContext.getStreamingMaxEstimatedBucketCount()).thenReturn(100000L);
        when(searchContext.getStreamingMinCardinalityRatio()).thenReturn(0.01);
        when(searchContext.getStreamingMinEstimatedBucketCount()).thenReturn(1000L);
        when(searchContext.setFlushModeIfAbsent(FlushMode.PER_SHARD)).thenReturn(true);

        TestStreamableCollector streamableCollector = mock(TestStreamableCollector.class);
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 500, 3, 100000);
        when(streamableCollector.getStreamingCostMetrics()).thenReturn(metrics);

        Aggregator aggregator = mock(Aggregator.class);
        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider = mock(CheckedFunction.class);
        when(aggProvider.apply(searchContext)).thenReturn(Collections.singletonList(aggregator));

        Collector result = AggregatorTreeEvaluator.evaluateAndRecreateIfNeeded(streamableCollector, searchContext, aggProvider);

        assertNotSame(streamableCollector, result);
        verify(aggProvider).apply(searchContext);
    }

    public void testConcurrentFlushModeSet() throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.isStreamSearch()).thenReturn(true);
        when(searchContext.getFlushMode()).thenReturn(null);
        when(searchContext.getStreamingMaxEstimatedBucketCount()).thenReturn(100000L);
        when(searchContext.getStreamingMinCardinalityRatio()).thenReturn(0.01);
        when(searchContext.getStreamingMinEstimatedBucketCount()).thenReturn(1000L);
        when(searchContext.setFlushModeIfAbsent(FlushMode.PER_SEGMENT)).thenReturn(false);
        when(searchContext.getFlushMode()).thenReturn(FlushMode.PER_SHARD);

        TestStreamableCollector streamableCollector = mock(TestStreamableCollector.class);
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 5000, 3, 100000);
        when(streamableCollector.getStreamingCostMetrics()).thenReturn(metrics);

        Aggregator aggregator = mock(Aggregator.class);
        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider = mock(CheckedFunction.class);
        when(aggProvider.apply(searchContext)).thenReturn(Collections.singletonList(aggregator));

        Collector result = AggregatorTreeEvaluator.evaluateAndRecreateIfNeeded(streamableCollector, searchContext, aggProvider);

        assertNotSame(streamableCollector, result);
        verify(aggProvider).apply(searchContext);
    }
}
