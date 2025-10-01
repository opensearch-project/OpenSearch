/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MultiCollector;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlushModeResolverTests extends OpenSearchTestCase {

    interface TestStreamableCollector extends Collector, Streamable {}

    public void testResolveWithNonStreamableCollector() {
        Collector nonStreamable = mock(Collector.class);

        FlushMode result = FlushModeResolver.resolve(nonStreamable, FlushMode.PER_SHARD, 10000, 0.01, 1000);

        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testResolveWithStreamableCollector() {
        TestStreamableCollector streamable = mock(TestStreamableCollector.class);
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 5000, 3, 50000);
        when(streamable.getStreamingCostMetrics()).thenReturn(metrics);

        FlushMode result = FlushModeResolver.resolve(streamable, FlushMode.PER_SHARD, 10000, 0.01, 1000);

        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeHighBucketCount() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 150000, 3, 1000000);

        TestStreamableCollector streamable = mock(TestStreamableCollector.class);
        when(streamable.getStreamingCostMetrics()).thenReturn(metrics);

        FlushMode result = FlushModeResolver.resolve(streamable, FlushMode.PER_SHARD, 100000, 0.01, 1000);

        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeLowBucketCount() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 500, 3, 50000);

        TestStreamableCollector streamable = mock(TestStreamableCollector.class);
        when(streamable.getStreamingCostMetrics()).thenReturn(metrics);

        FlushMode result = FlushModeResolver.resolve(streamable, FlushMode.PER_SHARD, 100000, 0.01, 1000);

        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeLowCardinalityRatio() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 5000, 3, 10000000);

        TestStreamableCollector streamable = mock(TestStreamableCollector.class);
        when(streamable.getStreamingCostMetrics()).thenReturn(metrics);

        FlushMode result = FlushModeResolver.resolve(streamable, FlushMode.PER_SHARD, 100000, 0.01, 1000);

        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeGoodForStreaming() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 5000, 3, 100000);

        TestStreamableCollector streamable = mock(TestStreamableCollector.class);
        when(streamable.getStreamingCostMetrics()).thenReturn(metrics);

        FlushMode result = FlushModeResolver.resolve(streamable, FlushMode.PER_SHARD, 100000, 0.01, 1000);

        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testResolveWithMultiCollector() {
        TestStreamableCollector streamable1 = mock(TestStreamableCollector.class);
        TestStreamableCollector streamable2 = mock(TestStreamableCollector.class);

        StreamingCostMetrics metrics1 = new StreamingCostMetrics(true, 50, 2000, 2, 20000);
        StreamingCostMetrics metrics2 = new StreamingCostMetrics(true, 75, 3000, 3, 30000);

        when(streamable1.getStreamingCostMetrics()).thenReturn(metrics1);
        when(streamable2.getStreamingCostMetrics()).thenReturn(metrics2);

        MultiCollector multiCollector = mock(MultiCollector.class);
        when(multiCollector.getCollectors()).thenReturn(new Collector[] { streamable1, streamable2 });

        FlushMode result = FlushModeResolver.resolve(multiCollector, FlushMode.PER_SHARD, 100000, 0.01, 1000);

        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testResolveWithNonStreamableMultiBucketCollector() {
        MultiBucketCollector multiBucket = mock(MultiBucketCollector.class);
        when(multiBucket.getCollectors()).thenReturn(new BucketCollector[0]);

        FlushMode result = FlushModeResolver.resolve(multiBucket, FlushMode.PER_SHARD, 100000, 0.01, 1000);

        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testSettingsDefaults() {
        assertEquals(100_000L, FlushModeResolver.STREAMING_MAX_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
        assertEquals(0.01, FlushModeResolver.STREAMING_MIN_CARDINALITY_RATIO.getDefault(Settings.EMPTY).doubleValue(), 0.001);
        assertEquals(1000L, FlushModeResolver.STREAMING_MIN_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
    }
}
