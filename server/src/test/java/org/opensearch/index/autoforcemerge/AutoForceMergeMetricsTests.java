/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.autoforcemerge;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoForceMergeMetricsTests extends OpenSearchTestCase {

    private MetricsRegistry metricsRegistry;
    private AutoForceMergeMetrics metrics;
    private Histogram mockHistogram;
    private Counter mockCounter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        metricsRegistry = mock(MetricsRegistry.class);
        mockHistogram = mock(Histogram.class);
        mockCounter = mock(Counter.class);

        when(metricsRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mockHistogram);
        when(metricsRegistry.createCounter(anyString(), anyString(), anyString())).thenReturn(mockCounter);

        metrics = new AutoForceMergeMetrics(metricsRegistry);
    }

    public void testMetricsInitialization() {
        // Verify histogram creation
        verify(metricsRegistry).createHistogram(
            eq("auto_force_merge.scheduler.execution_time"),
            eq("Histogram for tracking total scheduler execution time."),
            eq("ms")
        );
        verify(metricsRegistry).createHistogram(
            eq("auto_force_merge.shard.merge_latency"),
            eq("Histogram for tracking time taken by force merge on individual shards."),
            eq("ms")
        );

        // Verify counter creation
        verify(metricsRegistry).createCounter(
            eq("auto_force_merge.merges.triggered"),
            eq("Counter for number of force merges triggered."),
            eq("1")
        );
        verify(metricsRegistry).createCounter(
            eq("auto_force_merge.merges.skipped.config_validator"),
            eq("Counter for number of force merges skipped due to Configuration Validator."),
            eq("1")
        );
        verify(metricsRegistry).createCounter(
            eq("auto_force_merge.merges.skipped.node_validator"),
            eq("Counter for number of force merges skipped due to Node Validator."),
            eq("1")
        );
        verify(metricsRegistry).createCounter(
            eq("auto_force_merge.merges.failed"),
            eq("Counter for number of force merges failed."),
            eq("1")
        );
        verify(metricsRegistry).createCounter(
            eq("auto_force_merge.shard.size"),
            eq("Counter for tracking shard size during force merge operations."),
            eq("bytes")
        );
        verify(metricsRegistry).createCounter(
            eq("auto_force_merge.shard.segment_count"),
            eq("Counter for tracking segment count during force merge operations."),
            eq("1")
        );
    }

    public void testRecordInHistogramWithNullTags() {
        Double value = 100.0;
        metrics.recordInHistogram(mockHistogram, value, null);
        verify(mockHistogram, times(1)).record(value);
    }

    public void testRecordInHistogramWithEmptyTags() {
        Double value = 100.0;
        metrics.recordInHistogram(mockHistogram, value, Optional.empty());
        verify(mockHistogram, times(1)).record(value);
    }

    public void testRecordInHistogramWithTags() {
        Double value = 100.0;
        Tags tags = Tags.create();
        Optional<Tags> tagsOptional = Optional.of(tags);

        metrics.recordInHistogram(mockHistogram, value, tagsOptional);
        verify(mockHistogram, times(1)).record(eq(value), eq(tags));
    }

    public void testIncrementCounterWithNullTags() {
        Double value = 1.0;
        metrics.incrementCounter(mockCounter, value, null);
        verify(mockCounter, times(1)).add(value);
    }

    public void testIncrementCounterWithEmptyTags() {
        Double value = 1.0;
        metrics.incrementCounter(mockCounter, value, Optional.empty());
        verify(mockCounter, times(1)).add(value);
    }

    public void testIncrementCounterWithTags() {
        Double value = 1.0;
        Tags tags = Tags.create();
        Optional<Tags> tagsOptional = Optional.of(tags);

        metrics.incrementCounter(mockCounter, value, tagsOptional);
        verify(mockCounter, times(1)).add(eq(value), eq(tags));
    }

    public void testGetTagsWithOnlyNodeId() {
        String nodeId = "node1";
        Optional<Tags> tagsOptional = metrics.getTags(Optional.of(nodeId), Optional.empty());

        assertTrue("Tags should be present", tagsOptional.isPresent());
        assertNotNull("Tags should not be null", tagsOptional.get());
    }

    public void testGetTagsWithOnlyShardId() {
        String shardId = "shard1";
        Optional<Tags> tagsOptional = metrics.getTags(Optional.empty(), Optional.of(shardId));

        assertTrue("Tags should be present", tagsOptional.isPresent());
        assertNotNull("Tags should not be null", tagsOptional.get());
    }

    public void testGetTagsWithBothIds() {
        String nodeId = "node1";
        String shardId = "shard1";
        Optional<Tags> tagsOptional = metrics.getTags(Optional.of(nodeId), Optional.of(shardId));

        assertTrue("Tags should be present", tagsOptional.isPresent());
        assertNotNull("Tags should not be null", tagsOptional.get());
    }

    public void testGetTagsWithEmptyOptionals() {
        Optional<Tags> tagsOptional = metrics.getTags(Optional.empty(), Optional.empty());

        assertTrue("Tags should be present", tagsOptional.isPresent());
        assertNotNull("Tags should not be null", tagsOptional.get());
    }

    public void testConstantValues() {
        assertEquals("node_id", AutoForceMergeMetrics.NODE_ID);
        assertEquals("shard_id", AutoForceMergeMetrics.SHARD_ID);
    }
}
