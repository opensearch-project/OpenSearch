/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.aggregation;

import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.profile.ProfilingWrapper;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that ProfilingAggregator and ProfilingLeafBucketCollector
 * correctly implement the ProfilingWrapper interface.
 */
public class ProfilingWrapperAggregationTests extends OpenSearchTestCase {

    public void testProfilingAggregatorImplementsProfilingWrapper() {
        Aggregator mockAggregator = mock(Aggregator.class);
        when(mockAggregator.name()).thenReturn("test_agg");
        AggregationProfiler profiler = new AggregationProfiler();

        ProfilingAggregator profilingAggregator = new ProfilingAggregator(mockAggregator, profiler);

        // Verify implements ProfilingWrapper
        assertTrue("ProfilingAggregator should implement ProfilingWrapper", profilingAggregator instanceof ProfilingWrapper);

        // Verify getDelegate returns the original aggregator
        assertSame("getDelegate() should return the original aggregator", mockAggregator, profilingAggregator.getDelegate());
    }

    public void testProfilingLeafBucketCollectorImplementsProfilingWrapper() {
        LeafBucketCollector mockCollector = mock(LeafBucketCollector.class);
        AggregationProfileBreakdown profileBreakdown = new AggregationProfileBreakdown();

        ProfilingLeafBucketCollector profilingCollector = new ProfilingLeafBucketCollector(mockCollector, profileBreakdown);

        // Verify implements ProfilingWrapper
        assertTrue("ProfilingLeafBucketCollector should implement ProfilingWrapper", profilingCollector instanceof ProfilingWrapper);

        // Verify getDelegate returns the original collector
        assertSame("getDelegate() should return the original collector", mockCollector, profilingCollector.getDelegate());
    }
}
