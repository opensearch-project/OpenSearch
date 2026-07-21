/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Property-based tests for SearchLatencyBreakdown: Property 1 - Additive recording preserves sum.
 * <p>
 * For any sequence of non-negative nanosecond values recorded via addAndGet on a single metric,
 * the final stored value SHALL equal the sum of all recorded values, and the unified breakdown map
 * SHALL contain that sum converted to milliseconds (nanos / 1,000,000) if the millis value is > 0.
 * <p>
 * Feature: search-latency-breakdown, Property 1: Additive recording preserves sum
 * <p>
 * <b>Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6, 4.1, 8.3, 8.4, 8.5, 8.6, 10.1, 10.2, 10.3, 10.4</b>
 */
public class SearchLatencyBreakdownProperty1Tests extends OpenSearchTestCase {

    private static final int MIN_ITERATIONS = 100;

    /**
     * Helper: each entry pairs a recording method with the expected key in the unified breakdown map.
     */
    private static class MetricUnderTest {
        final String mapKey;
        final Consumer<SearchLatencyBreakdown> recorder;

        MetricUnderTest(String mapKey, Consumer<SearchLatencyBreakdown> recorder) {
            this.mapKey = mapKey;
            this.recorder = recorder;
        }
    }

    /**
     * All addAndGet-based recording methods that represent coordinator-level sequential operations.
     * These are the 14 methods specified in the task context.
     */
    private MetricUnderTest[] getAdditiveMetrics() {
        return new MetricUnderTest[] {
            new MetricUnderTest("pipeline_request_transform", b -> {}),
            new MetricUnderTest("query_rewrite", b -> {}),
            new MetricUnderTest("index_resolution", b -> {}),
            new MetricUnderTest("shard_routing", b -> {}),
            new MetricUnderTest("terms_lookup_sub_query", b -> {}),
            new MetricUnderTest("coordinator_queue_wait", b -> {}),
            new MetricUnderTest("reduce_top_docs", b -> {}),
            new MetricUnderTest("reduce_aggregations", b -> {}),
            new MetricUnderTest("reduce_pipeline_aggs", b -> {}),
            new MetricUnderTest("reduce_suggestions", b -> {}),
            new MetricUnderTest("request_serialization", b -> {}),
            new MetricUnderTest("response_deserialization", b -> {}),
            new MetricUnderTest("inbound_network_time", b -> {}),
            new MetricUnderTest("outbound_network_time", b -> {})
        };
    }

    /**
     * Property 1: Additive recording preserves sum for recordPipelineRequestTransform.
     * <p>
     * Validates: Requirements 1.1
     */
    public void testAdditiveRecordingPipelineRequestTransform() {
        verifyAdditiveProperty(
            "pipeline_request_transform",
            (breakdown, nanos) -> breakdown.recordPipelineRequestTransform(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordQueryRewrite.
     * <p>
     * Validates: Requirements 1.2
     */
    public void testAdditiveRecordingQueryRewrite() {
        verifyAdditiveProperty(
            "query_rewrite",
            (breakdown, nanos) -> breakdown.recordQueryRewrite(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordIndexResolution.
     * <p>
     * Validates: Requirements 1.3
     */
    public void testAdditiveRecordingIndexResolution() {
        verifyAdditiveProperty(
            "index_resolution",
            (breakdown, nanos) -> breakdown.recordIndexResolution(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordShardRouting.
     * <p>
     * Validates: Requirements 1.4
     */
    public void testAdditiveRecordingShardRouting() {
        verifyAdditiveProperty(
            "shard_routing",
            (breakdown, nanos) -> breakdown.recordShardRouting(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordTermsLookupSubQuery.
     * <p>
     * Validates: Requirements 1.6
     */
    public void testAdditiveRecordingTermsLookupSubQuery() {
        verifyAdditiveProperty(
            "terms_lookup_sub_query",
            (breakdown, nanos) -> breakdown.recordTermsLookupSubQuery(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordCoordinatorQueueWait.
     * <p>
     * Validates: Requirements 4.1
     */
    public void testAdditiveRecordingCoordinatorQueueWait() {
        verifyAdditiveProperty(
            "coordinator_queue_wait",
            (breakdown, nanos) -> breakdown.recordCoordinatorQueueWait(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordReduceTopDocs.
     * <p>
     * Validates: Requirements 10.1
     */
    public void testAdditiveRecordingReduceTopDocs() {
        verifyAdditiveProperty(
            "reduce_top_docs",
            (breakdown, nanos) -> breakdown.recordReduceTopDocs(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordReduceAggregations.
     * <p>
     * Validates: Requirements 10.2
     */
    public void testAdditiveRecordingReduceAggregations() {
        verifyAdditiveProperty(
            "reduce_aggregations",
            (breakdown, nanos) -> breakdown.recordReduceAggregations(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordReducePipelineAggs.
     * <p>
     * Validates: Requirements 10.3
     */
    public void testAdditiveRecordingReducePipelineAggs() {
        verifyAdditiveProperty(
            "reduce_pipeline_aggs",
            (breakdown, nanos) -> breakdown.recordReducePipelineAggs(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordReduceSuggestions.
     * <p>
     * Validates: Requirements 10.4
     */
    public void testAdditiveRecordingReduceSuggestions() {
        verifyAdditiveProperty(
            "reduce_suggestions",
            (breakdown, nanos) -> breakdown.recordReduceSuggestions(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordRequestSerialization.
     * <p>
     * Validates: Requirements 8.3
     */
    public void testAdditiveRecordingRequestSerialization() {
        verifyAdditiveProperty(
            "request_serialization",
            (breakdown, nanos) -> breakdown.recordRequestSerialization(nanos)
        );
    }

    /**
     * Property 1: Max-aggregation recording for recordResponseDeserialization.
     * <p>
     * Since response deserialization uses max-aggregation across shard responses,
     * recording multiple values should retain the maximum, not the sum.
     * <p>
     * Validates: Requirements 7.4
     */
    public void testMaxAggregationRecordingResponseDeserialization() {
        for (int iteration = 0; iteration < MIN_ITERATIONS; iteration++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            int sequenceLength = randomIntBetween(1, 20);
            long expectedMax = 0;

            for (int i = 0; i < sequenceLength; i++) {
                long nanos = randomLongBetween(0, 100_000_000L);
                breakdown.recordResponseDeserialization(nanos);
                expectedMax = Math.max(expectedMax, nanos);
            }

            long absoluteStartNanos = 0;
            long requestEndNanos = absoluteStartNanos + expectedMax + 1_000_000_000L;
            Map<String, Object> unifiedMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedMax);

            if (expectedMillis > 0) {
                assertTrue(
                    "Expected key 'response_deserialization' to be present in unified map when millis=" + expectedMillis
                        + " (nanos=" + expectedMax + ") at iteration " + iteration,
                    unifiedMap.containsKey("response_deserialization")
                );
                assertEquals(
                    "Expected value for 'response_deserialization' to be " + expectedMillis
                        + " but was " + unifiedMap.get("response_deserialization") + " at iteration " + iteration,
                    expectedMillis,
                    (long) unifiedMap.get("response_deserialization")
                );
            } else {
                assertFalse(
                    "Expected key 'response_deserialization' to be absent from unified map when millis=0"
                        + " (nanos=" + expectedMax + ") at iteration " + iteration,
                    unifiedMap.containsKey("response_deserialization")
                );
            }
        }
    }

    /**
     * Property 1: Additive recording preserves sum for recordInboundNetworkTime.
     * <p>
     * Validates: Requirements 8.5
     */
    public void testAdditiveRecordingInboundNetworkTime() {
        verifyAdditiveProperty(
            "inbound_network_time",
            (breakdown, nanos) -> breakdown.recordInboundNetworkTime(nanos)
        );
    }

    /**
     * Property 1: Additive recording preserves sum for recordOutboundNetworkTime.
     * <p>
     * Validates: Requirements 8.6
     */
    public void testAdditiveRecordingOutboundNetworkTime() {
        verifyAdditiveProperty(
            "outbound_network_time",
            (breakdown, nanos) -> breakdown.recordOutboundNetworkTime(nanos)
        );
    }

    /**
     * Core verification logic for the additive recording property.
     * <p>
     * For each iteration:
     * 1. Create a fresh SearchLatencyBreakdown instance
     * 2. Generate a random sequence of non-negative nanos values
     * 3. Record all values via the provided recording method
     * 4. Verify the final stored value equals the sum of all recorded values
     * 5. Verify toUnifiedBreakdownMap contains nanos/1_000_000 if millis > 0
     *
     * @param expectedKey the key expected in the unified breakdown map
     * @param recorder    a bi-consumer that records a nanos value on a breakdown instance
     */
    private void verifyAdditiveProperty(String expectedKey, RecordingFunction recorder) {
        for (int iteration = 0; iteration < MIN_ITERATIONS; iteration++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random sequence length (1 to 20 recordings per iteration)
            int sequenceLength = randomIntBetween(1, 20);
            long expectedSum = 0;

            for (int i = 0; i < sequenceLength; i++) {
                // Generate non-negative nanos value (0 to 100ms in nanos)
                long nanos = randomLongBetween(0, 100_000_000L);
                recorder.record(breakdown, nanos);
                expectedSum += nanos;
            }

            // Verify the unified breakdown map
            // Use absoluteStartNanos=0 and requestEndNanos=expectedSum+1 to avoid overhead computation issues
            long absoluteStartNanos = 0;
            long requestEndNanos = absoluteStartNanos + expectedSum + 1_000_000_000L;
            Map<String, Object> unifiedMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedSum);

            if (expectedMillis > 0) {
                // The key MUST be present with the correct millis value
                assertTrue(
                    "Expected key '" + expectedKey + "' to be present in unified map when millis=" + expectedMillis
                        + " (nanos=" + expectedSum + ") at iteration " + iteration,
                    unifiedMap.containsKey(expectedKey)
                );
                assertEquals(
                    "Expected value for '" + expectedKey + "' to be " + expectedMillis
                        + " but was " + unifiedMap.get(expectedKey) + " at iteration " + iteration,
                    expectedMillis,
                    (long) unifiedMap.get(expectedKey)
                );
            } else {
                // Zero millis entries MUST be omitted
                assertFalse(
                    "Expected key '" + expectedKey + "' to be absent from unified map when millis=0"
                        + " (nanos=" + expectedSum + ") at iteration " + iteration,
                    unifiedMap.containsKey(expectedKey)
                );
            }
        }
    }

    /**
     * Functional interface for recording a nanos value on a breakdown instance.
     */
    @FunctionalInterface
    private interface RecordingFunction {
        void record(SearchLatencyBreakdown breakdown, long nanos);
    }
}
