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

/**
 * Property-based test for SearchLatencyBreakdown.
 * <p>
 * Feature: search-latency-breakdown, Property 5: Zero-duration entries omitted from output map
 * <p>
 * For any breakdown instance where some fields are zero and some are non-zero,
 * the toUnifiedBreakdownMap SHALL contain no entry with a value of 0 or negative.
 * <p>
 * <b>Validates: Requirements 11.5</b>
 */
public class SearchLatencyBreakdownProperty5Tests extends OpenSearchTestCase {

    /**
     * Property 5: Zero-duration entries omitted from output map.
     * <p>
     * For each iteration, we create a fresh SearchLatencyBreakdown, randomly set some fields
     * to non-zero values (>= 1_000_000 nanos to ensure they convert to >= 1 millis) and leave
     * others at zero. Then we verify that NO entry in the resulting unified breakdown map has
     * a value of 0 or negative.
     * <p>
     * We also test values below 1_000_000 nanos (which round down to 0 millis) and verify
     * they are excluded from the output map.
     */
    public void testZeroDurationEntriesOmittedFromUnifiedBreakdownMap() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(10_000_000L, 500_000_000L);

            // Randomly record some fields with non-zero values, leaving others at 0
            // For pre-search metrics (addAndGet)
            if (randomBoolean()) breakdown.recordRestRequestParsing(randomLongBetween(0, 50_000_000L));
            if (randomBoolean()) breakdown.recordSecurityAuth(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordPipelineRequestTransform(randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordQueryRewrite(randomLongBetween(0, 100_000_000L));
            if (randomBoolean()) breakdown.recordTermsLookupSubQuery(randomLongBetween(0, 40_000_000L));
            if (randomBoolean()) breakdown.recordIndexResolution(randomLongBetween(0, 10_000_000L));
            if (randomBoolean()) breakdown.recordClusterStateCheck(randomLongBetween(0, 10_000_000L));
            if (randomBoolean()) breakdown.recordShardRouting(randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordWeightedRouting(randomLongBetween(0, 15_000_000L));

            // Queuing
            if (randomBoolean()) breakdown.recordCoordinatorQueueWait(randomLongBetween(0, 100_000_000L));
            if (randomBoolean()) breakdown.recordDataNodeQueueWaitMax(randomLongBetween(0, 80_000_000L));
            if (randomBoolean()) breakdown.recordDataNodeQueueWaitAvg(randomLongBetween(0, 50_000_000L));

            // Phases
            if (randomBoolean()) breakdown.recordCanMatchPhase(randomLongBetween(0, 50_000_000L));
            if (randomBoolean()) breakdown.recordDfsPhase(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordQueryPhase(randomLongBetween(0, 200_000_000L));
            if (randomBoolean()) breakdown.recordFetchPhase(randomLongBetween(0, 100_000_000L));
            if (randomBoolean()) breakdown.recordExpandPhase(randomLongBetween(0, 30_000_000L));

            // Inter-phase gaps
            if (randomBoolean()) breakdown.recordInterPhaseGap("can_match", "query", randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordInterPhaseGap("query", "fetch", randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordInterPhaseGap("fetch", "expand", randomLongBetween(0, 15_000_000L));

            // Query phase internals (max across shards)
            if (randomBoolean()) breakdown.recordSearchIdleReactivation(randomLongBetween(0, 10_000_000L));
            if (randomBoolean()) breakdown.recordSearchContextCreation(randomLongBetween(0, 50_000_000L));
            if (randomBoolean()) breakdown.recordReadLockAcquisition(randomLongBetween(0, 5_000_000L));
            if (randomBoolean()) breakdown.recordAcquireSearcher(randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordGlobalOrdinalsLoading(randomLongBetween(0, 80_000_000L));
            if (randomBoolean()) breakdown.recordFielddataLoading(randomLongBetween(0, 60_000_000L));
            if (randomBoolean()) breakdown.recordScriptCompilation(randomLongBetween(0, 40_000_000L));
            if (randomBoolean()) breakdown.recordNestedBitsetConstruction(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordStarTreeSetup(randomLongBetween(0, 25_000_000L));
            if (randomBoolean()) breakdown.recordDerivedFieldScript(randomLongBetween(0, 35_000_000L));

            // Cache
            if (randomBoolean()) breakdown.recordRequestCacheLookup(randomLongBetween(0, 10_000_000L));
            if (randomBoolean()) breakdown.recordRequestCacheWrite(randomLongBetween(0, 15_000_000L));
            if (randomBoolean()) breakdown.recordQueryCacheLookup(randomLongBetween(0, 10_000_000L));
            if (randomBoolean()) breakdown.recordQueryCacheWrite(randomLongBetween(0, 12_000_000L));

            // Segment execution
            if (randomBoolean()) breakdown.recordLuceneCreateWeight(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordLuceneBuildScorer(randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordLuceneNextDocAdvance(randomLongBetween(0, 50_000_000L));
            if (randomBoolean()) breakdown.recordLuceneScore(randomLongBetween(0, 40_000_000L));
            if (randomBoolean()) breakdown.recordPageCacheMiss(randomLongBetween(0, 60_000_000L));
            if (randomBoolean()) breakdown.recordRemoteStoreFetch(randomLongBetween(0, 100_000_000L));
            if (randomBoolean()) breakdown.recordMergeIoContention(randomLongBetween(0, 70_000_000L));

            // Aggregation
            if (randomBoolean()) breakdown.recordAggInitialize(randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordAggBuildLeafCollector(randomLongBetween(0, 25_000_000L));
            if (randomBoolean()) breakdown.recordAggCollect(randomLongBetween(0, 80_000_000L));
            if (randomBoolean()) breakdown.recordAggPostCollection(randomLongBetween(0, 15_000_000L));
            if (randomBoolean()) breakdown.recordAggDeferredReplay(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordAggBuildAggregation(randomLongBetween(0, 35_000_000L));
            if (randomBoolean()) breakdown.recordGlobalAggSeparatePass(randomLongBetween(0, 50_000_000L));

            // Concurrent segment search
            if (randomBoolean()) breakdown.recordSliceCreation(randomLongBetween(0, 10_000_000L));
            if (randomBoolean()) breakdown.recordSliceScheduling(randomLongBetween(0, 15_000_000L));
            if (randomBoolean()) breakdown.recordSliceMaxExecution(randomLongBetween(0, 80_000_000L));
            if (randomBoolean()) breakdown.recordSliceMinExecution(randomLongBetween(0, 40_000_000L));
            if (randomBoolean()) breakdown.recordSliceResultAggregation(randomLongBetween(0, 20_000_000L));

            // Reduce
            if (randomBoolean()) breakdown.recordReduceTopDocs(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordReduceAggregations(randomLongBetween(0, 25_000_000L));
            if (randomBoolean()) breakdown.recordReducePipelineAggs(randomLongBetween(0, 15_000_000L));
            if (randomBoolean()) breakdown.recordReduceSuggestions(randomLongBetween(0, 10_000_000L));

            // Transport
            if (randomBoolean()) breakdown.recordNetworkRoundtripMax(randomLongBetween(0, 50_000_000L));
            if (randomBoolean()) breakdown.recordNetworkRoundtripAvg(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordRequestSerialization(randomLongBetween(0, 10_000_000L));
            if (randomBoolean()) breakdown.recordResponseDeserialization(randomLongBetween(0, 12_000_000L));
            if (randomBoolean()) breakdown.recordInboundNetworkTime(randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordOutboundNetworkTime(randomLongBetween(0, 18_000_000L));

            // Fetch internals
            if (randomBoolean()) breakdown.recordFetchStoredFields(randomLongBetween(0, 30_000_000L));
            if (randomBoolean()) breakdown.recordFetchSourceLoading(randomLongBetween(0, 25_000_000L));
            if (randomBoolean()) breakdown.recordFetchHighlighting(randomLongBetween(0, 40_000_000L));
            if (randomBoolean()) breakdown.recordFetchScriptFields(randomLongBetween(0, 20_000_000L));
            if (randomBoolean()) breakdown.recordFetchInnerHits(randomLongBetween(0, 35_000_000L));

            // Post-search
            if (randomBoolean()) breakdown.recordPipelineResponseTransform(randomLongBetween(0, 15_000_000L));
            if (randomBoolean()) breakdown.recordResponseSerialization(randomLongBetween(0, 10_000_000L));

            // Circuit breaker
            if (randomBoolean()) breakdown.recordCircuitBreakerCheck(randomLongBetween(0, 5_000_000L));

            // Phase tracking for overhead computation
            if (randomBoolean()) {
                long phaseStart = absoluteStart + randomLongBetween(1_000_000L, 50_000_000L);
                breakdown.markFirstPhaseStart(phaseStart);
                breakdown.markPhaseEnd("query", phaseStart + randomLongBetween(1_000_000L, 100_000_000L));
            }

            // Generate the unified breakdown map
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // PROPERTY ASSERTION: No entry should have a value of 0 or negative
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                assertTrue(
                    "Entry '" + entry.getKey() + "' has non-positive value " + entry.getValue()
                        + " in iteration " + i + ". toUnifiedBreakdownMap must omit zero/negative entries.",
                    (entry.getValue() instanceof Long) && (Long) entry.getValue() > 0
                );
            }
        }
    }

    /**
     * Property 5 (Sub-property): Values below the millisecond threshold (less than 1_000_000 nanos) must NOT appear.
     * <p>
     * This test specifically generates values that are below 1ms (less than 1_000_000 nanos) and verifies
     * they are excluded from the output, since TimeUnit.NANOSECONDS.toMillis() truncates to 0.
     */
    public void testSubMillisecondValuesExcludedFromMap() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(10_000_000L, 500_000_000L);

            // Record values that are strictly below 1 millisecond (< 1_000_000 nanos)
            // These should all be omitted from the output map
            long subMillisValue = randomLongBetween(1, 999_999);

            breakdown.recordRestRequestParsing(subMillisValue);
            breakdown.recordQueryRewrite(subMillisValue);
            breakdown.recordShardRouting(subMillisValue);
            breakdown.recordQueryPhase(subMillisValue);
            breakdown.recordFetchPhase(subMillisValue);
            breakdown.recordSearchContextCreation(subMillisValue);
            breakdown.recordAggCollect(subMillisValue);
            breakdown.recordReduceTopDocs(subMillisValue);
            breakdown.recordNetworkRoundtripMax(subMillisValue);
            breakdown.recordFetchStoredFields(subMillisValue);

            // Set phase tracking with sub-millisecond overhead
            long phaseStart = absoluteStart + randomLongBetween(1, 999_999);
            breakdown.markFirstPhaseStart(phaseStart);
            long phaseEnd = requestEnd - randomLongBetween(1, 999_999);
            breakdown.markPhaseEnd("query", phaseEnd);

            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // PROPERTY ASSERTION: All entries in the map must be strictly positive
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                assertTrue(
                    "Entry '" + entry.getKey() + "' has non-positive value " + entry.getValue()
                        + " in iteration " + i + ". Sub-millisecond values must be excluded.",
                    (entry.getValue() instanceof Long) && (Long) entry.getValue() > 0
                );
            }
        }
    }

    /**
     * Property 5 (Sub-property): An all-zero breakdown produces an empty map.
     * <p>
     * When no metrics are recorded and phase tracking yields zero overhead,
     * the unified breakdown map should be completely empty.
     */
    public void testAllZeroBreakdownProducesEmptyMap() {
        final int iterations = 100;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            // Use the same value for absoluteStart and requestEnd so overheads are zero
            long time = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(time, time);

            // With no recordings and no phase starts, map should be empty
            assertTrue(
                "All-zero breakdown should produce empty map but got " + resultMap.size() + " entries in iteration " + i,
                resultMap.isEmpty()
            );
        }
    }

    /**
     * Property 5 (Sub-property): Mix of exactly-zero and positive fields.
     * <p>
     * Explicitly set some fields to exactly 0 nanos and others to values >= 1_000_000 nanos.
     * Verify only the positive-millis entries appear.
     */
    public void testMixedZeroAndPositiveFieldsOnlyPositiveAppear() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(100_000_000L, 500_000_000L);

            // Explicitly record zero (no-op since fields start at 0, but verify behavior)
            breakdown.recordRestRequestParsing(0);
            breakdown.recordSecurityAuth(0);
            breakdown.recordPipelineRequestTransform(0);

            // Record clearly positive values (>= 1ms in nanos)
            long positiveNanos = randomLongBetween(1_000_000L, 100_000_000L);
            breakdown.recordQueryRewrite(positiveNanos);
            breakdown.recordQueryPhase(positiveNanos);
            breakdown.recordFetchPhase(positiveNanos);

            // Record sub-millisecond values that should be excluded
            breakdown.recordShardRouting(randomLongBetween(1, 999_999));
            breakdown.recordIndexResolution(randomLongBetween(1, 999_999));

            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // PROPERTY ASSERTION: no zero or negative entries
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                assertTrue(
                    "Entry '" + entry.getKey() + "' has non-positive value " + entry.getValue()
                        + " in iteration " + i,
                    (entry.getValue() instanceof Long) && (Long) entry.getValue() > 0
                );
            }

            // PROPERTY ASSERTION: the fields we set to clearly positive nanos should appear
            long expectedMillis = positiveNanos / 1_000_000;
            if (expectedMillis > 0) {
                assertTrue(
                    "query_rewrite should appear in map with positive millis value, iteration " + i,
                    resultMap.containsKey("query_rewrite") && (Long) resultMap.get("query_rewrite") > 0
                );
            }
        }
    }
}
