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
 * Property-based test validating conditional metrics appear only when their feature is active.
 * <p>
 * Feature: breakdown-ordering-fix, Property 12: Conditional metrics appear only when their feature is active
 * <p>
 * For any breakdown produced without global aggregations configured, the key {@code global_agg_separate_pass}
 * SHALL NOT appear. Similarly for {@code search_idle_reactivation} (only when shard was idle),
 * {@code slice_*} metrics (only with concurrent segment search), and {@code pipeline_response_transform}
 * (only with response processors).
 * <p>
 * <b>Validates: Requirements 11.2, 12.2, 13.2, 14.2</b>
 */
public class ConditionalMetricsFeatureActivePropertyTests extends OpenSearchTestCase {

    /** Minimum 100 iterations as required by the design document. */
    private static final int ITERATIONS = 150;

    /**
     * Minimum nanos value that will produce a positive millis conversion (1ms = 1_000_000 nanos).
     * putIfPositive only includes entries where TimeUnit.NANOSECONDS.toMillis(nanos) > 0.
     */
    private static final long MIN_NANOS_FOR_POSITIVE_MILLIS = 1_000_000L;

    /** Maximum nanos value for random generation (100ms). */
    private static final long MAX_NANOS = 100_000_000L;

    // ========== Slice metric keys (CSS = Concurrent Segment Search) ==========
    private static final String[] SLICE_METRIC_KEYS = {
        "slice_creation",
        "slice_scheduling",
        "slice_max_execution",
        "slice_result_aggregation"
    };

    /**
     * Property 12: global_agg_separate_pass is absent when global aggregations are NOT configured.
     * <p>
     * Generates random feature flag combinations. When global aggs are disabled (nothing recorded),
     * asserts that global_agg_separate_pass does not appear in the unified breakdown map.
     * When enabled (recorded with positive value), asserts that it IS present.
     * <p>
     * <b>Validates: Requirements 11.2</b>
     */
    public void testProperty12_globalAggSeparatePassAbsentWithoutGlobalAggs() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            boolean hasGlobalAggs = randomBoolean();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Only record global_agg_separate_pass if the feature is active
            if (hasGlobalAggs) {
                long nanos = randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS);
                breakdown.recordGlobalAggSeparatePass(nanos);
            }

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            if (hasGlobalAggs) {
                assertTrue(
                    "Iteration " + i + ": global_agg_separate_pass MUST be present when global aggs are configured",
                    map.containsKey("global_agg_separate_pass")
                );
            } else {
                assertFalse(
                    "Iteration " + i + ": global_agg_separate_pass MUST be absent when global aggs are NOT configured",
                    map.containsKey("global_agg_separate_pass")
                );
            }
        }
    }

    /**
     * Property 12: search_idle_reactivation is absent when shard was NOT idle.
     * <p>
     * Generates random feature flag combinations. When shard idle reactivation did NOT occur
     * (nothing recorded), asserts that search_idle_reactivation does not appear.
     * When it did occur (recorded with positive value), asserts that it IS present.
     * <p>
     * <b>Validates: Requirements 12.2</b>
     */
    public void testProperty12_searchIdleReactivationAbsentWhenShardNotIdle() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            boolean shardWasIdle = randomBoolean();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Only record search_idle_reactivation if the shard was idle
            if (shardWasIdle) {
                long nanos = randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS);
                breakdown.recordSearchIdleReactivation(nanos);
            }

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            if (shardWasIdle) {
                assertTrue(
                    "Iteration " + i + ": search_idle_reactivation MUST be present when shard was idle",
                    map.containsKey("search_idle_reactivation")
                );
            } else {
                assertFalse(
                    "Iteration " + i + ": search_idle_reactivation MUST be absent when shard was NOT idle",
                    map.containsKey("search_idle_reactivation")
                );
            }
        }
    }

    /**
     * Property 12: slice_* metrics are absent when concurrent segment search is NOT active.
     * <p>
     * Generates random feature flag combinations. When CSS is disabled (nothing recorded for slice metrics),
     * asserts that no slice_* keys appear in the map. When CSS is enabled (recorded with positive values),
     * asserts that slice_* keys ARE present.
     * <p>
     * <b>Validates: Requirements 14.2</b>
     */
    public void testProperty12_sliceMetricsAbsentWithoutConcurrentSegmentSearch() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            boolean cssEnabled = randomBoolean();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Only record slice metrics if CSS is active
            if (cssEnabled) {
                breakdown.recordSliceCreation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
                breakdown.recordSliceScheduling(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
                breakdown.recordSliceMaxExecution(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
                breakdown.recordSliceResultAggregation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            }

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            if (cssEnabled) {
                for (String sliceKey : SLICE_METRIC_KEYS) {
                    assertTrue(
                        "Iteration " + i + ": " + sliceKey + " MUST be present when CSS is active",
                        map.containsKey(sliceKey)
                    );
                }
            } else {
                for (String sliceKey : SLICE_METRIC_KEYS) {
                    assertFalse(
                        "Iteration " + i + ": " + sliceKey + " MUST be absent when CSS is NOT active",
                        map.containsKey(sliceKey)
                    );
                }
            }
        }
    }

    /**
     * Property 12: pipeline_response_transform is absent when response processors are NOT configured.
     * <p>
     * Generates random feature flag combinations. When response processors are not configured
     * (nothing recorded), asserts that pipeline_response_transform does not appear.
     * When configured (recorded with positive value), asserts that it IS present.
     * <p>
     * <b>Validates: Requirements 13.2</b>
     */
    public void testProperty12_pipelineResponseTransformAbsentWithoutResponseProcessors() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            boolean hasResponseProcessors = randomBoolean();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Only record pipeline_response_transform if response processors are configured
            if (hasResponseProcessors) {
                long nanos = randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS);
                breakdown.recordPipelineResponseTransform(nanos);
            }

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            if (hasResponseProcessors) {
                assertTrue(
                    "Iteration " + i + ": pipeline_response_transform MUST be present when response processors configured",
                    map.containsKey("pipeline_response_transform")
                );
            } else {
                assertFalse(
                    "Iteration " + i + ": pipeline_response_transform MUST be absent when response processors NOT configured",
                    map.containsKey("pipeline_response_transform")
                );
            }
        }
    }

    /**
     * Property 12: Combined random feature flag test — all four conditional metrics tested
     * simultaneously with random combinations.
     * <p>
     * Generates random combinations of all four feature flags and verifies each conditional
     * metric's presence/absence independently matches its feature flag state.
     * <p>
     * <b>Validates: Requirements 11.2, 12.2, 13.2, 14.2</b>
     */
    public void testProperty12_allConditionalMetrics_randomCombinations() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Random feature flag combination
            boolean hasGlobalAggs = randomBoolean();
            boolean shardWasIdle = randomBoolean();
            boolean cssEnabled = randomBoolean();
            boolean hasResponseProcessors = randomBoolean();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Conditionally record metrics based on feature flags
            if (hasGlobalAggs) {
                breakdown.recordGlobalAggSeparatePass(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            }
            if (shardWasIdle) {
                breakdown.recordSearchIdleReactivation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            }
            if (cssEnabled) {
                breakdown.recordSliceCreation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
                breakdown.recordSliceScheduling(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
                breakdown.recordSliceMaxExecution(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
                breakdown.recordSliceResultAggregation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            }
            if (hasResponseProcessors) {
                breakdown.recordPipelineResponseTransform(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            }

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            // Assert: global_agg_separate_pass
            assertEquals(
                "Iteration " + i + ": global_agg_separate_pass presence must match hasGlobalAggs=" + hasGlobalAggs,
                hasGlobalAggs,
                map.containsKey("global_agg_separate_pass")
            );

            // Assert: search_idle_reactivation
            assertEquals(
                "Iteration " + i + ": search_idle_reactivation presence must match shardWasIdle=" + shardWasIdle,
                shardWasIdle,
                map.containsKey("search_idle_reactivation")
            );

            // Assert: slice_* metrics
            for (String sliceKey : SLICE_METRIC_KEYS) {
                assertEquals(
                    "Iteration " + i + ": " + sliceKey + " presence must match cssEnabled=" + cssEnabled,
                    cssEnabled,
                    map.containsKey(sliceKey)
                );
            }

            // Assert: pipeline_response_transform
            assertEquals(
                "Iteration " + i + ": pipeline_response_transform presence must match hasResponseProcessors="
                    + hasResponseProcessors,
                hasResponseProcessors,
                map.containsKey("pipeline_response_transform")
            );
        }
    }

    /**
     * Property 12: When ALL features are disabled (nothing recorded), no conditional metrics appear.
     * <p>
     * Verifies that a completely "vanilla" breakdown (no special features configured) produces
     * no conditional metric keys at all.
     * <p>
     * <b>Validates: Requirements 11.2, 12.2, 13.2, 14.2</b>
     */
    public void testProperty12_noConditionalMetrics_whenAllFeaturesDisabled() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Do NOT record any conditional metrics — all features are "off"

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            // None of the conditional metrics should appear
            assertFalse(
                "Iteration " + i + ": global_agg_separate_pass must NOT appear when all features disabled",
                map.containsKey("global_agg_separate_pass")
            );
            assertFalse(
                "Iteration " + i + ": search_idle_reactivation must NOT appear when all features disabled",
                map.containsKey("search_idle_reactivation")
            );
            for (String sliceKey : SLICE_METRIC_KEYS) {
                assertFalse(
                    "Iteration " + i + ": " + sliceKey + " must NOT appear when all features disabled",
                    map.containsKey(sliceKey)
                );
            }
            assertFalse(
                "Iteration " + i + ": pipeline_response_transform must NOT appear when all features disabled",
                map.containsKey("pipeline_response_transform")
            );
        }
    }

    /**
     * Property 12: When ALL features are enabled (all recorded with positive values),
     * all conditional metrics appear.
     * <p>
     * Verifies that when all conditional feature flags are "on" with valid positive durations,
     * all conditional metrics are present in the output map.
     * <p>
     * <b>Validates: Requirements 11.2, 12.2, 13.2, 14.2</b>
     */
    public void testProperty12_allConditionalMetrics_whenAllFeaturesEnabled() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Record ALL conditional metrics with values >= 1ms
            breakdown.recordGlobalAggSeparatePass(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            breakdown.recordSearchIdleReactivation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            breakdown.recordSliceCreation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            breakdown.recordSliceScheduling(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            breakdown.recordSliceMaxExecution(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            breakdown.recordSliceResultAggregation(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));
            breakdown.recordPipelineResponseTransform(randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS));

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            // All conditional metrics MUST be present
            assertTrue(
                "Iteration " + i + ": global_agg_separate_pass MUST be present when all features enabled",
                map.containsKey("global_agg_separate_pass")
            );
            assertTrue(
                "Iteration " + i + ": search_idle_reactivation MUST be present when all features enabled",
                map.containsKey("search_idle_reactivation")
            );
            for (String sliceKey : SLICE_METRIC_KEYS) {
                assertTrue(
                    "Iteration " + i + ": " + sliceKey + " MUST be present when all features enabled",
                    map.containsKey(sliceKey)
                );
            }
            assertTrue(
                "Iteration " + i + ": pipeline_response_transform MUST be present when all features enabled",
                map.containsKey("pipeline_response_transform")
            );
        }
    }
}
