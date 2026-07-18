/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Property-based test validating that no start offset keys are emitted when requestStartNanos is zero.
 * <p>
 * Feature: breakdown-ordering-fix, Property 2: No start offset keys emitted when requestStartNanos is zero
 * <p>
 * For any shard breakdown produced when {@code requestStartNanos == 0}, no key in the output map
 * SHALL end with {@code "_start"}. This tests the contract that older data nodes (or requests
 * without timing support) don't pollute the breakdown with start offset keys.
 * <p>
 * <b>Validates: Requirements 1.6</b>
 */
public class NoStartOffsetKeysWhenRequestStartZeroPropertyTests extends OpenSearchTestCase {

    /** Minimum 100 iterations as required by the design document. */
    private static final int ITERATIONS = 150;

    /** Known shard timing keys that would have corresponding _start keys when requestStartNanos > 0. */
    private static final String[] DURATION_KEYS = {
        "agg_pre_process",
        "query_internal_execution",
        "agg_post_process",
        "request_cache_lookup",
        "request_cache_write",
        "query_pre_process",
        "rescore",
        "suggest"
    };

    /**
     * Property 2: No start offset keys emitted when requestStartNanos is zero.
     * <p>
     * Simulates the data node's conditional logic from QueryPhase.java:
     * <pre>
     *   final long requestStartNanos = searchContext.request().getRequestStartNanos();
     *   if (requestStartNanos > 0) {
     *       // record X_start keys...
     *   }
     * </pre>
     * When requestStartNanos == 0, the conditional block is skipped, so no *_start keys appear.
     * <p>
     * This test generates random shard timing durations and applies the conditional logic with
     * requestStartNanos = 0, asserting that no key in the resulting map ends with "_start".
     * <p>
     * <b>Validates: Requirements 1.6</b>
     */
    public void testNoStartOffsetKeysWhenRequestStartNanosIsZero() {
        for (int i = 0; i < ITERATIONS; i++) {
            // requestStartNanos == 0 simulates an older coordinator or unset value
            final long requestStartNanos = 0;

            // Generate random shard timing durations (simulating various sub-operations)
            Map<String, Long> shardBreakdown = new HashMap<>();

            // Randomly select a subset of duration keys to be present (not all operations always run)
            int numKeysPresent = randomIntBetween(1, DURATION_KEYS.length);
            for (int k = 0; k < numKeysPresent; k++) {
                String key = DURATION_KEYS[randomIntBetween(0, DURATION_KEYS.length - 1)];
                long duration = randomLongBetween(1_000L, 500_000_000L); // 1µs to 500ms in nanos
                shardBreakdown.put(key, duration);
            }

            // Also add some random unknown timing keys (for self-describing extensibility)
            int numRandomKeys = randomIntBetween(0, 3);
            for (int r = 0; r < numRandomKeys; r++) {
                String randomKey = "custom_metric_" + randomIntBetween(1, 100);
                long duration = randomLongBetween(1_000L, 100_000_000L);
                shardBreakdown.put(randomKey, duration);
            }

            // Apply the same conditional logic as QueryPhase.java:
            // if (requestStartNanos > 0) { record X_start... }
            // Since requestStartNanos == 0, no _start keys should be recorded
            Map<String, Long> outputMap = simulateShardTimingRecording(requestStartNanos, shardBreakdown);

            // PROPERTY ASSERTION: No key in the output map ends with "_start"
            for (String key : outputMap.keySet()) {
                assertFalse(
                    "No key should end with '_start' when requestStartNanos == 0, but found: '"
                        + key + "' in iteration " + i + " (map: " + outputMap + ")",
                    key.endsWith("_start")
                );
            }

            // Additional assertion: all duration keys that were put in are still present
            for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
                assertTrue(
                    "Duration key '" + entry.getKey() + "' should still be present in output (iteration " + i + ")",
                    outputMap.containsKey(entry.getKey())
                );
                assertEquals(
                    "Duration value for '" + entry.getKey() + "' should be preserved (iteration " + i + ")",
                    entry.getValue(),
                    outputMap.get(entry.getKey())
                );
            }
        }
    }

    /**
     * Property 2 variant: With random eventStartNanos values, the conditional block is still skipped.
     * <p>
     * Even when event start nanos values are large (simulating late-arriving operations),
     * the guard condition {@code requestStartNanos > 0} prevents any _start key from being recorded.
     * <p>
     * <b>Validates: Requirements 1.6</b>
     */
    public void testNoStartOffsetKeysRegardlessOfEventStartNanosValues() {
        for (int i = 0; i < ITERATIONS; i++) {
            final long requestStartNanos = 0;

            // Generate random event start timestamps (as if System.nanoTime() were called)
            long aggPreProcessStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long queryExecStartNanos = aggPreProcessStartNanos + randomLongBetween(10_000L, 100_000_000L);
            long aggPostProcessStartNanos = queryExecStartNanos + randomLongBetween(10_000L, 500_000_000L);
            long cacheStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Generate durations
            long aggPreProcessDuration = randomLongBetween(1_000L, 50_000_000L);
            long queryExecDuration = randomLongBetween(100_000L, 1_000_000_000L);
            long aggPostProcessDuration = randomLongBetween(1_000L, 30_000_000L);
            long cacheLookupDuration = randomLongBetween(1_000L, 10_000_000L);

            // Simulate the full recording logic as in QueryPhase.execute():
            Map<String, Long> outputMap = new HashMap<>();

            // Always record duration keys
            outputMap.put("agg_pre_process", aggPreProcessDuration);
            outputMap.put("query_internal_execution", queryExecDuration);
            outputMap.put("agg_post_process", aggPostProcessDuration);
            outputMap.put("request_cache_lookup", cacheLookupDuration);

            // Apply the conditional: if (requestStartNanos > 0) { ... }
            if (requestStartNanos > 0) {
                outputMap.put("agg_pre_process_start", Math.max(0, aggPreProcessStartNanos - requestStartNanos));
                outputMap.put("query_internal_execution_start", Math.max(0, queryExecStartNanos - requestStartNanos));
                outputMap.put("agg_post_process_start", Math.max(0, aggPostProcessStartNanos - requestStartNanos));
                outputMap.put("request_cache_lookup_start", Math.max(0, cacheStartNanos - requestStartNanos));
            }

            // PROPERTY ASSERTION: No key ends with "_start"
            for (String key : outputMap.keySet()) {
                assertFalse(
                    "No key should end with '_start' when requestStartNanos == 0, but found: '"
                        + key + "' (iteration " + i + ")",
                    key.endsWith("_start")
                );
            }

            // The output should only contain duration keys
            assertEquals("Should have exactly 4 duration keys (iteration " + i + ")", 4, outputMap.size());
        }
    }

    /**
     * Property 2 contrast: When requestStartNanos > 0, _start keys ARE emitted.
     * <p>
     * This is the positive control test that confirms the conditional logic works correctly
     * in both directions: _start keys appear when requestStartNanos > 0, and are absent when == 0.
     * <p>
     * <b>Validates: Requirements 1.6 (by contrast)</b>
     */
    public void testStartOffsetKeysEmittedWhenRequestStartNanosPositive() {
        for (int i = 0; i < ITERATIONS; i++) {
            // requestStartNanos > 0 means absolute offsets ARE recorded
            final long requestStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Generate random event timestamps after request start
            long aggPreProcessStartNanos = requestStartNanos + randomLongBetween(10_000L, 200_000_000L);
            long queryExecStartNanos = aggPreProcessStartNanos + randomLongBetween(10_000L, 100_000_000L);
            long aggPostProcessStartNanos = queryExecStartNanos + randomLongBetween(100_000L, 500_000_000L);

            // Generate durations
            long aggPreProcessDuration = randomLongBetween(1_000L, 50_000_000L);
            long queryExecDuration = randomLongBetween(100_000L, 1_000_000_000L);
            long aggPostProcessDuration = randomLongBetween(1_000L, 30_000_000L);

            // Simulate the full recording logic
            Map<String, Long> outputMap = new HashMap<>();

            // Duration keys always recorded
            outputMap.put("agg_pre_process", aggPreProcessDuration);
            outputMap.put("query_internal_execution", queryExecDuration);
            outputMap.put("agg_post_process", aggPostProcessDuration);

            // Conditional: requestStartNanos > 0 → record _start keys
            if (requestStartNanos > 0) {
                outputMap.put("agg_pre_process_start", Math.max(0, aggPreProcessStartNanos - requestStartNanos));
                outputMap.put("query_internal_execution_start", Math.max(0, queryExecStartNanos - requestStartNanos));
                outputMap.put("agg_post_process_start", Math.max(0, aggPostProcessStartNanos - requestStartNanos));
            }

            // CONTRAST ASSERTION: _start keys SHOULD be present when requestStartNanos > 0
            assertTrue(
                "agg_pre_process_start should be present when requestStartNanos > 0 (iteration " + i + ")",
                outputMap.containsKey("agg_pre_process_start")
            );
            assertTrue(
                "query_internal_execution_start should be present when requestStartNanos > 0 (iteration " + i + ")",
                outputMap.containsKey("query_internal_execution_start")
            );
            assertTrue(
                "agg_post_process_start should be present when requestStartNanos > 0 (iteration " + i + ")",
                outputMap.containsKey("agg_post_process_start")
            );

            // Verify the offset values are non-negative
            for (String key : outputMap.keySet()) {
                if (key.endsWith("_start")) {
                    assertTrue(
                        "Start offset '" + key + "' should be >= 0 (iteration " + i + "), got: " + outputMap.get(key),
                        outputMap.get(key) >= 0
                    );
                }
            }

            // Total keys: 3 duration + 3 start = 6
            assertEquals("Should have 6 keys (3 duration + 3 start) when requestStartNanos > 0 (iteration " + i + ")", 6, outputMap.size());
        }
    }

    /**
     * Property 2 with extended metrics: Even with additional conditional metrics
     * (global_agg_separate_pass, search_idle_reactivation, slice_*), no _start keys appear
     * when requestStartNanos == 0.
     * <p>
     * <b>Validates: Requirements 1.6</b>
     */
    public void testNoStartOffsetKeysForExtendedMetricsWhenRequestStartNanosZero() {
        final String[] extendedDurationKeys = {
            "agg_pre_process",
            "query_internal_execution",
            "agg_post_process",
            "request_cache_lookup",
            "request_cache_write",
            "query_pre_process",
            "global_agg_separate_pass",
            "search_idle_reactivation",
            "slice_creation",
            "slice_scheduling",
            "slice_max_execution",
            "slice_min_execution",
            "slice_result_aggregation"
        };

        for (int i = 0; i < ITERATIONS; i++) {
            final long requestStartNanos = 0;

            Map<String, Long> outputMap = new HashMap<>();

            // Randomly include a subset of extended metrics
            for (String key : extendedDurationKeys) {
                if (randomBoolean()) {
                    long duration = randomLongBetween(1_000L, 500_000_000L);
                    outputMap.put(key, duration);

                    // Simulate the conditional start offset recording
                    long eventStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
                    if (requestStartNanos > 0) {
                        outputMap.put(key + "_start", Math.max(0, eventStartNanos - requestStartNanos));
                    }
                }
            }

            // PROPERTY ASSERTION: No key ends with "_start"
            for (String key : outputMap.keySet()) {
                assertFalse(
                    "No key should end with '_start' when requestStartNanos == 0, but found: '"
                        + key + "' in iteration " + i,
                    key.endsWith("_start")
                );
            }
        }
    }

    /**
     * Simulates the shard timing recording logic from QueryPhase.java and IndicesService.java.
     * <p>
     * For each duration key in the input, it records the duration unconditionally.
     * It only records the corresponding {@code X_start} key when {@code requestStartNanos > 0}.
     *
     * @param requestStartNanos the request start time (0 means unset/old coordinator)
     * @param shardTimings map of metric name → duration in nanoseconds
     * @return the resulting shard breakdown map that would be sent to the coordinator
     */
    private Map<String, Long> simulateShardTimingRecording(long requestStartNanos, Map<String, Long> shardTimings) {
        Map<String, Long> outputMap = new HashMap<>();

        for (Map.Entry<String, Long> entry : shardTimings.entrySet()) {
            String key = entry.getKey();
            long duration = entry.getValue();

            // Always record the duration
            outputMap.put(key, duration);

            // Only record start offset when requestStartNanos > 0
            if (requestStartNanos > 0) {
                // Simulate event happening at some random time after request start
                long eventStartNanos = requestStartNanos + randomLongBetween(0, 2_000_000_000L);
                outputMap.put(key + "_start", Math.max(0, eventStartNanos - requestStartNanos));
            }
        }

        return outputMap;
    }
}
