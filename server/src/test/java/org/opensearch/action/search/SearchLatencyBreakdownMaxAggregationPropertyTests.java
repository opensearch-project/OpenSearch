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
import java.util.function.BiConsumer;

/**
 * Property-based tests for {@link SearchLatencyBreakdown} max-aggregation across shards.
 * <p>
 * Feature: latency-breakdown-fixes, Property 4: Max-aggregation selects maximum across shards
 * <p>
 * For any non-empty set of per-shard nanosecond values for a given metric (rescore, suggest,
 * network_roundtrip_query, network_roundtrip_fetch, request_serialization, response_deserialization),
 * the coordinator-reported value SHALL equal the maximum of all shard values.
 * <p>
 * <b>Validates: Requirements 3.3, 4.3, 5.6, 7.3, 7.4</b>
 */
public class SearchLatencyBreakdownMaxAggregationPropertyTests extends OpenSearchTestCase {

    private static final int MIN_ITERATIONS = 100;

    /**
     * Max-aggregation recording methods targeted by this property, paired with their unified map keys.
     * These are the metrics added/modified in the latency-breakdown-fixes spec that use
     * accumulateAndGet(nanos, Math::max) to merge per-shard values at the coordinator.
     */
    private static final Object[][] MAX_AGGREGATION_METHODS = {
        // Rescore (Requirement 3.3)
        { (BiConsumer<SearchLatencyBreakdown, Long>) SearchLatencyBreakdown::recordRescoreMax, "rescore" },
        // Suggest (Requirement 4.3)
        { (BiConsumer<SearchLatencyBreakdown, Long>) SearchLatencyBreakdown::recordSuggestMax, "suggest" },
        // Network roundtrip query (Requirement 7.3 - max across shard requests)
        { (BiConsumer<SearchLatencyBreakdown, Long>) SearchLatencyBreakdown::recordNetworkRoundtripQuery,
            "network_roundtrip_query" },
        // Network roundtrip fetch (Requirement 7.4 - max across shard responses)
        { (BiConsumer<SearchLatencyBreakdown, Long>) SearchLatencyBreakdown::recordNetworkRoundtripFetch,
            "network_roundtrip_fetch" },
    };

    /**
     * Property 4: Max-aggregation selects maximum across shards — all targeted methods.
     * <p>
     * Generate random shard counts (1-100), random nanos values per shard.
     * Assert coordinator-reported value equals maximum of all shard values.
     * <p>
     * Values are generated as multiples of 1_000_000 (1ms) to ensure lossless nanos→millis conversion.
     */
    @SuppressWarnings("unchecked")
    public void testProperty4_maxAggregationSelectsMaximumAcrossShards() {
        for (int iter = 0; iter < MIN_ITERATIONS; iter++) {
            // Pick a random max-aggregation method to test
            int methodIndex = randomIntBetween(0, MAX_AGGREGATION_METHODS.length - 1);
            BiConsumer<SearchLatencyBreakdown, Long> recorder =
                (BiConsumer<SearchLatencyBreakdown, Long>) MAX_AGGREGATION_METHODS[methodIndex][0];
            String mapKey = (String) MAX_AGGREGATION_METHODS[methodIndex][1];

            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random shard count (1-100)
            int shardCount = randomIntBetween(1, 100);
            long expectedMax = 0;

            for (int s = 0; s < shardCount; s++) {
                // Generate values in millis then convert to nanos for lossless conversion
                long millis = randomLongBetween(1, 100_000);
                long nanos = millis * 1_000_000L;
                recorder.accept(breakdown, nanos);
                expectedMax = Math.max(expectedMax, nanos);
            }

            // Verify via unified breakdown map
            long absoluteStart = randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedMax);
            assertTrue(
                "Iteration " + iter + ": '" + mapKey + "' should be present in map for shardCount=" + shardCount,
                map.containsKey(mapKey)
            );
            assertEquals(
                "Iteration " + iter + ": Max-aggregation property violated for '" + mapKey + "'. "
                    + "Expected max=" + expectedMillis + "ms across " + shardCount + " shards",
                expectedMillis,
                ((Number) map.get(mapKey)).longValue()
            );
        }
    }

    /**
     * Property 4: Max-aggregation for rescore specifically.
     * <p>
     * Generates random shard counts and per-shard nanos values, records them via recordRescoreMax,
     * and asserts the coordinator-reported value in the unified map equals the maximum.
     * <p>
     * <b>Validates: Requirement 3.3</b>
     */
    public void testProperty4_rescoreMaxAggregation() {
        for (int iter = 0; iter < MIN_ITERATIONS; iter++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            int shardCount = randomIntBetween(1, 100);
            long expectedMax = 0;

            for (int s = 0; s < shardCount; s++) {
                long nanos = randomLongBetween(1_000_000, 100_000_000_000L);
                breakdown.recordRescoreMax(nanos);
                expectedMax = Math.max(expectedMax, nanos);
            }

            long absoluteStart = randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedMax);
            assertTrue(
                "Iteration " + iter + ": 'rescore' should be present for shardCount=" + shardCount,
                map.containsKey("rescore")
            );
            assertEquals(
                "Iteration " + iter + ": rescore max-aggregation violated. Expected=" + expectedMillis + "ms, shards=" + shardCount,
                expectedMillis,
                ((Number) map.get("rescore")).longValue()
            );
        }
    }

    /**
     * Property 4: Max-aggregation for suggest specifically.
     * <p>
     * Generates random shard counts and per-shard nanos values, records them via recordSuggestMax,
     * and asserts the coordinator-reported value in the unified map equals the maximum.
     * <p>
     * <b>Validates: Requirement 4.3</b>
     */
    public void testProperty4_suggestMaxAggregation() {
        for (int iter = 0; iter < MIN_ITERATIONS; iter++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            int shardCount = randomIntBetween(1, 100);
            long expectedMax = 0;

            for (int s = 0; s < shardCount; s++) {
                long nanos = randomLongBetween(1_000_000, 100_000_000_000L);
                breakdown.recordSuggestMax(nanos);
                expectedMax = Math.max(expectedMax, nanos);
            }

            long absoluteStart = randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedMax);
            assertTrue(
                "Iteration " + iter + ": 'suggest' should be present for shardCount=" + shardCount,
                map.containsKey("suggest")
            );
            assertEquals(
                "Iteration " + iter + ": suggest max-aggregation violated. Expected=" + expectedMillis + "ms, shards=" + shardCount,
                expectedMillis,
                ((Number) map.get("suggest")).longValue()
            );
        }
    }

    /**
     * Property 4: Max-aggregation for network roundtrip query specifically.
     * <p>
     * Generates random shard counts and per-shard nanos values, records them via
     * recordNetworkRoundtripQuery, and asserts the coordinator-reported value equals the maximum.
     * <p>
     * <b>Validates: Requirement 7.3</b>
     */
    public void testProperty4_networkRoundtripQueryMaxAggregation() {
        for (int iter = 0; iter < MIN_ITERATIONS; iter++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            int shardCount = randomIntBetween(1, 100);
            long expectedMax = 0;

            for (int s = 0; s < shardCount; s++) {
                long nanos = randomLongBetween(1_000_000, 100_000_000_000L);
                breakdown.recordNetworkRoundtripQuery(nanos);
                expectedMax = Math.max(expectedMax, nanos);
            }

            long absoluteStart = randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedMax);
            assertTrue(
                "Iteration " + iter + ": 'network_roundtrip_query' should be present for shardCount=" + shardCount,
                map.containsKey("network_roundtrip_query")
            );
            assertEquals(
                "Iteration " + iter + ": network_roundtrip_query max-aggregation violated. Expected=" + expectedMillis
                    + "ms, shards=" + shardCount,
                expectedMillis,
                ((Number) map.get("network_roundtrip_query")).longValue()
            );
        }
    }

    /**
     * Property 4: Max-aggregation for network roundtrip fetch specifically.
     * <p>
     * Generates random shard counts and per-shard nanos values, records them via
     * recordNetworkRoundtripFetch, and asserts the coordinator-reported value equals the maximum.
     * <p>
     * <b>Validates: Requirement 7.4</b>
     */
    public void testProperty4_networkRoundtripFetchMaxAggregation() {
        for (int iter = 0; iter < MIN_ITERATIONS; iter++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            int shardCount = randomIntBetween(1, 100);
            long expectedMax = 0;

            for (int s = 0; s < shardCount; s++) {
                long nanos = randomLongBetween(1_000_000, 100_000_000_000L);
                breakdown.recordNetworkRoundtripFetch(nanos);
                expectedMax = Math.max(expectedMax, nanos);
            }

            long absoluteStart = randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedMax);
            assertTrue(
                "Iteration " + iter + ": 'network_roundtrip_fetch' should be present for shardCount=" + shardCount,
                map.containsKey("network_roundtrip_fetch")
            );
            assertEquals(
                "Iteration " + iter + ": network_roundtrip_fetch max-aggregation violated. Expected=" + expectedMillis
                    + "ms, shards=" + shardCount,
                expectedMillis,
                ((Number) map.get("network_roundtrip_fetch")).longValue()
            );
        }
    }

    /**
     * Property 4: Max-aggregation with known maximum at random position.
     * <p>
     * Places a known maximum value at a random position in the shard sequence.
     * All other shards have values strictly less than the maximum.
     * Verifies the coordinator correctly identifies the maximum regardless of position.
     * <p>
     * <b>Validates: Requirements 3.3, 4.3, 5.6, 7.3, 7.4</b>
     */
    @SuppressWarnings("unchecked")
    public void testProperty4_maxAtRandomPosition() {
        for (int iter = 0; iter < MIN_ITERATIONS; iter++) {
            int methodIndex = randomIntBetween(0, MAX_AGGREGATION_METHODS.length - 1);
            BiConsumer<SearchLatencyBreakdown, Long> recorder =
                (BiConsumer<SearchLatencyBreakdown, Long>) MAX_AGGREGATION_METHODS[methodIndex][0];
            String mapKey = (String) MAX_AGGREGATION_METHODS[methodIndex][1];

            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            int shardCount = randomIntBetween(2, 100);
            int maxPosition = randomIntBetween(0, shardCount - 1);

            // Known maximum (large value in millisecond-aligned nanos)
            long knownMaxMillis = randomLongBetween(1000, 100_000);
            long knownMaxNanos = knownMaxMillis * 1_000_000L;

            for (int s = 0; s < shardCount; s++) {
                if (s == maxPosition) {
                    recorder.accept(breakdown, knownMaxNanos);
                } else {
                    // Values strictly less than the known max
                    long lesserMillis = randomLongBetween(1, knownMaxMillis - 1);
                    recorder.accept(breakdown, lesserMillis * 1_000_000L);
                }
            }

            long absoluteStart = randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            assertTrue(
                "Iteration " + iter + ": '" + mapKey + "' should be present",
                map.containsKey(mapKey)
            );
            assertEquals(
                "Iteration " + iter + ": Max at position " + maxPosition + "/" + shardCount
                    + " not correctly identified for '" + mapKey + "'",
                knownMaxMillis,
                ((Number) map.get(mapKey)).longValue()
            );
        }
    }

    /**
     * Property 4: Max-aggregation with all identical values.
     * <p>
     * When all shards report the same value, the max equals that value.
     * This validates the idempotent property of max-aggregation with duplicate inputs.
     * <p>
     * <b>Validates: Requirements 3.3, 4.3, 5.6, 7.3, 7.4</b>
     */
    @SuppressWarnings("unchecked")
    public void testProperty4_allIdenticalValues() {
        for (int iter = 0; iter < MIN_ITERATIONS; iter++) {
            int methodIndex = randomIntBetween(0, MAX_AGGREGATION_METHODS.length - 1);
            BiConsumer<SearchLatencyBreakdown, Long> recorder =
                (BiConsumer<SearchLatencyBreakdown, Long>) MAX_AGGREGATION_METHODS[methodIndex][0];
            String mapKey = (String) MAX_AGGREGATION_METHODS[methodIndex][1];

            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            int shardCount = randomIntBetween(1, 100);
            long identicalMillis = randomLongBetween(1, 100_000);
            long identicalNanos = identicalMillis * 1_000_000L;

            for (int s = 0; s < shardCount; s++) {
                recorder.accept(breakdown, identicalNanos);
            }

            long absoluteStart = randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            assertTrue(
                "Iteration " + iter + ": '" + mapKey + "' should be present",
                map.containsKey(mapKey)
            );
            assertEquals(
                "Iteration " + iter + ": All identical values should report that value for '" + mapKey + "'",
                identicalMillis,
                ((Number) map.get(mapKey)).longValue()
            );
        }
    }
}
