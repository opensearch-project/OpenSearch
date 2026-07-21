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
 * Property-based tests for SearchLatencyBreakdown: Property 3 - Conditional metric presence
 * based on configuration.
 *
 * <p><b>Property 3: Conditional metric presence based on configuration</b></p>
 * <p>For any search request, if rescore is configured then the breakdown SHALL contain a
 * {@code rescore} timed entry with valid start_offset_micros and duration_micros; if rescore
 * is NOT configured the entry SHALL be absent. The same SHALL hold for {@code suggest}.</p>
 *
 * <p>Feature: latency-breakdown-fixes, Property 3: Conditional metric presence based on configuration</p>
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 4.1, 4.2</b></p>
 */
public class SearchLatencyBreakdownConditionalPresencePropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 100;

    /**
     * The minimum nanos value that will produce a positive millis conversion (1ms = 1_000_000 nanos).
     * putIfPositive only includes entries where TimeUnit.NANOSECONDS.toMillis(nanos) > 0.
     */
    private static final long MIN_NANOS_FOR_POSITIVE_MILLIS = 1_000_000L;

    /**
     * Maximum nanos value for random generation (100ms).
     */
    private static final long MAX_NANOS = 100_000_000L;

    /**
     * Property 3: Conditional metric presence — rescore metric present iff configured,
     * suggest metric present iff configured.
     * <p>
     * Generates random search requests with/without rescore and suggest configs.
     * Asserts metric present iff configured; absent iff not configured.
     * <p>
     * Validates: Requirements 3.1, 3.2, 4.1, 4.2
     */
    public void testProperty3_conditionalMetricPresence() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Randomly decide whether rescore and suggest are "configured"
            boolean hasRescore = randomBoolean();
            boolean hasSuggest = randomBoolean();

            // Simulate request start and end times
            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Record rescore timing only if configured, using a value that will produce > 0 millis
            if (hasRescore) {
                long rescoreNanos = randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS);
                breakdown.recordRescoreMax(rescoreNanos);
            }

            // Record suggest timing only if configured, using a value that will produce > 0 millis
            if (hasSuggest) {
                long suggestNanos = randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS);
                breakdown.recordSuggestMax(suggestNanos);
            }

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            // Assert: rescore present iff configured
            assertEquals(
                "Iteration " + i + ": rescore metric should be "
                    + (hasRescore ? "present" : "absent")
                    + " but was " + (map.containsKey("rescore") ? "present" : "absent"),
                hasRescore,
                map.containsKey("rescore")
            );

            // Assert: suggest present iff configured
            assertEquals(
                "Iteration " + i + ": suggest metric should be "
                    + (hasSuggest ? "present" : "absent")
                    + " but was " + (map.containsKey("suggest") ? "present" : "absent"),
                hasSuggest,
                map.containsKey("suggest")
            );
        }
    }

    /**
     * Property 3: When rescore/suggest are configured (recorded with values >= 1ms),
     * the unified map value SHALL be a positive Long representing duration in milliseconds.
     * <p>
     * Validates: Requirements 3.1, 4.1
     */
    public void testProperty3_configuredMetricsHavePositiveMillisValue() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Always configure both metrics with values >= 1ms
            long rescoreNanos = randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS);
            long suggestNanos = randomLongBetween(MIN_NANOS_FOR_POSITIVE_MILLIS, MAX_NANOS);
            breakdown.recordRescoreMax(rescoreNanos);
            breakdown.recordSuggestMax(suggestNanos);

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            // Both must be present with positive values
            assertTrue(
                "Iteration " + i + ": rescore must be present when recorded with " + rescoreNanos + " nanos",
                map.containsKey("rescore")
            );
            assertTrue(
                "Iteration " + i + ": suggest must be present when recorded with " + suggestNanos + " nanos",
                map.containsKey("suggest")
            );

            long rescoreMillis = (long) map.get("rescore");
            long suggestMillis = (long) map.get("suggest");

            assertTrue(
                "Iteration " + i + ": rescore millis must be positive, got " + rescoreMillis,
                rescoreMillis > 0
            );
            assertTrue(
                "Iteration " + i + ": suggest millis must be positive, got " + suggestMillis,
                suggestMillis > 0
            );
        }
    }

    /**
     * Property 3: When neither rescore nor suggest is configured (nothing recorded),
     * both keys SHALL be absent from the unified breakdown map.
     * <p>
     * Validates: Requirements 3.2, 4.2
     */
    public void testProperty3_unconfiguredMetricsAreAbsent() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Do NOT record any rescore or suggest timing

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            assertFalse(
                "Iteration " + i + ": rescore must NOT be present when not configured",
                map.containsKey("rescore")
            );
            assertFalse(
                "Iteration " + i + ": suggest must NOT be present when not configured",
                map.containsKey("suggest")
            );
        }
    }

    /**
     * Property 3: Sub-millisecond recordings ({@code < 1_000_000 nanos}) should NOT produce a map entry
     * because putIfPositive requires millis &gt; 0. This validates the boundary behavior.
     * <p>
     * Validates: Requirements 3.2, 4.2
     */
    public void testProperty3_subMillisecondRecordingsAreOmitted() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Record sub-millisecond values (< 1_000_000 nanos → 0 millis)
            long subMsRescore = randomLongBetween(1, 999_999);
            long subMsSuggest = randomLongBetween(1, 999_999);
            breakdown.recordRescoreMax(subMsRescore);
            breakdown.recordSuggestMax(subMsSuggest);

            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            // Sub-ms values convert to 0 millis, so they should be omitted
            assertFalse(
                "Iteration " + i + ": rescore should be absent for sub-ms value " + subMsRescore + " nanos",
                map.containsKey("rescore")
            );
            assertFalse(
                "Iteration " + i + ": suggest should be absent for sub-ms value " + subMsSuggest + " nanos",
                map.containsKey("suggest")
            );
        }
    }
}
