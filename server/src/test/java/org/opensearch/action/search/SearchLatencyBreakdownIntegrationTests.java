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

/**
 * Integration tests for the end-to-end SearchLatencyBreakdown flow.
 * <p>
 * These tests simulate a realistic search request lifecycle by recording metrics
 * across the full breakdown path (pre-phase, phases, inter-phase gaps, reduce)
 * and verifying the final unified breakdown map output.
 * <p>
 * <b>Validates: Requirements 2.3, 11.1, 14.1</b>
 */
public class SearchLatencyBreakdownIntegrationTests extends OpenSearchTestCase {

    /**
     * Test that after recording metrics through the end-to-end flow,
     * toUnifiedBreakdownMap returns a non-empty map.
     */
    public void testBreakdownMapPresent() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Simulate absolute timestamps
        long absoluteStart = System.nanoTime();

        // Pre-phase: pipeline request transform
        long pipelineTransformNanos = TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordPipelineRequestTransform(pipelineTransformNanos);

        // Pre-phase: query rewrite
        long queryRewriteNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordQueryRewrite(queryRewriteNanos);

        // Pre-phase: shard routing
        long shardRoutingNanos = TimeUnit.MILLISECONDS.toNanos(1);
        breakdown.recordShardRouting(shardRoutingNanos);

        // Mark first phase start
        long firstPhaseStart = absoluteStart + pipelineTransformNanos + queryRewriteNanos + shardRoutingNanos;
        breakdown.markFirstPhaseStart(firstPhaseStart);

        // Query phase
        long queryPhaseNanos = TimeUnit.MILLISECONDS.toNanos(50);
        breakdown.recordQueryPhase(queryPhaseNanos);
        long queryPhaseEnd = firstPhaseStart + queryPhaseNanos;
        breakdown.markPhaseEnd("query", queryPhaseEnd);

        // Inter-phase gap: query → fetch
        long queryToFetchGapNanos = TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordInterPhaseGap("query", "fetch", queryToFetchGapNanos);

        // Fetch phase
        long fetchPhaseNanos = TimeUnit.MILLISECONDS.toNanos(20);
        breakdown.recordFetchPhase(fetchPhaseNanos);
        long fetchPhaseEnd = queryPhaseEnd + queryToFetchGapNanos + fetchPhaseNanos;
        breakdown.markPhaseEnd("fetch", fetchPhaseEnd);

        // Reduce
        long reduceTopDocsNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordReduceTopDocs(reduceTopDocsNanos);

        // Request end
        long requestEnd = fetchPhaseEnd + reduceTopDocsNanos + TimeUnit.MILLISECONDS.toNanos(2);

        // Get unified breakdown map
        Map<String, Object> breakdownMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // Verify map is non-empty
        assertFalse("Breakdown map should not be empty after recording metrics", breakdownMap.isEmpty());

        // Verify expected keys are present
        assertTrue("Expected 'pipeline_request_transform' in breakdown map", breakdownMap.containsKey("pipeline_request_transform"));
        assertTrue("Expected 'query_rewrite' in breakdown map", breakdownMap.containsKey("query_rewrite"));
        assertTrue("Expected 'shard_routing' in breakdown map", breakdownMap.containsKey("shard_routing"));
        assertTrue("Expected 'query' in breakdown map", breakdownMap.containsKey("query"));
        assertTrue("Expected 'fetch' in breakdown map", breakdownMap.containsKey("fetch"));
        assertTrue("Expected 'query_to_fetch_gap' in breakdown map", breakdownMap.containsKey("query_to_fetch_gap"));
        assertTrue("Expected 'reduce_top_docs' in breakdown map", breakdownMap.containsKey("reduce_top_docs"));
    }

    /**
     * Test that phase latency values recorded in the breakdown match
     * the corresponding entries in the unified breakdown map.
     */
    public void testPhaseLatencyMapMatchesBreakdown() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = System.nanoTime();

        // Record pre-phase metrics
        long prePhaseNanos = TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordQueryRewrite(prePhaseNanos);

        // Mark first phase start
        long firstPhaseStart = absoluteStart + prePhaseNanos;
        breakdown.markFirstPhaseStart(firstPhaseStart);

        // Record phase durations with specific millis values for easy verification
        long canMatchNanos = TimeUnit.MILLISECONDS.toNanos(10);
        long queryNanos = TimeUnit.MILLISECONDS.toNanos(45);
        long fetchNanos = TimeUnit.MILLISECONDS.toNanos(25);

        breakdown.recordCanMatchPhase(canMatchNanos);
        long canMatchEnd = firstPhaseStart + canMatchNanos;
        breakdown.markPhaseEnd("can_match", canMatchEnd);

        // Inter-phase gap: can_match → query
        long canMatchToQueryGapNanos = TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordInterPhaseGap("can_match", "query", canMatchToQueryGapNanos);

        breakdown.recordQueryPhase(queryNanos);
        long queryEnd = canMatchEnd + canMatchToQueryGapNanos + queryNanos;
        breakdown.markPhaseEnd("query", queryEnd);

        // Inter-phase gap: query → fetch
        long queryToFetchGapNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordInterPhaseGap("query", "fetch", queryToFetchGapNanos);

        breakdown.recordFetchPhase(fetchNanos);
        long fetchEnd = queryEnd + queryToFetchGapNanos + fetchNanos;
        breakdown.markPhaseEnd("fetch", fetchEnd);

        // Post-phase reduce
        long reduceNanos = TimeUnit.MILLISECONDS.toNanos(4);
        breakdown.recordReduceTopDocs(reduceNanos);

        long requestEnd = fetchEnd + reduceNanos + TimeUnit.MILLISECONDS.toNanos(1);

        // Get breakdown map
        Map<String, Object> breakdownMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // Verify phase durations match exactly what was recorded (nanos → millis)
        assertEquals("can_match phase should be 10ms", 10L, (long) breakdownMap.get("can_match"));
        assertEquals("query phase should be 45ms", 45L, (long) breakdownMap.get("query"));
        assertEquals("fetch phase should be 25ms", 25L, (long) breakdownMap.get("fetch"));

        // Verify inter-phase gaps match
        assertEquals("can_match_to_query_gap should be 2ms", 2L, (long) breakdownMap.get("can_match_to_query_gap"));
        assertEquals("query_to_fetch_gap should be 3ms", 3L, (long) breakdownMap.get("query_to_fetch_gap"));

        // Verify reduce metric matches
        assertEquals("reduce_top_docs should be 4ms", 4L, (long) breakdownMap.get("reduce_top_docs"));

        // Verify pre-phase overhead is present (time between absoluteStart and firstPhaseStart)
        long prePhaseOverheadMillis = TimeUnit.NANOSECONDS.toMillis(prePhaseNanos);
        assertEquals("pre_phase_overhead should be 5ms", prePhaseOverheadMillis, (long) breakdownMap.get("pre_phase_overhead"));
    }

    /**
     * Test that the sum of top-level non-overlapping breakdown entries approximates
     * the total request duration (took). This simulates a realistic end-to-end flow.
     * <p>
     * Top-level non-overlapping entries are: pre_phase_overhead, phase durations,
     * inter-phase gaps, and post_phase_overhead. Their sum should approximate took.
     */
    public void testSumApproximatesTook() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Use a realistic timeline with known durations
        long absoluteStart = 1_000_000_000L; // arbitrary base nanoTime

        // Pre-phase operations (5ms total)
        long pipelineTransformNanos = TimeUnit.MILLISECONDS.toNanos(2);
        long queryRewriteNanos = TimeUnit.MILLISECONDS.toNanos(2);
        long shardRoutingNanos = TimeUnit.MILLISECONDS.toNanos(1);
        breakdown.recordPipelineRequestTransform(pipelineTransformNanos);
        breakdown.recordQueryRewrite(queryRewriteNanos);
        breakdown.recordShardRouting(shardRoutingNanos);

        // First phase starts after pre-phase (5ms into request)
        long prePhaseTotal = pipelineTransformNanos + queryRewriteNanos + shardRoutingNanos;
        long firstPhaseStart = absoluteStart + prePhaseTotal;
        breakdown.markFirstPhaseStart(firstPhaseStart);

        // Query phase: 50ms
        long queryPhaseNanos = TimeUnit.MILLISECONDS.toNanos(50);
        breakdown.recordQueryPhase(queryPhaseNanos);
        long queryPhaseEnd = firstPhaseStart + queryPhaseNanos;
        breakdown.markPhaseEnd("query", queryPhaseEnd);

        // Inter-phase gap query → fetch: 3ms
        long queryToFetchGapNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordInterPhaseGap("query", "fetch", queryToFetchGapNanos);

        // Fetch phase: 20ms
        long fetchPhaseNanos = TimeUnit.MILLISECONDS.toNanos(20);
        breakdown.recordFetchPhase(fetchPhaseNanos);
        long fetchPhaseEnd = queryPhaseEnd + queryToFetchGapNanos + fetchPhaseNanos;
        breakdown.markPhaseEnd("fetch", fetchPhaseEnd);

        // Post-phase reduce: 4ms
        long reduceTopDocsNanos = TimeUnit.MILLISECONDS.toNanos(3);
        long reduceAggsNanos = TimeUnit.MILLISECONDS.toNanos(1);
        breakdown.recordReduceTopDocs(reduceTopDocsNanos);
        breakdown.recordReduceAggregations(reduceAggsNanos);

        // Post-phase overhead: 2ms (response serialization, pipeline response transform)
        long responseSerNanos = TimeUnit.MILLISECONDS.toNanos(1);
        long pipelineResponseNanos = TimeUnit.MILLISECONDS.toNanos(1);
        breakdown.recordResponseSerialization(responseSerNanos);
        breakdown.recordPipelineResponseTransform(pipelineResponseNanos);

        // Request ends after all post-phase work (reduce + response handling)
        long postPhaseWork = reduceTopDocsNanos + reduceAggsNanos + responseSerNanos + pipelineResponseNanos;
        long requestEnd = fetchPhaseEnd + postPhaseWork;

        // Get the breakdown map
        Map<String, Object> breakdownMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // Calculate total "took" in millis
        long tookNanos = requestEnd - absoluteStart;
        long tookMillis = TimeUnit.NANOSECONDS.toMillis(tookNanos);

        // Sum the top-level non-overlapping entries that should account for total request time:
        // pre_phase_overhead + phases + inter-phase gaps + post_phase_overhead
        long sumTopLevel = 0;

        // Pre-phase overhead (time from absoluteStart to firstPhaseStart)
        if (breakdownMap.containsKey("pre_phase_overhead")) {
            sumTopLevel += (Long) breakdownMap.get("pre_phase_overhead");
        }

        // Phase durations
        if (breakdownMap.containsKey("query")) {
            sumTopLevel += (Long) breakdownMap.get("query");
        }
        if (breakdownMap.containsKey("fetch")) {
            sumTopLevel += (Long) breakdownMap.get("fetch");
        }

        // Inter-phase gaps
        if (breakdownMap.containsKey("query_to_fetch_gap")) {
            sumTopLevel += (Long) breakdownMap.get("query_to_fetch_gap");
        }

        // Post-phase overhead (time from lastPhaseEnd to requestEnd)
        if (breakdownMap.containsKey("post_phase_overhead")) {
            sumTopLevel += (Long) breakdownMap.get("post_phase_overhead");
        }

        // The sum of top-level entries should approximate took.
        // Allow a tolerance of 1ms due to nanos-to-millis truncation at each entry.
        // Each entry loses up to 999,999 nanos during truncation, so with ~5 entries
        // the maximum cumulative loss is ~5ms. We use took as upper bound.
        assertTrue(
            "Sum of top-level entries (" + sumTopLevel + "ms) should approximate took (" + tookMillis + "ms). "
                + "Difference should be within 5ms due to truncation.",
            Math.abs(tookMillis - sumTopLevel) <= 5
        );

        // Also verify the sum doesn't exceed took (entries are sub-components of took)
        assertTrue(
            "Sum of top-level entries (" + sumTopLevel + "ms) should not exceed took (" + tookMillis + "ms)",
            sumTopLevel <= tookMillis
        );
    }
}
