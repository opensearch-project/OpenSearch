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
 * Unit tests for coordinator-level instrumentation of SearchLatencyBreakdown.
 * <p>
 * Verifies that all recording methods accept non-negative values, inter-phase gap
 * computation works correctly with controlled timestamps, reduce metrics record
 * correctly, queue wait recording works, and network round-trip max/avg logic
 * functions as expected.
 * <p>
 * <b>Validates: Requirements 1.1–1.6, 2.1–2.5, 3.1–3.4, 4.1, 10.1–10.4</b>
 */
public class CoordinatorInstrumentationTests extends OpenSearchTestCase {

    // ========== 1. Recording methods accept non-negative values ==========

    /**
     * Verifies that all pre-phase coordinator recording methods accept non-negative values
     * and the unified breakdown map reflects them correctly.
     * <p>
     * Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6
     */
    public void testPrePhaseRecordingMethods_acceptNonNegativeValues() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Record non-negative values for all pre-phase metrics (use values >= 1ms in nanos)
        long pipelineNanos = 5_000_000L;   // 5ms
        long queryRewriteNanos = 8_000_000L; // 8ms
        long indexResNanos = 2_000_000L;   // 2ms
        long shardRoutingNanos = 3_000_000L; // 3ms
        long termsLookupNanos = 1_000_000L; // 1ms

        breakdown.recordPipelineRequestTransform(pipelineNanos);
        breakdown.recordQueryRewrite(queryRewriteNanos);
        breakdown.recordIndexResolution(indexResNanos);
        breakdown.recordShardRouting(shardRoutingNanos);
        breakdown.recordTermsLookupSubQuery(termsLookupNanos);

        // Set up timestamps for unified map
        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        // Mark first phase start so pre_phase_overhead is computed
        breakdown.markFirstPhaseStart(absoluteStart + 20_000_000L);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals(TimeUnit.NANOSECONDS.toMillis(pipelineNanos), (long) map.get("pipeline_request_transform"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(queryRewriteNanos), (long) map.get("query_rewrite"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(indexResNanos), (long) map.get("index_resolution"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(shardRoutingNanos), (long) map.get("shard_routing"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(termsLookupNanos), (long) map.get("terms_lookup_sub_query"));
    }

    /**
     * Verifies that recording zero values does not produce entries in the unified map.
     * <p>
     * Validates: Requirements 1.1–1.6 (zero-handling behavior)
     */
    public void testRecordingZeroValues_omittedFromUnifiedMap() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Record zero for all coordinator metrics
        breakdown.recordPipelineRequestTransform(0);
        breakdown.recordQueryRewrite(0);
        breakdown.recordIndexResolution(0);
        breakdown.recordShardRouting(0);
        breakdown.recordTermsLookupSubQuery(0);
        breakdown.recordCoordinatorQueueWait(0);
        breakdown.recordReduceTopDocs(0);
        breakdown.recordReduceAggregations(0);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 100_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // All these keys should be absent
        assertFalse(map.containsKey("pipeline_request_transform"));
        assertFalse(map.containsKey("query_rewrite"));
        assertFalse(map.containsKey("index_resolution"));
        assertFalse(map.containsKey("shard_routing"));
        assertFalse(map.containsKey("terms_lookup_sub_query"));
        assertFalse(map.containsKey("coordinator_queue_wait"));
        assertFalse(map.containsKey("reduce_top_docs"));
        assertFalse(map.containsKey("reduce_aggregations"));
    }

    // ========== 2. Inter-phase gap computation with controlled timestamps ==========

    /**
     * Simulates the full phase lifecycle with controlled timestamps:
     * markFirstPhaseStart → markPhaseEnd("can_match") → recordInterPhaseGap("can_match","query")
     * → markPhaseEnd("query") → recordInterPhaseGap("query","fetch")
     * and verifies the unified breakdown map contains correct gap values.
     * <p>
     * Validates: Requirements 3.1, 3.2, 3.3
     */
    public void testInterPhaseGapComputation_fullLifecycle() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Controlled timestamps (all in nanos)
        long absoluteStart = 1_000_000_000L;   // t0: request start
        long canMatchStart = absoluteStart + 20_000_000L;  // t1: can_match starts (20ms after request)
        long canMatchEnd = canMatchStart + 5_000_000L;     // t2: can_match ends (5ms duration)
        long queryStart = canMatchEnd + 12_000_000L;       // t3: query starts (12ms gap)
        long queryEnd = queryStart + 40_000_000L;          // t4: query ends (40ms duration)
        long fetchStart = queryEnd + 8_000_000L;           // t5: fetch starts (8ms gap)
        long fetchEnd = fetchStart + 15_000_000L;          // t6: fetch ends (15ms duration)
        long requestEnd = fetchEnd + 4_000_000L;           // t7: request ends (4ms post overhead)

        // Simulate the phase lifecycle as the coordinator would
        breakdown.markFirstPhaseStart(canMatchStart);
        breakdown.recordCanMatchPhase(canMatchEnd - canMatchStart);
        breakdown.markPhaseEnd("can_match", canMatchEnd);

        // Record can_match → query gap
        breakdown.recordInterPhaseGap("can_match", "query", queryStart - canMatchEnd);

        breakdown.recordQueryPhase(queryEnd - queryStart);
        breakdown.markPhaseEnd("query", queryEnd);

        // Record query → fetch gap
        breakdown.recordInterPhaseGap("query", "fetch", fetchStart - queryEnd);

        breakdown.recordFetchPhase(fetchEnd - fetchStart);
        breakdown.markPhaseEnd("fetch", fetchEnd);

        // Get the unified breakdown map
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // Verify inter-phase gaps
        assertEquals("can_match_to_query_gap should be 12ms",
            12L, (long) map.get("can_match_to_query_gap"));
        assertEquals("query_to_fetch_gap should be 8ms",
            8L, (long) map.get("query_to_fetch_gap"));

        // Verify phase durations
        assertEquals("can_match should be 5ms",
            5L, (long) map.get("can_match"));
        assertEquals("query should be 40ms",
            40L, (long) map.get("query"));
        assertEquals("fetch should be 15ms",
            15L, (long) map.get("fetch"));

        // Verify pre/post phase overhead
        assertEquals("pre_phase_overhead should be 20ms",
            20L, (long) map.get("pre_phase_overhead"));
        assertEquals("post_phase_overhead should be 4ms",
            4L, (long) map.get("post_phase_overhead"));
    }

    /**
     * Verifies the fetch → expand gap is correctly computed when an expand phase follows fetch.
     * <p>
     * Validates: Requirements 3.3
     */
    public void testFetchToExpandGap_withControlledTimestamps() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;
        long queryStart = absoluteStart + 10_000_000L;
        long queryEnd = queryStart + 30_000_000L;
        long fetchStart = queryEnd + 5_000_000L;
        long fetchEnd = fetchStart + 20_000_000L;
        long expandStart = fetchEnd + 7_000_000L;
        long expandEnd = expandStart + 3_000_000L;
        long requestEnd = expandEnd + 2_000_000L;

        breakdown.markFirstPhaseStart(queryStart);
        breakdown.recordQueryPhase(queryEnd - queryStart);
        breakdown.markPhaseEnd("query", queryEnd);
        breakdown.recordInterPhaseGap("query", "fetch", fetchStart - queryEnd);
        breakdown.recordFetchPhase(fetchEnd - fetchStart);
        breakdown.markPhaseEnd("fetch", fetchEnd);
        breakdown.recordInterPhaseGap("fetch", "expand", expandStart - fetchEnd);
        breakdown.recordExpandPhase(expandEnd - expandStart);
        breakdown.markPhaseEnd("expand", expandEnd);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("query_to_fetch_gap should be 5ms",
            5L, (long) map.get("query_to_fetch_gap"));
        assertEquals("fetch_to_expand_gap should be 7ms",
            7L, (long) map.get("fetch_to_expand_gap"));
        assertEquals("expand should be 3ms",
            3L, (long) map.get("expand"));
    }

    /**
     * Verifies that markFirstPhaseStart is idempotent — only the first call sets the value.
     * <p>
     * Validates: Requirements 2.4
     */
    public void testMarkFirstPhaseStart_isIdempotent() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long firstStart = 1_000_000_000L;
        long secondStart = 2_000_000_000L;

        breakdown.markFirstPhaseStart(firstStart);
        breakdown.markFirstPhaseStart(secondStart);

        assertEquals("firstPhaseStartNanos should remain the first recorded value",
            firstStart, breakdown.getFirstPhaseStartNanos());
    }

    /**
     * Verifies that markPhaseEnd updates the last phase end time and name.
     * <p>
     * Validates: Requirements 2.5
     */
    public void testMarkPhaseEnd_updatesLastPhaseEndAndName() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long firstEnd = 1_000_000_000L;
        long secondEnd = 2_000_000_000L;

        breakdown.markPhaseEnd("can_match", firstEnd);
        assertEquals("can_match", breakdown.getLastCompletedPhaseName());
        assertEquals(firstEnd, breakdown.getLastPhaseEndNanos());

        breakdown.markPhaseEnd("query", secondEnd);
        assertEquals("query", breakdown.getLastCompletedPhaseName());
        assertEquals(secondEnd, breakdown.getLastPhaseEndNanos());
    }

    // ========== 3. Reduce metric recording ==========

    /**
     * Verifies that all reduce metrics record correctly and appear in the unified map.
     * <p>
     * Validates: Requirements 10.1, 10.2, 10.3, 10.4
     */
    public void testReduceMetrics_recordAndAppearInUnifiedMap() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long reduceTopDocsNanos = 4_000_000L;    // 4ms
        long reduceAggsNanos = 3_000_000L;       // 3ms
        long reducePipelineNanos = 2_000_000L;   // 2ms
        long reduceSuggestNanos = 1_000_000L;    // 1ms

        breakdown.recordReduceTopDocs(reduceTopDocsNanos);
        breakdown.recordReduceAggregations(reduceAggsNanos);
        breakdown.recordReducePipelineAggs(reducePipelineNanos);
        breakdown.recordReduceSuggestions(reduceSuggestNanos);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals(4L, (long) map.get("reduce_top_docs"));
        assertEquals(3L, (long) map.get("reduce_aggregations"));
        assertEquals(2L, (long) map.get("reduce_pipeline_aggs"));
        assertEquals(1L, (long) map.get("reduce_suggestions"));
    }

    /**
     * Verifies that reduce metrics accumulate additively when recorded multiple times.
     * <p>
     * Validates: Requirements 10.1, 10.2
     */
    public void testReduceMetrics_accumulateAdditively() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Record multiple times to simulate partial reduce steps
        breakdown.recordReduceTopDocs(5_000_000L);
        breakdown.recordReduceTopDocs(3_000_000L);
        breakdown.recordReduceAggregations(4_000_000L);
        breakdown.recordReduceAggregations(2_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("reduce_top_docs should sum to 8ms",
            8L, (long) map.get("reduce_top_docs"));
        assertEquals("reduce_aggregations should sum to 6ms",
            6L, (long) map.get("reduce_aggregations"));
    }

    // ========== 4. Queue wait recording ==========

    /**
     * Verifies that coordinator queue wait records correctly.
     * <p>
     * Validates: Requirements 4.1
     */
    public void testCoordinatorQueueWait_recordsCorrectly() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long queueWaitNanos = 35_000_000L; // 35ms
        breakdown.recordCoordinatorQueueWait(queueWaitNanos);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("coordinator_queue_wait should be 35ms",
            35L, (long) map.get("coordinator_queue_wait"));
    }

    /**
     * Verifies that coordinator queue wait accumulates additively (e.g., retry scenario).
     * <p>
     * Validates: Requirements 4.1
     */
    public void testCoordinatorQueueWait_accumulatesAdditively() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        breakdown.recordCoordinatorQueueWait(10_000_000L);
        breakdown.recordCoordinatorQueueWait(5_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("coordinator_queue_wait should sum to 15ms",
            15L, (long) map.get("coordinator_queue_wait"));
    }

    // ========== 5. Network round-trip recording ==========

    /**
     * Verifies that recordNetworkRoundtripMax takes the maximum value across
     * multiple recordings (simulating multiple shard responses).
     * <p>
     * Validates: Requirements 8.1
     */
    public void testNetworkRoundtripMax_takesMaximum() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Simulate network round-trips from multiple shards
        breakdown.recordNetworkRoundtripMax(10_000_000L);  // 10ms
        breakdown.recordNetworkRoundtripMax(25_000_000L);  // 25ms (max)
        breakdown.recordNetworkRoundtripMax(15_000_000L);  // 15ms
        breakdown.recordNetworkRoundtripMax(5_000_000L);   // 5ms

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("network_roundtrip_max should be the maximum: 25ms",
            25L, (long) map.get("network_roundtrip_max"));
    }

    /**
     * Verifies that recordNetworkRoundtripAvg sets the value (overwrites previous).
     * <p>
     * Validates: Requirements 8.2
     */
    public void testNetworkRoundtripAvg_setsValue() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // The average is computed by the coordinator and set once
        breakdown.recordNetworkRoundtripAvg(12_000_000L); // 12ms

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("network_roundtrip_avg should be 12ms",
            12L, (long) map.get("network_roundtrip_avg"));

        // Setting again overwrites (not additive)
        breakdown.recordNetworkRoundtripAvg(18_000_000L);
        map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);
        assertEquals("network_roundtrip_avg should be overwritten to 18ms",
            18L, (long) map.get("network_roundtrip_avg"));
    }

    /**
     * Verifies network round-trip max with a single shard (edge case).
     * <p>
     * Validates: Requirements 8.1
     */
    public void testNetworkRoundtripMax_singleShard() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        breakdown.recordNetworkRoundtripMax(20_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("network_roundtrip_max with single shard should be 20ms",
            20L, (long) map.get("network_roundtrip_max"));
    }

    // ========== 6. Pre-phase and post-phase overhead with controlled timestamps ==========

    /**
     * Verifies pre-phase overhead is computed correctly as firstPhaseStart - absoluteStart.
     * <p>
     * Validates: Requirements 2.4
     */
    public void testPrePhaseOverhead_computedCorrectly() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;
        long firstPhaseStart = absoluteStart + 18_000_000L; // 18ms of pre-phase work

        breakdown.markFirstPhaseStart(firstPhaseStart);

        long preOverhead = breakdown.computePrePhaseOverhead(absoluteStart);
        assertEquals("Pre-phase overhead should be 18ms in nanos",
            18_000_000L, preOverhead);

        // Verify it shows up in the unified map
        long requestEnd = firstPhaseStart + 100_000_000L;
        breakdown.markPhaseEnd("query", requestEnd - 10_000_000L);
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);
        assertEquals("pre_phase_overhead in map should be 18ms",
            18L, (long) map.get("pre_phase_overhead"));
    }

    /**
     * Verifies post-phase overhead is computed correctly as requestEnd - lastPhaseEnd.
     * <p>
     * Validates: Requirements 2.5
     */
    public void testPostPhaseOverhead_computedCorrectly() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;
        long lastPhaseEnd = absoluteStart + 100_000_000L;
        long requestEnd = lastPhaseEnd + 4_000_000L; // 4ms of post-phase work

        breakdown.markFirstPhaseStart(absoluteStart + 5_000_000L);
        breakdown.markPhaseEnd("fetch", lastPhaseEnd);

        long postOverhead = breakdown.computePostPhaseOverhead(requestEnd);
        assertEquals("Post-phase overhead should be 4ms in nanos",
            4_000_000L, postOverhead);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);
        assertEquals("post_phase_overhead in map should be 4ms",
            4L, (long) map.get("post_phase_overhead"));
    }

    /**
     * Verifies that computePrePhaseOverhead returns 0 when no phase has started.
     * <p>
     * Validates: Requirements 2.4 (edge case)
     */
    public void testPrePhaseOverhead_zeroWhenNoPhaseStarted() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;
        assertEquals("Pre-phase overhead should be 0 when no phase started",
            0L, breakdown.computePrePhaseOverhead(absoluteStart));
    }

    /**
     * Verifies that computePostPhaseOverhead returns 0 when no phase has ended.
     * <p>
     * Validates: Requirements 2.5 (edge case)
     */
    public void testPostPhaseOverhead_zeroWhenNoPhaseEnded() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long requestEnd = 2_000_000_000L;
        assertEquals("Post-phase overhead should be 0 when no phase ended",
            0L, breakdown.computePostPhaseOverhead(requestEnd));
    }

    // ========== 7. Timed event recording ==========

    /**
     * Verifies that recordTimedEvent stores events and they appear in toTimedBreakdownMap.
     * <p>
     * Validates: Requirements 1.5, 2.2
     */
    public void testTimedEventRecording_appearsInTimedMap() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;
        long eventStart = absoluteStart + 5_000_000L;    // 5ms after start
        long eventEnd = eventStart + 8_000_000L;         // 8ms duration

        breakdown.recordTimedEvent("query_rewrite", eventStart, eventEnd);

        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        assertTrue("query_rewrite should be present in timed map",
            timedMap.containsKey("query_rewrite"));

        Map<String, Long> timing = timedMap.get("query_rewrite");
        assertEquals("start_offset_micros should be 5000 (5ms in micros)",
            5000L, (long) timing.get("start_offset_micros"));
        assertEquals("duration_micros should be 8000 (8ms in micros)",
            8000L, (long) timing.get("duration_micros"));
        assertEquals("duration_millis should be 8",
            8L, (long) timing.get("duration_millis"));
    }

    /**
     * Verifies that timed events with zero duration are omitted from the timed map.
     * <p>
     * Validates: Requirements 1.5 (zero-duration handling)
     */
    public void testTimedEventRecording_zeroDurationOmitted() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;
        long eventStart = absoluteStart + 5_000_000L;

        // Record an event with zero duration (startNanos == endNanos)
        breakdown.recordTimedEvent("zero_event", eventStart, eventStart);

        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);
        assertFalse("Zero-duration events should be omitted from timed map",
            timedMap.containsKey("zero_event"));
    }

    // ========== 8. Combined coordinator flow simulation ==========

    /**
     * End-to-end test simulating a realistic coordinator instrumentation flow:
     * pre-phase → can_match → gap → query → gap → fetch → reduce → post-phase.
     * Verifies the complete unified breakdown map contains all expected entries.
     * <p>
     * Validates: Requirements 1.1–1.6, 2.1–2.5, 3.1–3.4, 4.1, 10.1–10.4
     */
    public void testFullCoordinatorFlow_producesCompleteBreakdown() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Simulated timeline (all in nanos, durations chosen to be >= 1ms)
        long absoluteStart = 1_000_000_000L;

        // Pre-phase work
        breakdown.recordPipelineRequestTransform(5_000_000L);
        breakdown.recordQueryRewrite(8_000_000L);
        breakdown.recordIndexResolution(2_000_000L);
        breakdown.recordShardRouting(3_000_000L);
        breakdown.recordCoordinatorQueueWait(10_000_000L);

        // Phase lifecycle: can_match
        long canMatchStart = absoluteStart + 28_000_000L;
        breakdown.markFirstPhaseStart(canMatchStart);
        breakdown.recordCanMatchPhase(5_000_000L);
        long canMatchEnd = canMatchStart + 5_000_000L;
        breakdown.markPhaseEnd("can_match", canMatchEnd);

        // Inter-phase gap: can_match → query
        long queryStart = canMatchEnd + 2_000_000L;
        breakdown.recordInterPhaseGap("can_match", "query", queryStart - canMatchEnd);

        // Phase lifecycle: query
        breakdown.recordQueryPhase(40_000_000L);
        long queryEnd = queryStart + 40_000_000L;
        breakdown.markPhaseEnd("query", queryEnd);

        // Inter-phase gap: query → fetch
        long fetchStart = queryEnd + 12_000_000L;
        breakdown.recordInterPhaseGap("query", "fetch", fetchStart - queryEnd);

        // Phase lifecycle: fetch
        breakdown.recordFetchPhase(15_000_000L);
        long fetchEnd = fetchStart + 15_000_000L;
        breakdown.markPhaseEnd("fetch", fetchEnd);

        // Reduce
        breakdown.recordReduceTopDocs(4_000_000L);
        breakdown.recordReduceAggregations(3_000_000L);

        // Network
        breakdown.recordNetworkRoundtripMax(20_000_000L);
        breakdown.recordNetworkRoundtripAvg(15_000_000L);

        long requestEnd = fetchEnd + 10_000_000L;

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // Pre-phase metrics
        assertEquals(5L, (long) map.get("pipeline_request_transform"));
        assertEquals(8L, (long) map.get("query_rewrite"));
        assertEquals(2L, (long) map.get("index_resolution"));
        assertEquals(3L, (long) map.get("shard_routing"));
        assertEquals(10L, (long) map.get("coordinator_queue_wait"));

        // Pre-phase overhead
        assertEquals(28L, (long) map.get("pre_phase_overhead"));

        // Phases
        assertEquals(5L, (long) map.get("can_match"));
        assertEquals(40L, (long) map.get("query"));
        assertEquals(15L, (long) map.get("fetch"));

        // Inter-phase gaps
        assertEquals(2L, (long) map.get("can_match_to_query_gap"));
        assertEquals(12L, (long) map.get("query_to_fetch_gap"));

        // Reduce
        assertEquals(4L, (long) map.get("reduce_top_docs"));
        assertEquals(3L, (long) map.get("reduce_aggregations"));

        // Network
        assertEquals(20L, (long) map.get("network_roundtrip_max"));
        assertEquals(15L, (long) map.get("network_roundtrip_avg"));

        // Post-phase overhead
        assertEquals(10L, (long) map.get("post_phase_overhead"));
    }
}
