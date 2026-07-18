/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end verification test for the coordinator's {@code toTimedBreakdownMap()} output format.
 * <p>
 * Verifies:
 * <ul>
 *   <li>Each recorded event appears with {@code .start_offset_micros} and {@code .duration_micros} keys</li>
 *   <li>{@code _has_timed_breakdown} flag is set to 1 when named events exist</li>
 *   <li>Wire bars (wire_out_query, wire_back_query) appear when wall_clock_offset > 0</li>
 *   <li>Reduce metrics appear when recorded</li>
 *   <li>Conditional metrics appear only when recorded</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 8.1, 8.2, 10.4</b>
 */
public class SearchLatencyBreakdownTimedMapOutputFormatTests extends OpenSearchTestCase {

    /**
     * Verifies that toTimedBreakdownMap() produces entries with .start_offset_micros and .duration_micros
     * for all recorded timed events, including query phase, wire bars, and reduce metrics.
     */
    public void testTimedBreakdownMapOutputFormat_allKeysHaveCorrectSuffixes() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Record query phase as a timed event
        long queryStart = absoluteStart + TimeUnit.MILLISECONDS.toNanos(5);
        long queryEnd = queryStart + TimeUnit.MILLISECONDS.toNanos(50);
        breakdown.recordTimedEvent("query", queryStart, queryEnd);

        // Record wire_out_query (simulating coordinator wire decomposition)
        long wireOutStart = absoluteStart + TimeUnit.MILLISECONDS.toNanos(3);
        long wireOutEnd = wireOutStart + TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordTimedEvent("wire_out_query", wireOutStart, wireOutEnd);

        // Record wire_back_query
        long wireBackStart = queryEnd;
        long wireBackEnd = wireBackStart + TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordTimedEvent("wire_back_query", wireBackStart, wireBackEnd);

        // Record reduce_top_docs
        long reduceStart = wireBackEnd + TimeUnit.MILLISECONDS.toNanos(1);
        long reduceEnd = reduceStart + TimeUnit.MILLISECONDS.toNanos(4);
        breakdown.recordTimedEvent("reduce_top_docs", reduceStart, reduceEnd);

        // Record reduce_aggregations
        long reduceAggsStart = reduceEnd;
        long reduceAggsEnd = reduceAggsStart + TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordTimedEvent("reduce_aggregations", reduceAggsStart, reduceAggsEnd);

        // Get the timed breakdown map
        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        // Verify all recorded events are present
        assertTrue("query should be present", timedMap.containsKey("query"));
        assertTrue("wire_out_query should be present", timedMap.containsKey("wire_out_query"));
        assertTrue("wire_back_query should be present", timedMap.containsKey("wire_back_query"));
        assertTrue("reduce_top_docs should be present", timedMap.containsKey("reduce_top_docs"));
        assertTrue("reduce_aggregations should be present", timedMap.containsKey("reduce_aggregations"));

        // Verify each entry has the correct keys: start_offset_micros, duration_micros, duration_millis
        for (Map.Entry<String, Map<String, Long>> entry : timedMap.entrySet()) {
            String eventName = entry.getKey();
            Map<String, Long> timing = entry.getValue();

            assertTrue(eventName + " should have start_offset_micros",
                timing.containsKey("start_offset_micros"));
            assertTrue(eventName + " should have duration_micros",
                timing.containsKey("duration_micros"));
            assertTrue(eventName + " should have duration_millis",
                timing.containsKey("duration_millis"));

            // Values should be non-negative
            assertTrue(eventName + ".start_offset_micros should be >= 0",
                timing.get("start_offset_micros") >= 0);
            assertTrue(eventName + ".duration_micros should be > 0",
                timing.get("duration_micros") > 0);
        }

        // Verify specific offset values
        Map<String, Long> queryTiming = timedMap.get("query");
        assertEquals("query.start_offset_micros should be 5000",
            5000L, (long) queryTiming.get("start_offset_micros"));
        assertEquals("query.duration_micros should be 50000",
            50000L, (long) queryTiming.get("duration_micros"));

        Map<String, Long> wireOutTiming = timedMap.get("wire_out_query");
        assertEquals("wire_out_query.start_offset_micros should be 3000",
            3000L, (long) wireOutTiming.get("start_offset_micros"));
        assertEquals("wire_out_query.duration_micros should be 2000",
            2000L, (long) wireOutTiming.get("duration_micros"));
    }

    /**
     * Verifies that the _has_timed_breakdown flag is set to 1 in the XContent output
     * when named events are present.
     */
    public void testHasTimedBreakdownFlagSet_whenNamedEventsExist() throws IOException {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Record at least one timed event
        long eventStart = absoluteStart + TimeUnit.MILLISECONDS.toNanos(2);
        long eventEnd = eventStart + TimeUnit.MILLISECONDS.toNanos(10);
        breakdown.recordTimedEvent("query", eventStart, eventEnd);

        // Serialize to XContent — toXContent starts its own object, so we need the root object
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        breakdown.toXContent(builder, null);
        builder.endObject();
        builder.close();
        String json = builder.getOutputStream().toString();

        assertTrue("XContent output should contain _has_timed_breakdown",
            json.contains("\"_has_timed_breakdown\":1"));
        assertTrue("XContent output should contain query.start_offset_micros",
            json.contains("\"query.start_offset_micros\""));
        assertTrue("XContent output should contain query.duration_micros",
            json.contains("\"query.duration_micros\""));
    }

    /**
     * Verifies that _has_timed_breakdown flag is NOT set when no named events are recorded.
     */
    public void testHasTimedBreakdownFlagAbsent_whenNoNamedEvents() throws IOException {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Record only AtomicLong metrics (no timed events)
        breakdown.recordQueryPhase(TimeUnit.MILLISECONDS.toNanos(50));

        // Serialize to XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        breakdown.toXContent(builder, null);
        builder.endObject();
        builder.close();
        String json = builder.getOutputStream().toString();

        assertFalse("XContent output should NOT contain _has_timed_breakdown when no timed events",
            json.contains("_has_timed_breakdown"));
    }

    /**
     * Verifies that wire bars (wire_out_query, wire_back_query) appear in the timed breakdown map
     * when wall_clock_offset > 0 and they are recorded as timed events.
     */
    public void testWireBarsAppear_whenWallClockOffsetPositive() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Simulate wall_clock_offset > 0 (coordinator records this from shard response)
        long wallClockOffsetMicros = 250L;
        breakdown.recordWallClockOffsetMicros(wallClockOffsetMicros);

        // Simulate the coordinator recording wire bars after computing them
        long wireOutNanos = TimeUnit.MICROSECONDS.toNanos(wallClockOffsetMicros);
        long preSearchEndNanos = absoluteStart + TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordTimedEvent("wire_out_query", preSearchEndNanos, preSearchEndNanos + wireOutNanos);

        // Query phase
        long queryStart = preSearchEndNanos + wireOutNanos;
        long queryDuration = TimeUnit.MILLISECONDS.toNanos(40);
        breakdown.recordTimedEvent("query", queryStart, queryStart + queryDuration);

        // Wire back query
        long wireBackNanos = TimeUnit.MICROSECONDS.toNanos(180);
        long wireBackStart = queryStart + queryDuration;
        breakdown.recordTimedEvent("wire_back_query", wireBackStart, wireBackStart + wireBackNanos);

        // Get timed breakdown map
        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        // Wire bars should be present
        assertTrue("wire_out_query should be present when wall_clock_offset > 0",
            timedMap.containsKey("wire_out_query"));
        assertTrue("wire_back_query should be present when wall_clock_offset > 0",
            timedMap.containsKey("wire_back_query"));

        // Verify wire_out starts at preSearchEnd relative to absoluteStart
        Map<String, Long> wireOutTiming = timedMap.get("wire_out_query");
        assertEquals("wire_out_query.start_offset_micros should equal pre-search end offset",
            5000L, (long) wireOutTiming.get("start_offset_micros"));
        assertEquals("wire_out_query.duration_micros should match wall_clock_offset",
            wallClockOffsetMicros, (long) wireOutTiming.get("duration_micros"));

        // Verify wire_back starts after query phase
        Map<String, Long> wireBackTiming = timedMap.get("wire_back_query");
        long expectedWireBackStartMicros = TimeUnit.NANOSECONDS.toMicros(wireBackStart - absoluteStart);
        assertEquals("wire_back_query.start_offset_micros should be after query phase end",
            expectedWireBackStartMicros, (long) wireBackTiming.get("start_offset_micros"));
    }

    /**
     * Verifies that wire bars do NOT appear in the timed breakdown map
     * when wall_clock_offset is 0 and no wire events are recorded.
     */
    public void testWireBarsAbsent_whenNoWireEventsRecorded() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Only record query phase, no wire events
        long queryStart = absoluteStart + TimeUnit.MILLISECONDS.toNanos(5);
        long queryEnd = queryStart + TimeUnit.MILLISECONDS.toNanos(40);
        breakdown.recordTimedEvent("query", queryStart, queryEnd);

        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        assertFalse("wire_out_query should NOT be present when no wire events recorded",
            timedMap.containsKey("wire_out_query"));
        assertFalse("wire_back_query should NOT be present when no wire events recorded",
            timedMap.containsKey("wire_back_query"));
    }

    /**
     * Verifies that reduce metrics appear in the timed breakdown map when recorded as timed events.
     */
    public void testReduceMetricsAppear_whenRecordedAsTimedEvents() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Simulate reduce phase metrics recorded as timed events by the coordinator
        long reduceStart = absoluteStart + TimeUnit.MILLISECONDS.toNanos(60);
        long reduceTopDocsEnd = reduceStart + TimeUnit.MILLISECONDS.toNanos(4);
        breakdown.recordTimedEvent("reduce_top_docs", reduceStart, reduceTopDocsEnd);

        long reduceAggsEnd = reduceTopDocsEnd + TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordTimedEvent("reduce_aggregations", reduceTopDocsEnd, reduceAggsEnd);

        long reducePipelineEnd = reduceAggsEnd + TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordTimedEvent("reduce_pipeline_aggs", reduceAggsEnd, reducePipelineEnd);

        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        // All reduce metrics should appear
        assertTrue("reduce_top_docs should be present", timedMap.containsKey("reduce_top_docs"));
        assertTrue("reduce_aggregations should be present", timedMap.containsKey("reduce_aggregations"));
        assertTrue("reduce_pipeline_aggs should be present", timedMap.containsKey("reduce_pipeline_aggs"));

        // Verify start offsets and durations
        Map<String, Long> reduceTopDocsTiming = timedMap.get("reduce_top_docs");
        assertEquals("reduce_top_docs.start_offset_micros should be 60000",
            60000L, (long) reduceTopDocsTiming.get("start_offset_micros"));
        assertEquals("reduce_top_docs.duration_micros should be 4000",
            4000L, (long) reduceTopDocsTiming.get("duration_micros"));

        Map<String, Long> reduceAggsTiming = timedMap.get("reduce_aggregations");
        assertEquals("reduce_aggregations.start_offset_micros should be 64000",
            64000L, (long) reduceAggsTiming.get("start_offset_micros"));
        assertEquals("reduce_aggregations.duration_micros should be 3000",
            3000L, (long) reduceAggsTiming.get("duration_micros"));
    }

    /**
     * Verifies that conditional metrics (global_agg_separate_pass, search_idle_reactivation,
     * slice_creation, pipeline_response_transform) appear ONLY when recorded as timed events.
     */
    public void testConditionalMetricsAppear_onlyWhenRecorded() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Record only query phase — no conditional metrics
        long queryStart = absoluteStart + TimeUnit.MILLISECONDS.toNanos(5);
        long queryEnd = queryStart + TimeUnit.MILLISECONDS.toNanos(50);
        breakdown.recordTimedEvent("query", queryStart, queryEnd);

        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        // Conditional metrics should NOT be present
        assertFalse("global_agg_separate_pass should NOT be present without recording",
            timedMap.containsKey("global_agg_separate_pass"));
        assertFalse("search_idle_reactivation should NOT be present without recording",
            timedMap.containsKey("search_idle_reactivation"));
        assertFalse("slice_creation should NOT be present without recording",
            timedMap.containsKey("slice_creation"));
        assertFalse("pipeline_response_transform should NOT be present without recording",
            timedMap.containsKey("pipeline_response_transform"));

        // Now record conditional metrics
        long globalAggStart = queryStart + TimeUnit.MILLISECONDS.toNanos(30);
        long globalAggEnd = globalAggStart + TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordTimedEvent("global_agg_separate_pass", globalAggStart, globalAggEnd);

        long idleReactivationStart = queryStart;
        long idleReactivationEnd = idleReactivationStart + TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordTimedEvent("search_idle_reactivation", idleReactivationStart, idleReactivationEnd);

        long sliceStart = queryStart + TimeUnit.MILLISECONDS.toNanos(10);
        long sliceEnd = sliceStart + TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordTimedEvent("slice_creation", sliceStart, sliceEnd);

        long pipelineStart = queryEnd + TimeUnit.MILLISECONDS.toNanos(5);
        long pipelineEnd = pipelineStart + TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordTimedEvent("pipeline_response_transform", pipelineStart, pipelineEnd);

        // Re-fetch timed breakdown map
        timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        // Now conditional metrics should be present
        assertTrue("global_agg_separate_pass should be present when recorded",
            timedMap.containsKey("global_agg_separate_pass"));
        assertTrue("search_idle_reactivation should be present when recorded",
            timedMap.containsKey("search_idle_reactivation"));
        assertTrue("slice_creation should be present when recorded",
            timedMap.containsKey("slice_creation"));
        assertTrue("pipeline_response_transform should be present when recorded",
            timedMap.containsKey("pipeline_response_transform"));

        // Verify correct durations
        assertEquals("global_agg_separate_pass.duration_micros should be 5000",
            5000L, (long) timedMap.get("global_agg_separate_pass").get("duration_micros"));
        assertEquals("search_idle_reactivation.duration_micros should be 2000",
            2000L, (long) timedMap.get("search_idle_reactivation").get("duration_micros"));
        assertEquals("slice_creation.duration_micros should be 3000",
            3000L, (long) timedMap.get("slice_creation").get("duration_micros"));
        assertEquals("pipeline_response_transform.duration_micros should be 2000",
            2000L, (long) timedMap.get("pipeline_response_transform").get("duration_micros"));
    }

    /**
     * End-to-end test simulating a full search request lifecycle with all metric categories
     * and verifying the complete timed breakdown map output format.
     * <p>
     * Records:
     * - Pre-search setup (as timed event)
     * - Wire out query
     * - Query phase + internal events
     * - Wire back query
     * - Reduce metrics
     * - Conditional metrics (global_agg_separate_pass, slice_creation)
     * <p>
     * Validates: Requirements 8.1, 8.2, 10.4
     */
    public void testEndToEndTimedBreakdownMap_fullLifecycle() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // ---- Pre-search setup ----
        long preSearchStart = absoluteStart;
        long preSearchEnd = absoluteStart + TimeUnit.MILLISECONDS.toNanos(6);
        breakdown.recordTimedEvent("pre_search_setup", preSearchStart, preSearchEnd);

        // ---- Wire Out Query ----
        long wireOutQueryStart = preSearchEnd;
        long wireOutQueryEnd = wireOutQueryStart + TimeUnit.MICROSECONDS.toNanos(250);
        breakdown.recordTimedEvent("wire_out_query", wireOutQueryStart, wireOutQueryEnd);
        breakdown.recordWallClockOffsetMicros(250L);

        // ---- Query Phase ----
        long queryPhaseStart = wireOutQueryEnd;
        long queryPhaseDuration = TimeUnit.MILLISECONDS.toNanos(35);
        long queryPhaseEnd = queryPhaseStart + queryPhaseDuration;
        breakdown.recordTimedEvent("query", queryPhaseStart, queryPhaseEnd);
        breakdown.recordQueryPhase(queryPhaseDuration);
        breakdown.markFirstPhaseStart(queryPhaseStart);
        breakdown.markPhaseEnd("query", queryPhaseEnd);

        // Query phase internals
        long searchContextStart = queryPhaseStart + TimeUnit.MILLISECONDS.toNanos(1);
        long searchContextEnd = searchContextStart + TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordTimedEvent("search_context_creation", searchContextStart, searchContextEnd);

        long aggInitStart = searchContextEnd;
        long aggInitEnd = aggInitStart + TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordTimedEvent("agg_initialize", aggInitStart, aggInitEnd);

        // Conditional: global_agg_separate_pass (global aggs configured)
        long globalAggStart = queryPhaseStart + TimeUnit.MILLISECONDS.toNanos(25);
        long globalAggEnd = globalAggStart + TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordTimedEvent("global_agg_separate_pass", globalAggStart, globalAggEnd);

        // Conditional: slice_creation (concurrent segment search active)
        long sliceCreateStart = queryPhaseStart + TimeUnit.MILLISECONDS.toNanos(5);
        long sliceCreateEnd = sliceCreateStart + TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordTimedEvent("slice_creation", sliceCreateStart, sliceCreateEnd);

        // ---- Wire Back Query ----
        long wireBackQueryStart = queryPhaseEnd;
        long wireBackQueryEnd = wireBackQueryStart + TimeUnit.MICROSECONDS.toNanos(180);
        breakdown.recordTimedEvent("wire_back_query", wireBackQueryStart, wireBackQueryEnd);

        // ---- Reduce Phase ----
        long reduceTopDocsStart = wireBackQueryEnd + TimeUnit.MILLISECONDS.toNanos(1);
        long reduceTopDocsEnd = reduceTopDocsStart + TimeUnit.MILLISECONDS.toNanos(4);
        breakdown.recordTimedEvent("reduce_top_docs", reduceTopDocsStart, reduceTopDocsEnd);
        breakdown.recordReduceTopDocs(reduceTopDocsEnd - reduceTopDocsStart);

        long reduceAggsStart = reduceTopDocsEnd;
        long reduceAggsEnd = reduceAggsStart + TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordTimedEvent("reduce_aggregations", reduceAggsStart, reduceAggsEnd);
        breakdown.recordReduceAggregations(reduceAggsEnd - reduceAggsStart);

        long reducePipelineStart = reduceAggsEnd;
        long reducePipelineEnd = reducePipelineStart + TimeUnit.MILLISECONDS.toNanos(1);
        breakdown.recordTimedEvent("reduce_pipeline_aggs", reducePipelineStart, reducePipelineEnd);
        breakdown.recordReducePipelineAggs(reducePipelineEnd - reducePipelineStart);

        // ---- Get Timed Breakdown Map ----
        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        // ---- Assertions: All events present ----
        String[] expectedKeys = {
            "pre_search_setup",
            "wire_out_query",
            "query",
            "search_context_creation",
            "agg_initialize",
            "global_agg_separate_pass",
            "slice_creation",
            "wire_back_query",
            "reduce_top_docs",
            "reduce_aggregations",
            "reduce_pipeline_aggs"
        };

        for (String key : expectedKeys) {
            assertTrue("Expected key '" + key + "' in timed breakdown map", timedMap.containsKey(key));
            Map<String, Long> timing = timedMap.get(key);
            assertTrue(key + " must have start_offset_micros", timing.containsKey("start_offset_micros"));
            assertTrue(key + " must have duration_micros", timing.containsKey("duration_micros"));
            assertTrue(key + ".start_offset_micros must be >= 0", timing.get("start_offset_micros") >= 0);
            assertTrue(key + ".duration_micros must be > 0", timing.get("duration_micros") > 0);
        }

        // ---- Assertions: Verify temporal ordering ----
        long wireOutStartMicros = timedMap.get("wire_out_query").get("start_offset_micros");
        long queryStartMicros = timedMap.get("query").get("start_offset_micros");
        long wireBackStartMicros = timedMap.get("wire_back_query").get("start_offset_micros");
        long reduceStartMicros = timedMap.get("reduce_top_docs").get("start_offset_micros");

        assertTrue("wire_out_query should start before query phase",
            wireOutStartMicros <= queryStartMicros);
        assertTrue("wire_back_query should start at or after query phase end",
            wireBackStartMicros >= queryStartMicros + timedMap.get("query").get("duration_micros"));
        assertTrue("reduce_top_docs should start after wire_back_query",
            reduceStartMicros >= wireBackStartMicros);
    }

    /**
     * Verifies that the XContent output includes .start_offset_micros and .duration_micros
     * suffixed keys for timed events — the API output format consumed by the dashboard.
     */
    public void testXContentOutput_hasSuffixedKeys() throws IOException {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Record events
        breakdown.recordTimedEvent("query", absoluteStart + TimeUnit.MILLISECONDS.toNanos(5),
            absoluteStart + TimeUnit.MILLISECONDS.toNanos(55));
        breakdown.recordTimedEvent("wire_out_query", absoluteStart + TimeUnit.MILLISECONDS.toNanos(3),
            absoluteStart + TimeUnit.MILLISECONDS.toNanos(5));
        breakdown.recordTimedEvent("reduce_top_docs", absoluteStart + TimeUnit.MILLISECONDS.toNanos(58),
            absoluteStart + TimeUnit.MILLISECONDS.toNanos(62));

        // Serialize to JSON
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        breakdown.toXContent(builder, null);
        builder.endObject();
        builder.close();
        String json = builder.getOutputStream().toString();

        // Verify _has_timed_breakdown flag
        assertTrue("Should have _has_timed_breakdown flag",
            json.contains("\"_has_timed_breakdown\":1"));

        // Verify suffixed keys for each event
        assertTrue("Should have query.start_offset_micros",
            json.contains("\"query.start_offset_micros\""));
        assertTrue("Should have query.duration_micros",
            json.contains("\"query.duration_micros\""));
        assertTrue("Should have wire_out_query.start_offset_micros",
            json.contains("\"wire_out_query.start_offset_micros\""));
        assertTrue("Should have wire_out_query.duration_micros",
            json.contains("\"wire_out_query.duration_micros\""));
        assertTrue("Should have reduce_top_docs.start_offset_micros",
            json.contains("\"reduce_top_docs.start_offset_micros\""));
        assertTrue("Should have reduce_top_docs.duration_micros",
            json.contains("\"reduce_top_docs.duration_micros\""));
    }

    /**
     * Verifies that self-describing metrics (unknown keys following the X + X_start convention)
     * automatically appear in the timed breakdown map when recorded as timed events.
     * This validates the extensibility requirement — no code changes needed for new metrics.
     */
    public void testSelfDescribingMetrics_autoAppearInTimedMap() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Record a completely unknown metric via the self-describing convention
        long customStart = absoluteStart + TimeUnit.MILLISECONDS.toNanos(10);
        long customEnd = customStart + TimeUnit.MILLISECONDS.toNanos(7);
        breakdown.recordTimedEvent("my_custom_new_metric", customStart, customEnd);

        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        // Unknown metric should automatically appear
        assertTrue("Self-describing metric should auto-appear in timed map",
            timedMap.containsKey("my_custom_new_metric"));

        Map<String, Long> timing = timedMap.get("my_custom_new_metric");
        assertEquals("Custom metric start_offset_micros should be 10000",
            10000L, (long) timing.get("start_offset_micros"));
        assertEquals("Custom metric duration_micros should be 7000",
            7000L, (long) timing.get("duration_micros"));
    }

    /**
     * Verifies that zero-duration timed events are omitted from the output map.
     */
    public void testZeroDurationTimedEvents_omittedFromOutput() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 1_000_000_000L;

        // Record a zero-duration event (start == end)
        long eventTime = absoluteStart + TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordTimedEvent("zero_duration_event", eventTime, eventTime);

        // Record a sub-microsecond duration event (< 1000 nanos, rounds to 0 micros)
        breakdown.recordTimedEvent("sub_micro_event", eventTime, eventTime + 500);

        // Record a valid event for comparison
        breakdown.recordTimedEvent("valid_event", eventTime, eventTime + TimeUnit.MILLISECONDS.toNanos(5));

        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

        assertFalse("Zero-duration event should be omitted",
            timedMap.containsKey("zero_duration_event"));
        assertFalse("Sub-microsecond event should be omitted",
            timedMap.containsKey("sub_micro_event"));
        assertTrue("Valid event should be present",
            timedMap.containsKey("valid_event"));
    }
}
