/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link SearchResponse.SearchLatencyBreakdown}, the coordinator-side latency timeline
 * (phases + coordinator events + derived dispatch/reduce gaps), all in microseconds, derived from timings
 * the coordinator already records.
 */
public class SearchLatencyBreakdownTests extends OpenSearchTestCase {

    private static String toJson(SearchResponse.SearchLatencyBreakdown breakdown) throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        breakdown.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    public void testEmitsOnlyExecutedPhases() throws Exception {
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 1200L);
        durations.put(SearchPhaseName.QUERY.getName(), 50000L); // 50ms in micros
        offsets.put(SearchPhaseName.FETCH.getName(), 60000L);
        durations.put(SearchPhaseName.FETCH.getName(), 25000L);

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, new HashMap<>());
        String json = toJson(breakdown);

        // Executed phases present with their values.
        assertTrue(json.contains("\"query\""));
        assertTrue(json.contains("\"fetch\""));
        assertTrue(json.contains("\"start_offset_micros\":1200"));
        assertTrue(json.contains("\"duration_micros\":50000"));
        assertTrue(json.contains("\"start_offset_micros\":60000"));
        assertTrue(json.contains("\"duration_micros\":25000"));

        // Unexecuted phases must NOT appear as phantom zero bars.
        assertFalse("phantom dfs_pre_query", json.contains("\"dfs_pre_query\""));
        assertFalse("phantom dfs_query", json.contains("\"dfs_query\""));
        assertFalse("phantom expand", json.contains("\"expand\""));
        assertFalse("phantom can_match", json.contains("\"can_match\""));
    }

    public void testSubMillisecondEventPreserved() throws Exception {
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 1200L);
        durations.put(SearchPhaseName.QUERY.getName(), 50000L);

        Map<String, long[]> events = new HashMap<>();
        // index_resolution: 20 micros duration — must survive as 20, not floored to 0.
        events.put(CoordinatorLatencyEventName.INDEX_RESOLUTION.getName(), new long[] { 1150L, 20L });

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events);
        String json = toJson(breakdown);

        assertTrue("index_resolution missing", json.contains("\"index_resolution\""));
        assertTrue("sub-ms duration lost", json.contains("\"duration_micros\":20"));
    }

    public void testDerivedReduceGapPicksLargestOfThreePhases() throws Exception {
        // dfs_query @0 for 5ms; query @20ms for 10ms; fetch @70ms.
        // gap dfs_query->query = 20ms-5ms = 15ms; gap query->fetch = 70ms-30ms = 40ms (largest).
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        offsets.put(SearchPhaseName.DFS_QUERY.getName(), 0L);
        durations.put(SearchPhaseName.DFS_QUERY.getName(), 5000L);
        offsets.put(SearchPhaseName.QUERY.getName(), 20000L);
        durations.put(SearchPhaseName.QUERY.getName(), 10000L);
        offsets.put(SearchPhaseName.FETCH.getName(), 70000L);
        durations.put(SearchPhaseName.FETCH.getName(), 25000L);

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, new HashMap<>());
        String json = toJson(breakdown);

        // Largest gap is query(end 30000) -> fetch(start 70000): start 30000, duration 40000.
        assertTrue("reduce gap missing", json.contains("\"reduce_and_coordinate\""));
        assertTrue("expected largest gap start 30000", json.contains("\"start_offset_micros\":30000"));
        assertTrue("expected largest gap duration 40000", json.contains("\"duration_micros\":40000"));
    }

    /**
     * Regression test for the enum-order bug: DFS executes can_match -> dfs_pre_query -> dfs_query -> query ->
     * fetch, but the enum declares query/fetch before dfs_query. The reduce gap must be computed in temporal
     * (start-offset) order, not enum order.
     */
    public void testDerivedReduceGapRespectsTemporalOrderForDfs() throws Exception {
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        // Temporal order by offset: dfs_pre_query @0(1ms), dfs_query @5ms(2ms), query @30ms(10ms), fetch @45ms(5ms).
        offsets.put(SearchPhaseName.DFS_PRE_QUERY.getName(), 0L);
        durations.put(SearchPhaseName.DFS_PRE_QUERY.getName(), 1000L);
        offsets.put(SearchPhaseName.DFS_QUERY.getName(), 5000L);
        durations.put(SearchPhaseName.DFS_QUERY.getName(), 2000L);
        offsets.put(SearchPhaseName.QUERY.getName(), 30000L);
        durations.put(SearchPhaseName.QUERY.getName(), 10000L);
        offsets.put(SearchPhaseName.FETCH.getName(), 45000L);
        durations.put(SearchPhaseName.FETCH.getName(), 5000L);

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, new HashMap<>());
        String json = toJson(breakdown);

        // Gaps in temporal order: dfs_pre_query(end 1000)->dfs_query(5000)=4000;
        // dfs_query(end 7000)->query(30000)=23000 (largest); query(end 40000)->fetch(45000)=5000.
        // With the enum-order bug this would be computed wrongly (negative gaps dropped).
        assertTrue("reduce gap missing", json.contains("\"reduce_and_coordinate\""));
        assertTrue("expected largest temporal gap start 7000", json.contains("\"start_offset_micros\":7000"));
        assertTrue("expected largest temporal gap duration 23000", json.contains("\"duration_micros\":23000"));
    }

    public void testDerivedCoordinatorDispatchValue() throws Exception {
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 1200L);
        durations.put(SearchPhaseName.QUERY.getName(), 50000L);

        Map<String, long[]> events = new HashMap<>();
        events.put(CoordinatorLatencyEventName.INDEX_RESOLUTION.getName(), new long[] { 1150L, 20L }); // ends 1170

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events);
        String json = toJson(breakdown);

        // dispatch spans event end 1170 -> query start 1200: start 1170, duration 30.
        assertTrue("dispatch missing", json.contains("\"coordinator_dispatch\""));
        assertTrue("dispatch start 1170", json.contains("\"start_offset_micros\":1170"));
        assertTrue("dispatch duration 30", json.contains("\"duration_micros\":30"));
    }

    public void testNoPhasesProducesEmptyTimeline() throws Exception {
        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>()
        );
        String json = toJson(breakdown);
        // No phases, no events, no derivable spans.
        assertFalse(json.contains("reduce_and_coordinate"));
        assertFalse(json.contains("coordinator_dispatch"));
    }

    public void testSinglePhaseHasNoReduceGap() throws Exception {
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 1000L);
        durations.put(SearchPhaseName.QUERY.getName(), 50000L);

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, new HashMap<>());
        String json = toJson(breakdown);

        assertTrue(json.contains("\"query\""));
        assertFalse("single phase has no reduce gap", json.contains("reduce_and_coordinate"));
    }

    public void testEqualsAndHashCode() {
        Map<String, Long> offsets = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 5L);
        Map<String, Long> durations = new HashMap<>();
        durations.put(SearchPhaseName.QUERY.getName(), 10L);
        Map<String, long[]> events = new HashMap<>();
        events.put(CoordinatorLatencyEventName.QUERY_REWRITE.getName(), new long[] { 1L, 2L });

        SearchResponse.SearchLatencyBreakdown a = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events);
        Map<String, long[]> events2 = new HashMap<>();
        events2.put(CoordinatorLatencyEventName.QUERY_REWRITE.getName(), new long[] { 1L, 2L });
        SearchResponse.SearchLatencyBreakdown b = new SearchResponse.SearchLatencyBreakdown(
            new HashMap<>(offsets),
            new HashMap<>(durations),
            events2
        );
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        // Differ by a phase duration.
        Map<String, Long> durations2 = new HashMap<>();
        durations2.put(SearchPhaseName.QUERY.getName(), 11L);
        SearchResponse.SearchLatencyBreakdown c = new SearchResponse.SearchLatencyBreakdown(offsets, durations2, events2);
        assertNotEquals(a, c);

        // Differ by a coordinator event.
        Map<String, long[]> events3 = new HashMap<>();
        events3.put(CoordinatorLatencyEventName.QUERY_REWRITE.getName(), new long[] { 9L, 2L });
        SearchResponse.SearchLatencyBreakdown d = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events3);
        assertNotEquals(a, d);
    }

    public void testGettersReturnUnmodifiableMaps() {
        Map<String, Long> offsets = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 7L);
        Map<String, Long> durations = new HashMap<>();
        durations.put(SearchPhaseName.QUERY.getName(), 3L);
        Map<String, long[]> events = new HashMap<>();

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events);
        assertEquals(offsets, breakdown.getPhaseStartOffsetMicrosMap());
        assertEquals(durations, breakdown.getPhaseDurationMicrosMap());

        // Getters are read-only; mutating the returned maps must fail.
        expectThrows(UnsupportedOperationException.class, () -> breakdown.getPhaseStartOffsetMicrosMap().put("x", 1L));
        expectThrows(UnsupportedOperationException.class, () -> breakdown.getCoordinatorEventMap().put("x", new long[] { 0, 0 }));

        // Mutating the source map after construction must not affect the breakdown (defensive copy).
        offsets.put(SearchPhaseName.FETCH.getName(), 99L);
        assertFalse(breakdown.getPhaseStartOffsetMicrosMap().containsKey(SearchPhaseName.FETCH.getName()));
    }
}
