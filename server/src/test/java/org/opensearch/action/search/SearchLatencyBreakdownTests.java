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

    private static Map<String, Long> phaseMap(long value) {
        Map<String, Long> m = new HashMap<>();
        for (SearchPhaseName name : SearchPhaseName.values()) {
            m.put(name.getName(), value);
        }
        return m;
    }

    private static String toJson(SearchResponse.SearchLatencyBreakdown breakdown) throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        breakdown.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    public void testXContentEmitsOffsetAndDurationForEveryPhase() throws Exception {
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 1200L);
        durations.put(SearchPhaseName.QUERY.getName(), 50000L); // 50ms in micros
        offsets.put(SearchPhaseName.FETCH.getName(), 60000L);
        durations.put(SearchPhaseName.FETCH.getName(), 25000L);

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, new HashMap<>());
        String json = toJson(breakdown);

        for (SearchPhaseName name : SearchPhaseName.values()) {
            assertTrue("missing phase " + name.getName(), json.contains("\"" + name.getName() + "\""));
        }
        assertTrue(json.contains("\"start_offset_micros\":1200"));
        assertTrue(json.contains("\"duration_micros\":50000"));
        assertTrue(json.contains("\"start_offset_micros\":60000"));
        assertTrue(json.contains("\"duration_micros\":25000"));
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

    public void testDerivedCoordinatorDispatch() throws Exception {
        Map<String, Long> offsets = new HashMap<>();
        Map<String, Long> durations = new HashMap<>();
        offsets.put(SearchPhaseName.QUERY.getName(), 1200L);
        durations.put(SearchPhaseName.QUERY.getName(), 50000L);

        Map<String, long[]> events = new HashMap<>();
        events.put(CoordinatorLatencyEventName.INDEX_RESOLUTION.getName(), new long[] { 1150L, 20L }); // ends 1170

        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events);
        String json = toJson(breakdown);

        // dispatch spans 1170 -> 1200 (query start): start 1170, duration 30.
        assertTrue("dispatch missing", json.contains("\"coordinator_dispatch\""));
    }

    public void testEqualsAndHashCode() {
        Map<String, Long> offsets = phaseMap(5L);
        Map<String, Long> durations = phaseMap(10L);
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

        Map<String, long[]> events3 = new HashMap<>();
        events3.put(CoordinatorLatencyEventName.QUERY_REWRITE.getName(), new long[] { 9L, 2L });
        SearchResponse.SearchLatencyBreakdown c = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events3);
        assertNotEquals(a, c);
    }

    public void testGettersReturnUnderlyingMaps() {
        Map<String, Long> offsets = phaseMap(7L);
        Map<String, Long> durations = phaseMap(3L);
        Map<String, long[]> events = new HashMap<>();
        SearchResponse.SearchLatencyBreakdown breakdown = new SearchResponse.SearchLatencyBreakdown(offsets, durations, events);
        assertEquals(offsets, breakdown.getPhaseStartOffsetMicrosMap());
        assertEquals(durations, breakdown.getPhaseDurationMicrosMap());
        assertEquals(events, breakdown.getCoordinatorEventMap());
    }
}
