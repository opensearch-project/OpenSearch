/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Unit tests for {@link CompositeShardStats} and {@link CompositeShardStatsTracker}:
 * counter increments, snapshot fidelity, aggregation, and wire/XContent round-trips.
 */
public class CompositeShardStatsTests extends OpenSearchTestCase {

    public void testTrackerCountersFlowIntoSnapshot() {
        CompositeShardStatsTracker tracker = new CompositeShardStatsTracker();
        tracker.incRefreshTotal();
        tracker.addRefreshTimeMillis(10);
        tracker.incRefreshMergeTotal();
        tracker.addRefreshMergeTimeMillis(4);
        tracker.incRefreshMergeFailures();
        tracker.incMergeTotal();
        tracker.addMergeTimeMillis(7);
        tracker.incMergeFailures();
        tracker.incWriteTotal();
        tracker.incWritePrimaryFailures();
        tracker.incWriteSecondaryFailures();
        tracker.incMappingUpdateExecutedTotal();

        CompositeShardStats s = tracker.stats();
        Map<String, Object> json = toMap(s);

        assertEquals(1L, get(json, "refresh.refresh_total"));
        assertEquals(10L, get(json, "refresh.refresh_time_millis"));
        assertEquals(1L, get(json, "refresh.refresh_merge_total"));
        assertEquals(4L, get(json, "refresh.refresh_merge_time_millis"));
        assertEquals(1L, get(json, "refresh.refresh_merge_failures"));
        assertEquals(1L, get(json, "merge.merge_total"));
        assertEquals(7L, get(json, "merge.merge_time_millis"));
        assertEquals(1L, get(json, "merge.merge_failures"));
        assertEquals(1L, get(json, "write.write_total"));
        assertEquals(1L, get(json, "write.write_primary_failures"));
        assertEquals(1L, get(json, "write.write_secondary_failures"));
        assertEquals(1L, get(json, "mapping.mapping_update_executed_total"));
    }

    public void testEmptyIsAllZero() {
        Map<String, Object> json = toMap(CompositeShardStats.empty());
        assertEquals(0L, get(json, "refresh.refresh_total"));
        assertEquals(0L, get(json, "merge.merge_failures"));
        assertEquals(0L, get(json, "write.write_primary_failures"));
        assertEquals(0L, get(json, "mapping.mapping_update_executed_total"));
    }

    public void testAddSumsAllCounters() {
        CompositeShardStats a = new CompositeShardStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        CompositeShardStats b = new CompositeShardStats(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120);
        Map<String, Object> json = toMap(a.add(b));

        assertEquals(11L, get(json, "refresh.refresh_total"));
        assertEquals(33L, get(json, "refresh.refresh_merge_total"));
        assertEquals(55L, get(json, "refresh.refresh_merge_failures"));
        assertEquals(66L, get(json, "merge.merge_total"));
        assertEquals(88L, get(json, "merge.merge_failures"));
        assertEquals(99L, get(json, "write.write_total"));
        assertEquals(110L, get(json, "write.write_primary_failures"));
        assertEquals(132L, get(json, "mapping.mapping_update_executed_total"));
    }

    public void testStreamRoundTrip() throws Exception {
        CompositeShardStats original = new CompositeShardStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            CompositeShardStats restored = new CompositeShardStats(in);
            assertEquals(toMap(original), toMap(restored));
        }
    }

    private static Map<String, Object> toMap(CompositeShardStats stats) {
        try {
            var builder = XContentFactory.jsonBuilder().startObject();
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, builder.toString(), true);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static long get(Map<String, Object> json, String dotted) {
        String[] parts = dotted.split("\\.");
        Object cur = json;
        for (String p : parts) {
            cur = ((Map<String, Object>) cur).get(p);
        }
        return ((Number) cur).longValue();
    }
}
