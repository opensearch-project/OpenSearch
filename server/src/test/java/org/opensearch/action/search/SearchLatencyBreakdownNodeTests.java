/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link SearchLatencyBreakdownNode}.
 * Validates serialization round-trip, toXContent API contract, and category validation.
 */
public class SearchLatencyBreakdownNodeTests extends OpenSearchTestCase {

    // --- Serialization Round-Trip Tests ---

    public void testWriteToReadFromRoundTrip_BasicFields() throws IOException {
        SearchLatencyBreakdownNode original = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE,
            1000L,  // startOffsetNanos
            5000L   // durationNanos
        );

        SearchLatencyBreakdownNode deserialized = roundTrip(original);

        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getCategory(), deserialized.getCategory());
        assertEquals(original.getStartOffsetNanos(), deserialized.getStartOffsetNanos());
        assertEquals(original.getDurationNanos(), deserialized.getDurationNanos());
        assertEquals(original.getEndOffsetNanos(), deserialized.getEndOffsetNanos());
        assertEquals(0, deserialized.getShardCount());
        assertEquals(0, deserialized.getChildren().size());
    }

    public void testWriteToReadFromRoundTrip_WithShardStats() throws IOException {
        SearchLatencyBreakdownNode original = new SearchLatencyBreakdownNode(
            "Search Ctx Creation",
            SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
            2000L,
            10000L
        );
        original.setShardStats(5, 8000L, 3000L, 15000L, 40000L);

        SearchLatencyBreakdownNode deserialized = roundTrip(original);

        assertEquals(original.getShardCount(), deserialized.getShardCount());
        assertEquals(original.getAvgNanos(), deserialized.getAvgNanos());
        assertEquals(original.getMinNanos(), deserialized.getMinNanos());
        assertEquals(original.getMaxNanos(), deserialized.getMaxNanos());
        assertEquals(original.getTotalNanos(), deserialized.getTotalNanos());
    }

    public void testWriteToReadFromRoundTrip_WithChildren() throws IOException {
        SearchLatencyBreakdownNode parent = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE,
            0L,
            50000L
        );
        parent.addChild("Acquire Searcher", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, 1000L, 3000L);
        parent.addChild("Agg Collect", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 5000L, 20000L);

        SearchLatencyBreakdownNode deserialized = roundTrip(parent);

        assertEquals(2, deserialized.getChildren().size());
        assertEquals("Acquire Searcher", deserialized.getChildren().get(0).getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, deserialized.getChildren().get(0).getCategory());
        assertEquals(1000L, deserialized.getChildren().get(0).getStartOffsetNanos());
        assertEquals(3000L, deserialized.getChildren().get(0).getDurationNanos());
        assertEquals(4000L, deserialized.getChildren().get(0).getEndOffsetNanos());

        assertEquals("Agg Collect", deserialized.getChildren().get(1).getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, deserialized.getChildren().get(1).getCategory());
        assertEquals(5000L, deserialized.getChildren().get(1).getStartOffsetNanos());
        assertEquals(20000L, deserialized.getChildren().get(1).getDurationNanos());
    }

    public void testWriteToReadFromRoundTrip_RecursiveChildren() throws IOException {
        // Create a 3-level deep tree
        SearchLatencyBreakdownNode root = new SearchLatencyBreakdownNode(
            "Root",
            SearchLatencyBreakdownNode.CATEGORY_COORDINATOR,
            0L,
            100000L
        );

        SearchLatencyBreakdownNode child = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE,
            5000L,
            80000L
        );

        SearchLatencyBreakdownNode grandchild = new SearchLatencyBreakdownNode(
            "Agg Initialize",
            SearchLatencyBreakdownNode.CATEGORY_AGGREGATION,
            10000L,
            15000L
        );
        grandchild.setShardStats(3, 12000L, 8000L, 20000L, 36000L);

        child.addChild(grandchild);
        root.addChild(child);

        SearchLatencyBreakdownNode deserialized = roundTrip(root);

        assertEquals("Root", deserialized.getName());
        assertEquals(1, deserialized.getChildren().size());

        SearchLatencyBreakdownNode deserializedChild = deserialized.getChildren().get(0);
        assertEquals("Query Phase", deserializedChild.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_PHASE, deserializedChild.getCategory());
        assertEquals(1, deserializedChild.getChildren().size());

        SearchLatencyBreakdownNode deserializedGrandchild = deserializedChild.getChildren().get(0);
        assertEquals("Agg Initialize", deserializedGrandchild.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, deserializedGrandchild.getCategory());
        assertEquals(10000L, deserializedGrandchild.getStartOffsetNanos());
        assertEquals(15000L, deserializedGrandchild.getDurationNanos());
        assertEquals(25000L, deserializedGrandchild.getEndOffsetNanos());
        assertEquals(3, deserializedGrandchild.getShardCount());
        assertEquals(12000L, deserializedGrandchild.getAvgNanos());
        assertEquals(8000L, deserializedGrandchild.getMinNanos());
        assertEquals(20000L, deserializedGrandchild.getMaxNanos());
        assertEquals(36000L, deserializedGrandchild.getTotalNanos());
    }

    // --- toXContent API Contract Tests ---

    public void testToXContent_BasicTimingFields() throws IOException {
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
            "Query Rewrite",
            SearchLatencyBreakdownNode.CATEGORY_COORDINATOR,
            1000000L,  // 1000 micros start offset
            5000000L   // 5000 micros duration
        );

        Map<String, Object> xContentMap = toMap(node);

        assertEquals("Query Rewrite", xContentMap.get("name"));
        assertEquals("coordinator", xContentMap.get("category"));
        assertEquals(TimeUnit.NANOSECONDS.toMicros(1000000L), ((Number) xContentMap.get("start_offset_micros")).longValue());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(5000000L), ((Number) xContentMap.get("duration_micros")).longValue());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(6000000L), ((Number) xContentMap.get("end_offset_micros")).longValue());
        // 5ms > 0, so duration_millis should be present
        assertEquals(5L, ((Number) xContentMap.get("duration_millis")).longValue());
    }

    public void testToXContent_DurationMillisOmittedWhenZero() throws IOException {
        // 500 micros = 0 millis, so duration_millis should be omitted
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
            "Small Op",
            SearchLatencyBreakdownNode.CATEGORY_CACHE,
            0L,
            500000L  // 500 micros in nanos
        );

        Map<String, Object> xContentMap = toMap(node);

        assertNull(xContentMap.get("duration_millis"));
        assertEquals(500L, ((Number) xContentMap.get("duration_micros")).longValue());
    }

    @SuppressWarnings("unchecked")
    public void testToXContent_ShardStats() throws IOException {
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
            "Search Ctx Creation",
            SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
            0L,
            12000000L  // 12ms
        );
        node.setShardStats(5, 8000000L, 3000000L, 15000000L, 40000000L);

        Map<String, Object> xContentMap = toMap(node);

        assertTrue(xContentMap.containsKey("shard_stats"));
        Map<String, Object> shardStats = (Map<String, Object>) xContentMap.get("shard_stats");
        assertEquals(5, ((Number) shardStats.get("shard_count")).intValue());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(8000000L), ((Number) shardStats.get("avg_micros")).longValue());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(3000000L), ((Number) shardStats.get("min_micros")).longValue());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(15000000L), ((Number) shardStats.get("max_micros")).longValue());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(40000000L), ((Number) shardStats.get("total_micros")).longValue());
    }

    public void testToXContent_ShardStatsAbsentWhenZeroShards() throws IOException {
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
            "Queue Wait",
            SearchLatencyBreakdownNode.CATEGORY_CONCURRENCY,
            0L,
            1000000L
        );

        Map<String, Object> xContentMap = toMap(node);

        assertFalse(xContentMap.containsKey("shard_stats"));
    }

    @SuppressWarnings("unchecked")
    public void testToXContent_ChildrenPresent() throws IOException {
        SearchLatencyBreakdownNode parent = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE,
            0L,
            50000000L
        );
        parent.addChild("Acquire Searcher", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, 1000000L, 3000000L);

        Map<String, Object> xContentMap = toMap(parent);

        assertTrue(xContentMap.containsKey("children"));
        java.util.List<Map<String, Object>> children = (java.util.List<Map<String, Object>>) xContentMap.get("children");
        assertEquals(1, children.size());
        assertEquals("Acquire Searcher", children.get(0).get("name"));
        assertEquals("search_context", children.get(0).get("category"));
    }

    public void testToXContent_ChildrenAbsentWhenEmpty() throws IOException {
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
            "Leaf Node",
            SearchLatencyBreakdownNode.CATEGORY_CACHE,
            0L,
            1000L
        );

        Map<String, Object> xContentMap = toMap(node);

        assertFalse(xContentMap.containsKey("children"));
    }

    // --- Category Validation Tests ---

    public void testAllValidCategories() {
        assertEquals(12, SearchLatencyBreakdownNode.VALID_CATEGORIES.size());
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("coordinator"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("query"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("phase"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("aggregation"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("pipeline"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("transport"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("search_context"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("cache"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("concurrency"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("concurrent_segment_search"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("fetch"));
        assertTrue(SearchLatencyBreakdownNode.VALID_CATEGORIES.contains("reduce"));
    }

    public void testInvalidCategoryThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchLatencyBreakdownNode("test", "invalid_category")
        );
        assertTrue(e.getMessage().contains("Invalid breakdown node category"));
        assertTrue(e.getMessage().contains("invalid_category"));
    }

    public void testNullCategoryThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchLatencyBreakdownNode("test", null)
        );
        assertTrue(e.getMessage().contains("Invalid breakdown node category"));
    }

    public void testValidCategoryConstants() {
        // Ensure all category constants can construct a node without error
        for (String category : SearchLatencyBreakdownNode.VALID_CATEGORIES) {
            SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode("test_" + category, category);
            assertEquals(category, node.getCategory());
        }
    }

    // --- EndOffsetNanos Invariant Tests ---

    public void testEndOffsetEquals_StartPlusDuration() {
        long startOffset = 5000L;
        long duration = 12000L;
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
            "Test",
            SearchLatencyBreakdownNode.CATEGORY_COORDINATOR,
            startOffset,
            duration
        );
        assertEquals(startOffset + duration, node.getEndOffsetNanos());
    }

    public void testSetTimingUpdatesAllFields() {
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode("Test", SearchLatencyBreakdownNode.CATEGORY_PHASE);
        node.setTiming(1000L, 5000L);
        assertEquals(1000L, node.getStartOffsetNanos());
        assertEquals(5000L, node.getEndOffsetNanos());
        assertEquals(4000L, node.getDurationNanos());
    }

    // --- Helper methods ---

    private SearchLatencyBreakdownNode roundTrip(SearchLatencyBreakdownNode original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                return new SearchLatencyBreakdownNode(in);
            }
        }
    }

    private Map<String, Object> toMap(SearchLatencyBreakdownNode node) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            node.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            return org.opensearch.common.xcontent.XContentHelper.convertToMap(
                org.opensearch.core.xcontent.MediaTypeRegistry.JSON.xContent(),
                org.opensearch.core.common.bytes.BytesReference.bytes(builder).streamInput(),
                false
            );
        }
    }
}
