/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FieldFileStatsTests extends OpenSearchTestCase {

    private FieldFileStats createStats() {
        Map<String, Map<String, Long>> stats = new HashMap<>();

        Map<String, Long> field1Extensions = new HashMap<>();
        field1Extensions.put("tim", 1000L);
        field1Extensions.put("tip", 500L);
        stats.put("field1", field1Extensions);

        Map<String, Long> field2Extensions = new HashMap<>();
        field2Extensions.put("dvd", 2000L);
        stats.put("field2", field2Extensions);

        return new FieldFileStats(stats);
    }

    public void testEmptyStats() {
        Map<String, Map<String, Long>> empty = new HashMap<>();
        FieldFileStats stats = new FieldFileStats(empty);
        assertEquals(0, stats.getFieldCount());
    }

    public void testGetField() {
        FieldFileStats stats = createStats();

        Map<String, Long> field1 = stats.getField("field1");
        assertNotNull(field1);
        assertEquals(Long.valueOf(1000L), field1.get("tim"));
        assertEquals(Long.valueOf(500L), field1.get("tip"));

        Map<String, Long> field2 = stats.getField("field2");
        assertNotNull(field2);
        assertEquals(Long.valueOf(2000L), field2.get("dvd"));

        assertNull(stats.getField("nonexistent"));
    }

    public void testGet() {
        FieldFileStats stats = createStats();

        assertEquals(1000L, stats.get("field1", "tim"));
        assertEquals(500L, stats.get("field1", "tip"));
        assertEquals(2000L, stats.get("field2", "dvd"));

        assertEquals(0L, stats.get("field1", "nonexistent"));
        assertEquals(0L, stats.get("nonexistent", "tim"));
    }

    public void testContainsField() {
        FieldFileStats stats = createStats();

        assertTrue(stats.containsField("field1"));
        assertTrue(stats.containsField("field2"));
        assertFalse(stats.containsField("nonexistent"));
    }

    public void testGetFieldCount() {
        FieldFileStats stats = createStats();
        assertEquals(2, stats.getFieldCount());

        Map<String, Map<String, Long>> empty = new HashMap<>();
        FieldFileStats emptyStats = new FieldFileStats(empty);
        assertEquals(0, emptyStats.getFieldCount());
    }

    public void testAddFieldFileStats() {
        Map<String, Map<String, Long>> stats1Map = new HashMap<>();
        Map<String, Long> field1Ext = new HashMap<>();
        field1Ext.put("tim", 1000L);
        stats1Map.put("field1", field1Ext);

        Map<String, Long> field2Ext = new HashMap<>();
        field2Ext.put("dvd", 2000L);
        stats1Map.put("field2", field2Ext);

        FieldFileStats stats1 = new FieldFileStats(stats1Map);

        Map<String, Map<String, Long>> stats2Map = new HashMap<>();
        Map<String, Long> field1Ext2 = new HashMap<>();
        field1Ext2.put("tim", 500L); // Should be added to existing
        stats2Map.put("field1", field1Ext2);

        Map<String, Long> field3Ext = new HashMap<>();
        field3Ext.put("doc", 3000L); // New field
        stats2Map.put("field3", field3Ext);

        FieldFileStats stats2 = new FieldFileStats(stats2Map);

        stats1.add(stats2);

        assertEquals(3, stats1.getFieldCount());
        assertEquals(1500L, stats1.get("field1", "tim")); // 1000 + 500
        assertEquals(2000L, stats1.get("field2", "dvd"));
        assertEquals(3000L, stats1.get("field3", "doc"));
    }

    public void testCopy() {
        FieldFileStats original = createStats();
        FieldFileStats copy = original.copy();

        assertNotSame(original, copy);
        assertEquals(original.getFieldCount(), copy.getFieldCount());
        assertEquals(original.get("field1", "tim"), copy.get("field1", "tim"));
        assertEquals(original.get("field1", "tip"), copy.get("field1", "tip"));
        assertEquals(original.get("field2", "dvd"), copy.get("field2", "dvd"));

        // Modifying copy should not affect original
        Map<String, Map<String, Long>> newFieldMap = new HashMap<>();
        Map<String, Long> newFieldExt = new HashMap<>();
        newFieldExt.put("nvd", 4000L);
        newFieldMap.put("field3", newFieldExt);
        FieldFileStats additionalStats = new FieldFileStats(newFieldMap);

        copy.add(additionalStats);

        assertEquals(2, original.getFieldCount());
        assertEquals(3, copy.getFieldCount());
        assertFalse(original.containsField("field3"));
        assertTrue(copy.containsField("field3"));
    }

    public void testSerialization() throws IOException {
        FieldFileStats original = createStats();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        FieldFileStats deserialized = new FieldFileStats(in);

        assertEquals(original.getFieldCount(), deserialized.getFieldCount());
        assertEquals(original.get("field1", "tim"), deserialized.get("field1", "tim"));
        assertEquals(original.get("field1", "tip"), deserialized.get("field1", "tip"));
        assertEquals(original.get("field2", "dvd"), deserialized.get("field2", "dvd"));
        assertEquals(original, deserialized);
    }

    public void testSerializationEmpty() throws IOException {
        Map<String, Map<String, Long>> empty = new HashMap<>();
        FieldFileStats original = new FieldFileStats(empty);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        FieldFileStats deserialized = new FieldFileStats(in);

        assertEquals(0, deserialized.getFieldCount());
        assertEquals(original, deserialized);
    }

    public void testToXContent() throws IOException {
        FieldFileStats stats = createStats();

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, "field_level_file_sizes");
        builder.endObject();

        String json = builder.toString();

        assertTrue(json.contains("field_level_file_sizes"));
        assertTrue(json.contains("field1"));
        assertTrue(json.contains("field2"));
        assertTrue(json.contains("tim"));
        assertTrue(json.contains("tip"));
        assertTrue(json.contains("dvd"));
        assertTrue(json.contains("size_in_bytes"));
        assertTrue(json.contains("\"size_in_bytes\" : 1000"));
        assertTrue(json.contains("\"size_in_bytes\" : 500"));
        assertTrue(json.contains("\"size_in_bytes\" : 2000"));
    }

    public void testToXContentEmpty() throws IOException {
        Map<String, Map<String, Long>> empty = new HashMap<>();
        FieldFileStats stats = new FieldFileStats(empty);

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, "field_level_file_sizes");
        builder.endObject();

        String json = builder.toString();

        assertTrue(json.contains("field_level_file_sizes"));
        assertTrue(json.contains("{ }"));
    }

    public void testIterator() {
        FieldFileStats stats = createStats();

        int count = 0;
        for (Map.Entry<String, Map<String, Long>> entry : stats) {
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());
            count++;
        }
        assertEquals(2, count);
    }

    public void testEquals() {
        FieldFileStats stats1 = createStats();
        FieldFileStats stats2 = createStats();
        FieldFileStats stats3 = new FieldFileStats(new HashMap<>());

        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats1, stats1);
        assertNotEquals(stats1, null);
        assertNotEquals(stats1, "not a FieldFileStats");
    }

    public void testHashCode() {
        FieldFileStats stats1 = createStats();
        FieldFileStats stats2 = createStats();

        assertEquals(stats1.hashCode(), stats2.hashCode());
    }
}
