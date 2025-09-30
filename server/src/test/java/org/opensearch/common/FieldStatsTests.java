/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldStatsTests extends OpenSearchTestCase {
    public void testAdd() {
        Map<String, Map<String, Long>> values1 = new HashMap<>();
        values1.put("field1", new HashMap<>(Map.of("memory_size_in_bytes", 10L, "item_count", 2L)));
        values1.put("field2", new HashMap<>(Map.of("memory_size_in_bytes", 1L, "item_count", 3L)));
        FieldStats stats1 = new FieldStats(statNames(), isMemory(), readableKeys(), values1);

        Map<String, Map<String, Long>> values2 = new HashMap<>();
        values2.put("field1", new HashMap<>(Map.of("memory_size_in_bytes", 5L, "item_count", 7L)));
        values2.put("field3", new HashMap<>(Map.of("memory_size_in_bytes", 100L, "item_count", 1L)));
        FieldStats stats2 = new FieldStats(statNames(), isMemory(), readableKeys(), values2);

        stats1.add(stats2);

        Map<String, Map<String, Long>> expected = new HashMap<>();
        expected.put("field1", Map.of("memory_size_in_bytes", 15L, "item_count", 9L));
        expected.put("field2", Map.of("memory_size_in_bytes", 1L, "item_count", 3L));
        expected.put("field3", Map.of("memory_size_in_bytes", 100L, "item_count", 1L));
        FieldStats sExpected = new FieldStats(statNames(), isMemory(), readableKeys(), expected);

        assertEquals(sExpected, stats1);
        assertEquals(sExpected.hashCode(), stats1.hashCode());

        // Assert we can't add if there's a mismatch
        FieldStats mismatched = new FieldStats(statNames(), Map.of("memory_size_in_bytes", false, "item_count", false), Map.of(), values2);
        assertThrows(IllegalArgumentException.class, () -> stats1.add(mismatched));
    }

    public void testSerializeRoundTrip() throws IOException {
        Map<String, Map<String, Long>> values = new LinkedHashMap<>();
        values.put("field1", Map.of("memory_size_in_bytes", 42L, "item_count", 7L));
        values.put("field2", Map.of("memory_size_in_bytes", 0L, "item_count", 0L));
        values.put("field3", Map.of("memory_size_in_bytes", 1024L, "item_count", 1L));

        FieldStats stats = new FieldStats(statNames(), isMemory(), readableKeys(), values);

        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();

        FieldStats deser = new FieldStats(in);
        assertEquals(stats, deser);
        assertEquals(stats.hashCode(), deser.hashCode());
    }

    public void testMissingStatForField() {
        Map<String, Map<String, Long>> bad = new HashMap<>();
        // Missing "item_count" for field1
        bad.put("field1", Map.of("memory_size_in_bytes", 1L));
        expectThrows(AssertionError.class, () -> new FieldStats(statNames(), isMemory(), readableKeys(), bad));
    }

    public void testUnexpectedStatForField() {
        Map<String, Map<String, Long>> bad = new HashMap<>();
        // Extra key "evictions"
        bad.put("field1", Map.of("memory_size_in_bytes", 1L, "item_count", 2L, "evictions", 3L));
        expectThrows(AssertionError.class, () -> new FieldStats(statNames(), isMemory(), readableKeys(), bad));
    }

    public void testNullValuesRejected() {
        Map<String, Long> badValues = new HashMap<>();
        badValues.put("memory_size_in_bytes", null);
        badValues.put("item_count", 1L);
        expectThrows(AssertionError.class, () -> new FieldStats(statNames(), isMemory(), readableKeys(), Map.of("field1", badValues)));
    }

    public void testMissingIsMemory() {
        Map<String, Map<String, Long>> values = new HashMap<>();
        values.put("field1", Map.of("memory_size_in_bytes", 10L, "item_count", 5L));

        expectThrows(AssertionError.class, () -> new FieldStats(statNames(), isMemory(), Map.of(), values));
    }

    private static List<String> statNames() {
        return List.of("memory_size_in_bytes", "item_count");
    }

    private static Map<String, Boolean> isMemory() {
        return Map.of("memory_size_in_bytes", true, "item_count", false);
    }

    private static Map<String, String> readableKeys() {
        return Map.of("memory_size_in_bytes", "memory_size");
    }
}
