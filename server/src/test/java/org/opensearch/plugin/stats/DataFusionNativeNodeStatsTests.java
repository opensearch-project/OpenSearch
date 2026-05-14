/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests for {@link DataFusionNativeNodeStats} covering serialization round-trip (property test),
 * constructor correctness, XContent output, and equals/hashCode contract.
 */
public class DataFusionNativeNodeStatsTests extends OpenSearchTestCase {

    // ─── Property Test: Serialization Round-Trip (Property 3) ───────────────────

    /**
     * Property 3: DataFusionNativeNodeStats serialization round-trip.
     *
     * For any valid long values for all 4 fields, constructing a DataFusionNativeNodeStats,
     * serializing via writeTo, and deserializing via the StreamInput constructor SHALL produce
     * an equal instance.
     *
     * Uses non-negative longs since the implementation uses writeVLong (variable-length encoding
     * for non-negative values) and these fields represent counters which are always >= 0.
     *
     * Validates: Requirements 6.4, 6.5, 6.8
     */
    public void testSerializationRoundTrip() throws IOException {
        for (int i = 0; i < 100; i++) {
            long searchTaskCurrent = randomNonNegativeLong();
            long searchTaskTotal = randomNonNegativeLong();
            long shardTaskCurrent = randomNonNegativeLong();
            long shardTaskTotal = randomNonNegativeLong();

            DataFusionNativeNodeStats original = new DataFusionNativeNodeStats(
                searchTaskCurrent,
                searchTaskTotal,
                shardTaskCurrent,
                shardTaskTotal
            );

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    DataFusionNativeNodeStats deserialized = new DataFusionNativeNodeStats(in);
                    assertEquals(
                        "Round-trip failed for values: ["
                            + searchTaskCurrent
                            + ", "
                            + searchTaskTotal
                            + ", "
                            + shardTaskCurrent
                            + ", "
                            + shardTaskTotal
                            + "]",
                        original,
                        deserialized
                    );
                    assertEquals("hashCode mismatch after round-trip", original.hashCode(), deserialized.hashCode());
                }
            }
        }
    }

    // ─── Unit Tests ─────────────────────────────────────────────────────────────

    /**
     * Test that the constructor stores all 4 fields correctly.
     *
     * Validates: Requirement 6.3
     */
    public void testConstructorStoresFieldsCorrectly() {
        DataFusionNativeNodeStats stats = new DataFusionNativeNodeStats(10, 20, 30, 40);

        assertEquals(10, stats.getNativeSearchTaskCurrent());
        assertEquals(20, stats.getNativeSearchTaskTotal());
        assertEquals(30, stats.getNativeSearchShardTaskCurrent());
        assertEquals(40, stats.getNativeSearchShardTaskTotal());
    }

    /**
     * Test that toXContent produces the expected JSON structure with native_search_task
     * and native_search_shard_task sub-objects.
     *
     * Validates: Requirement 6.6
     */
    public void testToXContentProducesExpectedJsonStructure() throws IOException {
        DataFusionNativeNodeStats stats = new DataFusionNativeNodeStats(2, 147, 5, 892);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();

        // Verify the JSON contains the expected structure
        String expected = "{\"native_search_task\":{\"current_count_post_cancel\":2,\"total_count_post_cancel\":147},"
            + "\"native_search_shard_task\":{\"current_count_post_cancel\":5,\"total_count_post_cancel\":892}}";
        assertEquals(expected, json);
    }

    /**
     * Test that toXContent handles zero values correctly.
     *
     * Validates: Requirement 6.6
     */
    public void testToXContentWithZeroValues() throws IOException {
        DataFusionNativeNodeStats stats = new DataFusionNativeNodeStats(0, 0, 0, 0);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();

        String expected = "{\"native_search_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":0},"
            + "\"native_search_shard_task\":{\"current_count_post_cancel\":0,\"total_count_post_cancel\":0}}";
        assertEquals(expected, json);
    }

    /**
     * Test equals: same values produce equal instances.
     *
     * Validates: Requirement 6.7
     */
    public void testEqualsWithSameValues() {
        DataFusionNativeNodeStats stats1 = new DataFusionNativeNodeStats(1, 2, 3, 4);
        DataFusionNativeNodeStats stats2 = new DataFusionNativeNodeStats(1, 2, 3, 4);

        assertEquals(stats1, stats2);
        assertEquals(stats2, stats1);
    }

    /**
     * Test equals: reflexive property.
     *
     * Validates: Requirement 6.7
     */
    public void testEqualsReflexive() {
        DataFusionNativeNodeStats stats = new DataFusionNativeNodeStats(5, 10, 15, 20);
        assertEquals(stats, stats);
    }

    /**
     * Test equals: different values produce non-equal instances.
     *
     * Validates: Requirement 6.7
     */
    public void testEqualsWithDifferentValues() {
        DataFusionNativeNodeStats stats1 = new DataFusionNativeNodeStats(1, 2, 3, 4);

        // Differ in each field
        assertNotEquals(stats1, new DataFusionNativeNodeStats(99, 2, 3, 4));
        assertNotEquals(stats1, new DataFusionNativeNodeStats(1, 99, 3, 4));
        assertNotEquals(stats1, new DataFusionNativeNodeStats(1, 2, 99, 4));
        assertNotEquals(stats1, new DataFusionNativeNodeStats(1, 2, 3, 99));
    }

    /**
     * Test equals: null and different type.
     *
     * Validates: Requirement 6.7
     */
    public void testEqualsNullAndDifferentType() {
        DataFusionNativeNodeStats stats = new DataFusionNativeNodeStats(1, 2, 3, 4);
        assertNotEquals(null, stats);
        assertNotEquals("not a stats object", stats);
    }

    /**
     * Test hashCode: equal objects have equal hash codes.
     *
     * Validates: Requirement 6.7
     */
    public void testHashCodeConsistentWithEquals() {
        DataFusionNativeNodeStats stats1 = new DataFusionNativeNodeStats(7, 14, 21, 28);
        DataFusionNativeNodeStats stats2 = new DataFusionNativeNodeStats(7, 14, 21, 28);

        assertEquals(stats1, stats2);
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    /**
     * Test hashCode: different objects likely have different hash codes.
     *
     * Validates: Requirement 6.7
     */
    public void testHashCodeDiffersForDifferentValues() {
        DataFusionNativeNodeStats stats1 = new DataFusionNativeNodeStats(1, 2, 3, 4);
        DataFusionNativeNodeStats stats2 = new DataFusionNativeNodeStats(5, 6, 7, 8);

        // While not strictly required by the contract, different values should produce different hashes
        assertNotEquals(stats1.hashCode(), stats2.hashCode());
    }
}
