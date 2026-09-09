/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Randomized tests for {@link AdaptiveBudgetStats} serialization round-trip and JSON rendering.
 *
 * <p><b>Validates: Requirements 2.6, 2.7</b> — AdaptiveBudgetStats survives StreamOutput/StreamInput
 * round-trip and renders correct JSON.
 */
public class AdaptiveBudgetStatsTests extends OpenSearchTestCase {

    private static final int TRIES = 200;

    // ---- Generators ----

    private long nonNegLong() {
        return randomLongBetween(0, Long.MAX_VALUE / 2);
    }

    private AdaptiveBudgetStats randomAdaptiveBudgetStats() {
        return new AdaptiveBudgetStats(nonNegLong(), nonNegLong());
    }

    // ---- Property: StreamOutput/StreamInput round trip produces equal object ----

    public void testRoundTripPreservesAllFields() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            AdaptiveBudgetStats original = randomAdaptiveBudgetStats();

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            AdaptiveBudgetStats deserialized = new AdaptiveBudgetStats(in);

            // Verify equality
            assertEquals("StreamOutput/StreamInput round trip must preserve all fields", original, deserialized);
            assertEquals("hashCode must be consistent after round trip", original.hashCode(), deserialized.hashCode());

            // Verify individual fields
            assertEquals("fallbacks mismatch", original.fallbacks, deserialized.fallbacks);
            assertEquals("rejections mismatch", original.rejections, deserialized.rejections);
        }
    }

    // ---- Property: toXContent renders correct JSON shape ----

    @SuppressWarnings("unchecked")
    public void testToXContentRoundTrip() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            AdaptiveBudgetStats original = randomAdaptiveBudgetStats();

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            Map<String, Object> root = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();

            // adaptive_budget wrapper must be present
            assertTrue("adaptive_budget key must be present", root.containsKey("adaptive_budget"));
            Map<String, Object> budgetNode = (Map<String, Object>) root.get("adaptive_budget");
            assertNotNull("adaptive_budget object must not be null", budgetNode);
            assertEquals("adaptive_budget must have exactly 2 fields", 2, budgetNode.size());

            // Verify field values
            assertEquals(original.fallbacks, ((Number) budgetNode.get("fallbacks")).longValue());
            assertEquals(original.rejections, ((Number) budgetNode.get("rejections")).longValue());
        }
    }

    // ---- Edge case: zero values ----

    public void testZeroValues() throws IOException {
        AdaptiveBudgetStats zeroed = new AdaptiveBudgetStats(0L, 0L);

        BytesStreamOutput out = new BytesStreamOutput();
        zeroed.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        AdaptiveBudgetStats deserialized = new AdaptiveBudgetStats(in);

        assertEquals(zeroed, deserialized);
        assertEquals(0L, deserialized.fallbacks);
        assertEquals(0L, deserialized.rejections);
    }
}
