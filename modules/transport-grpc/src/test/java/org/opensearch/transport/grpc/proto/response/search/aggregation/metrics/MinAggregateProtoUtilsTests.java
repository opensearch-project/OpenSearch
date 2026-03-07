/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.metrics;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.NullValue;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link MinAggregateProtoUtils} verifying it correctly mirrors
 * {@link InternalMin#doXContentBody} behavior.
 */
public class MinAggregateProtoUtilsTests extends OpenSearchTestCase {

    // ========================================
    // Line 100-101: hasValue and value field tests
    // ========================================

    public void testToProtoWithValidValue() {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoWithPositiveInfinity() {
        // When no documents match, min returns POSITIVE_INFINITY (REST line 100)
        InternalMin internalMin = new InternalMin("min_price", Double.POSITIVE_INFINITY, DocValueFormat.RAW, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as null", result.getValue().hasNullValue());
        assertEquals("Value should be NULL_VALUE", NullValue.NULL_VALUE_NULL, result.getValue().getNullValue());
        assertFalse("value_as_string should NOT be set for infinity", result.hasValueAsString());
    }

    public void testToProtoWithNegativeInfinity() {
        // Edge case: NEGATIVE_INFINITY should also be treated as null
        InternalMin internalMin = new InternalMin("min_price", Double.NEGATIVE_INFINITY, DocValueFormat.RAW, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as null", result.getValue().hasNullValue());
        assertEquals("Value should be NULL_VALUE", NullValue.NULL_VALUE_NULL, result.getValue().getNullValue());
        assertFalse("value_as_string should NOT be set for infinity", result.hasValueAsString());
    }

    public void testToProtoWithZero() {
        // Edge case: Zero is a valid value (specific case from original bug report)
        InternalMin internalMin = new InternalMin("min_discount", 0.0, DocValueFormat.RAW, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should be 0.0", 0.0, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoWithNegativeValue() {
        InternalMin internalMin = new InternalMin("min_temp", -10.5, DocValueFormat.RAW, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should be -10.5", -10.5, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    // ========================================
    // Lines 102-104: value_as_string field tests
    // ========================================

    public void testToProtoWithRawFormat() {
        // REST line 102: format != DocValueFormat.RAW condition
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoWithDecimalFormat() {
        // REST line 103: format.format(min).toString()
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", 10.5, format, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getValue().getDouble(), 0.001);
        assertTrue("value_as_string SHOULD be set with custom format", result.hasValueAsString());
        assertEquals("value_as_string should be formatted", "10.50", result.getValueAsString());
    }

    public void testToProtoWithDecimalFormatLargeNumber() {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", 1234.5, format, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 1234.5, result.getValue().getDouble(), 0.001);
        assertTrue("value_as_string SHOULD be set with custom format", result.hasValueAsString());
        assertEquals("value_as_string should be formatted", "1234.50", result.getValueAsString());
    }

    public void testToProtoWithInfinityAndCustomFormat() {
        // REST line 102: hasValue condition prevents value_as_string for infinity
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", Double.POSITIVE_INFINITY, format, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as null", result.getValue().hasNullValue());
        assertFalse("value_as_string should NOT be set even with custom format when value is infinity", result.hasValueAsString());
    }

    // ========================================
    // Metadata tests
    // Note: Metadata is handled centrally by AggregateProtoUtils.toProto()
    // These tests verify that toProto() does NOT include metadata (as expected)
    // ========================================

    public void testToProtoBuilderWithMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("color", "red");
        metadata.put("priority", 1);

        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, metadata);

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        // Metadata is NOT included - it's handled by AggregateProtoUtils.toProto()
        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoBuilderWithEmptyMetadata() {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set", result.hasValue());
    }

    public void testToProtoBuilderWithNullMetadata() {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, null);

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set", result.hasValue());
    }

    // ========================================
    // Combined scenarios
    // ========================================

    public void testToProtoBuilderWithFormattedValue() {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", 5.25, format, new HashMap<>());

        Aggregate result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 5.25, result.getValue().getDouble(), 0.001);
        assertTrue("value_as_string SHOULD be set", result.hasValueAsString());
        assertEquals("value_as_string should be formatted", "5.25", result.getValueAsString());
    }
}
