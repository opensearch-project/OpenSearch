/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.metrics;

import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.SingleMetricAggregateBase;
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

    public void testToProtoWithValidValue() {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoWithPositiveInfinity() {
        InternalMin internalMin = new InternalMin("min_price", Double.POSITIVE_INFINITY, DocValueFormat.RAW, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as null", result.getValue().hasNullValue());
        assertEquals("Value should be NULL_VALUE", NullValue.NULL_VALUE_NULL, result.getValue().getNullValue());
        assertFalse("value_as_string should NOT be set for infinity", result.hasValueAsString());
    }

    public void testToProtoWithNegativeInfinity() {
        InternalMin internalMin = new InternalMin("min_price", Double.NEGATIVE_INFINITY, DocValueFormat.RAW, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as null", result.getValue().hasNullValue());
        assertEquals("Value should be NULL_VALUE", NullValue.NULL_VALUE_NULL, result.getValue().getNullValue());
        assertFalse("value_as_string should NOT be set for infinity", result.hasValueAsString());
    }

    public void testToProtoWithZero() {
        InternalMin internalMin = new InternalMin("min_discount", 0.0, DocValueFormat.RAW, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should be 0.0", 0.0, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoWithNegativeValue() {
        InternalMin internalMin = new InternalMin("min_temp", -10.5, DocValueFormat.RAW, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should be -10.5", -10.5, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoWithRawFormat() {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getValue().getDouble(), 0.001);
        assertFalse("value_as_string should NOT be set with RAW format", result.hasValueAsString());
    }

    public void testToProtoWithDecimalFormat() {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", 10.5, format, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getValue().getDouble(), 0.001);
        assertTrue("value_as_string SHOULD be set with custom format", result.hasValueAsString());
        assertEquals("value_as_string should be formatted", "10.50", result.getValueAsString());
    }

    public void testToProtoWithDecimalFormatLargeNumber() {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", 1234.5, format, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 1234.5, result.getValue().getDouble(), 0.001);
        assertTrue("value_as_string SHOULD be set with custom format", result.hasValueAsString());
        assertEquals("value_as_string should be formatted", "1234.50", result.getValueAsString());
    }

    public void testToProtoWithInfinityAndCustomFormat() {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", Double.POSITIVE_INFINITY, format, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as null", result.getValue().hasNullValue());
        assertFalse("value_as_string should NOT be set even with custom format when value is infinity", result.hasValueAsString());
    }

    public void testToProtoWithMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("color", "red");
        metadata.put("priority", 1);

        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, metadata);

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain color", result.getMeta().getFieldsMap().containsKey("color"));
        assertTrue("Metadata should contain priority", result.getMeta().getFieldsMap().containsKey("priority"));
    }

    public void testToProtoWithEmptyMetadata() {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set", result.hasValue());
        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testToProtoWithNullMetadata() {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, null);

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set", result.hasValue());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testToProtoWithFormattedValue() {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        InternalMin internalMin = new InternalMin("min_price", 5.25, format, new HashMap<>());

        SingleMetricAggregateBase result = MinAggregateProtoUtils.toProto(internalMin);

        assertTrue("Value should be set as double", result.getValue().hasDouble());
        assertEquals("Value should match", 5.25, result.getValue().getDouble(), 0.001);
        assertTrue("value_as_string SHOULD be set", result.hasValueAsString());
        assertEquals("value_as_string should be formatted", "5.25", result.getValueAsString());
    }
}
