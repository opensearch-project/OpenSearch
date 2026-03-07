/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.metrics;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MinAggregation;
import org.opensearch.protobufs.ValueType;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.aggregation.metrics.MinAggregationProtoUtils;

public class MinAggregationProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithField() {
        MinAggregation proto = MinAggregation.newBuilder().setField("price").build();

        MinAggregationBuilder result = MinAggregationProtoUtils.fromProto("min_price", proto);

        assertNotNull(result);
        assertEquals("min_price", result.getName());
        assertEquals("price", result.field());
    }

    public void testFromProtoWithFieldAndFormat() {
        MinAggregation proto = MinAggregation.newBuilder().setField("price").setFormat("0.00").build();

        MinAggregationBuilder result = MinAggregationProtoUtils.fromProto("min_price", proto);

        assertEquals("price", result.field());
        assertEquals("0.00", result.format());
    }

    public void testFromProtoWithMissingValue() {
        FieldValue missingValue = FieldValue.newBuilder()
            .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setDoubleValue(0.0).build())
            .build();

        MinAggregation proto = MinAggregation.newBuilder().setField("price").setMissing(missingValue).build();

        MinAggregationBuilder result = MinAggregationProtoUtils.fromProto("min_price", proto);

        assertEquals(0.0, result.missing());
    }

    public void testFromProtoWithoutFieldOrScript() {
        MinAggregation proto = MinAggregation.newBuilder().build();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> MinAggregationProtoUtils.fromProto("test", proto));
        assertTrue(ex.getMessage().contains("field") || ex.getMessage().contains("script"));
    }

    public void testFromProtoWithNullProto() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> MinAggregationProtoUtils.fromProto("test", null));
        assertTrue(ex.getMessage().contains("must not be null"));
    }

    public void testFromProtoWithValueTypeUnsignedLong() {
        MinAggregation proto = MinAggregation.newBuilder()
            .setField("price")
            .setValueType(ValueType.VALUE_TYPE_UNSIGNED_LONG)
            .build();

        MinAggregationBuilder result = MinAggregationProtoUtils.fromProto("min_price", proto);

        assertNotNull(result);
        assertEquals("price", result.field());
        assertNotNull(result.userValueTypeHint());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.UNSIGNED_LONG, result.userValueTypeHint());
    }

    public void testFromProtoWithValueTypeLong() {
        MinAggregation proto = MinAggregation.newBuilder()
            .setField("count")
            .setValueType(ValueType.VALUE_TYPE_LONG)
            .build();

        MinAggregationBuilder result = MinAggregationProtoUtils.fromProto("min_count", proto);

        assertEquals("count", result.field());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.LONG, result.userValueTypeHint());
    }

    public void testFromProtoWithValueTypeDouble() {
        MinAggregation proto = MinAggregation.newBuilder()
            .setField("rating")
            .setValueType(ValueType.VALUE_TYPE_DOUBLE)
            .build();

        MinAggregationBuilder result = MinAggregationProtoUtils.fromProto("min_rating", proto);

        assertEquals("rating", result.field());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.DOUBLE, result.userValueTypeHint());
    }

    public void testFromProtoWithoutValueType() {
        MinAggregation proto = MinAggregation.newBuilder().setField("price").build();

        MinAggregationBuilder result = MinAggregationProtoUtils.fromProto("min_price", proto);

        assertEquals("price", result.field());
        assertNull(result.userValueTypeHint());
    }
}
