/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.metrics;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.protobufs.ValueType;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.aggregation.metrics.MaxAggregationProtoUtils;

public class MaxAggregationProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithField() {
        MaxAggregation proto = MaxAggregation.newBuilder().setField("price").build();

        MaxAggregationBuilder result = MaxAggregationProtoUtils.fromProto("max_price", proto);

        assertNotNull(result);
        assertEquals("max_price", result.getName());
        assertEquals("price", result.field());
    }

    public void testFromProtoWithFieldAndFormat() {
        MaxAggregation proto = MaxAggregation.newBuilder().setField("price").setFormat("0.00").build();

        MaxAggregationBuilder result = MaxAggregationProtoUtils.fromProto("max_price", proto);

        assertEquals("price", result.field());
        assertEquals("0.00", result.format());
    }

    public void testFromProtoWithMissingValue() {
        FieldValue missingValue = FieldValue.newBuilder()
            .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setDoubleValue(0.0).build())
            .build();

        MaxAggregation proto = MaxAggregation.newBuilder().setField("price").setMissing(missingValue).build();

        MaxAggregationBuilder result = MaxAggregationProtoUtils.fromProto("max_price", proto);

        assertEquals(0.0, result.missing());
    }

    public void testFromProtoWithoutFieldOrScript() {
        MaxAggregation proto = MaxAggregation.newBuilder().build();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> MaxAggregationProtoUtils.fromProto("test", proto));
        assertTrue(ex.getMessage().contains("field") || ex.getMessage().contains("script"));
    }

    public void testFromProtoWithNullProto() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> MaxAggregationProtoUtils.fromProto("test", null));
        assertTrue(ex.getMessage().contains("must not be null"));
    }

    public void testFromProtoWithValueTypeUnsignedLong() {
        MaxAggregation proto = MaxAggregation.newBuilder()
            .setField("price")
            .setValueType(ValueType.VALUE_TYPE_UNSIGNED_LONG)
            .build();

        MaxAggregationBuilder result = MaxAggregationProtoUtils.fromProto("max_price", proto);

        assertNotNull(result);
        assertEquals("price", result.field());
        assertNotNull(result.userValueTypeHint());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.UNSIGNED_LONG, result.userValueTypeHint());
    }

    public void testFromProtoWithValueTypeLong() {
        MaxAggregation proto = MaxAggregation.newBuilder()
            .setField("count")
            .setValueType(ValueType.VALUE_TYPE_LONG)
            .build();

        MaxAggregationBuilder result = MaxAggregationProtoUtils.fromProto("max_count", proto);

        assertEquals("count", result.field());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.LONG, result.userValueTypeHint());
    }

    public void testFromProtoWithValueTypeDouble() {
        MaxAggregation proto = MaxAggregation.newBuilder()
            .setField("rating")
            .setValueType(ValueType.VALUE_TYPE_DOUBLE)
            .build();

        MaxAggregationBuilder result = MaxAggregationProtoUtils.fromProto("max_rating", proto);

        assertEquals("rating", result.field());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.DOUBLE, result.userValueTypeHint());
    }

    public void testFromProtoWithoutValueType() {
        MaxAggregation proto = MaxAggregation.newBuilder().setField("price").build();

        MaxAggregationBuilder result = MaxAggregationProtoUtils.fromProto("max_price", proto);

        assertEquals("price", result.field());
        assertNull(result.userValueTypeHint());
    }
}
