/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class TermsAggregationProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithField() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("test_field").build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("my_terms", proto);

        assertNotNull(result);
        assertEquals("my_terms", result.getName());
        assertEquals("test_field", result.field());
    }

    public void testFromProtoWithSize() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setSize(25).build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertEquals(25, result.size());
    }

    public void testFromProtoWithShardSize() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setShardSize(100).build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertEquals(100, result.shardSize());
    }

    public void testFromProtoWithMinDocCount() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setMinDocCount(5).build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertEquals(5, result.minDocCount());
    }

    public void testFromProtoWithShardMinDocCount() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setShardMinDocCount(2).build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertEquals(2, result.shardMinDocCount());
    }

    public void testFromProtoWithShowTermDocCountError() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setShowTermDocCountError(true).build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertTrue(result.showTermDocCountError());
    }

    public void testFromProtoWithOrder() {
        Map<String, SortOrder> orderMap = new HashMap<>();
        orderMap.put("_count", SortOrder.SORT_ORDER_DESC);

        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").putAllOrder(orderMap).build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertNotNull(result.order());
        // Note: Can't easily test BucketOrder equality, but we verify it doesn't throw
    }

    public void testFromProtoWithCollectMode() {
        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .setCollectMode(TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_BREADTH_FIRST)
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertEquals(Aggregator.SubAggCollectionMode.BREADTH_FIRST, result.collectMode());
    }

    public void testFromProtoWithMissingValue() {
        FieldValue missingValue = FieldValue.newBuilder().setString("N/A").build();

        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setMissing(missingValue).build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        // Missing value is stored as BytesRef internally
        assertNotNull(result.missing());
        assertTrue(result.missing().toString().contains("N/A") || result.missing().toString().contains("4e 2f 41"));
    }

    public void testFromProtoWithValueType() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setValueType("string").build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertNotNull(result.userValueTypeHint());
    }

    public void testFromProtoWithFormat() {
        TermsAggregation proto = TermsAggregation.newBuilder().setField("date_field").setFormat("yyyy-MM-dd").build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertEquals("yyyy-MM-dd", result.format());
    }

    public void testFromProtoWithoutFieldOrScript() {
        TermsAggregation proto = TermsAggregation.newBuilder().setSize(10).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> TermsAggregationProtoUtils.fromProto("test", proto)
        );
        assertTrue(ex.getMessage().contains("field") || ex.getMessage().contains("script"));
    }

    public void testFromProtoWithNullProto() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> TermsAggregationProtoUtils.fromProto("test", null)
        );
        assertTrue(ex.getMessage().contains("must not be null"));
    }

    public void testFromProtoWithAllFields() {
        // Create a comprehensive proto with many fields set
        Map<String, SortOrder> orderMap = new HashMap<>();
        orderMap.put("_count", SortOrder.SORT_ORDER_DESC);

        FieldValue missingValue = FieldValue.newBuilder().setString("N/A").build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .setSize(50)
            .setShardSize(200)
            .setMinDocCount(10)
            .setShardMinDocCount(5)
            .setShowTermDocCountError(true)
            .setCollectMode(TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_DEPTH_FIRST)
            .putAllOrder(orderMap)
            .setMissing(missingValue)
            .setValueType("string")
            .setFormat("###.##")
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("comprehensive_test", proto);

        // Verify all fields
        assertNotNull(result);
        assertEquals("comprehensive_test", result.getName());
        assertEquals("category", result.field());
        assertEquals(50, result.size());
        assertEquals(200, result.shardSize());
        assertEquals(10, result.minDocCount());
        assertEquals(5, result.shardMinDocCount());
        assertTrue(result.showTermDocCountError());
        assertEquals(Aggregator.SubAggCollectionMode.DEPTH_FIRST, result.collectMode());
        assertNotNull(result.missing());  // Missing value present (stored as BytesRef)
        assertEquals("###.##", result.format());
    }
}
