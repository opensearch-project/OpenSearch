/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms;

import org.junit.Ignore;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.protobufs.MinAggregation;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.SortOrderMap;
import org.opensearch.protobufs.SortOrderSingleMap;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms.TermsAggregationProtoUtils;

import java.util.Collection;
import java.util.List;

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

    public void testFromProtoWithSingleOrder() {
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .addOrder(orderMap)
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertNotNull("Order should not be null", result.order());
        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals("Order should be COUNT_DESC with key tiebreaker",
            BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true)), result.order());
    }

    public void testFromProtoWithMultipleOrders() {
        SortOrderSingleMap countOrder = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        SortOrderSingleMap keyOrder = SortOrderSingleMap.newBuilder()
            .setField("_key").setSortOrder(SortOrder.SORT_ORDER_ASC)
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .addOrder(countOrder)
            .addOrder(keyOrder)
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertNotNull("Order should not be null", result.order());
        // Compound order: [_count desc, _key asc]
        BucketOrder expected = BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true));
        assertEquals("Order should be compound", expected, result.order());
    }

    public void testFromProtoWithNoOrder() {
        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test", proto);

        assertNotNull("Result should not be null", result);
        assertNotNull("Order should have default value", result.order());
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
        TermsAggregation proto = TermsAggregation.newBuilder().setField("category").setValueType(org.opensearch.protobufs.ValueType.VALUE_TYPE_STRING).build();

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
        FieldValue missingValue = FieldValue.newBuilder().setString("N/A").build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .setSize(50)
            .setShardSize(200)
            .setMinDocCount(10)
            .setShardMinDocCount(5)
            .setShowTermDocCountError(true)
            .setCollectMode(TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_DEPTH_FIRST)
            // Note: order field not yet supported in proto
            .setMissing(missingValue)
            .setValueType(org.opensearch.protobufs.ValueType.VALUE_TYPE_STRING)
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

    public void testFromProtoWithNestedSubAggregations() {
        // Create a terms aggregation with nested sub-aggregations (min and max)
        AggregationContainer minPriceAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        AggregationContainer maxQuantityAgg = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("quantity").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .setSize(10)
            .putAggregations("min_price", minPriceAgg)
            .putAggregations("max_quantity", maxQuantityAgg)
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("categories", proto);

        // Verify the terms aggregation
        assertNotNull(result);
        assertEquals("categories", result.getName());
        assertEquals("category", result.field());
        assertEquals(10, result.size());

        // Verify sub-aggregations
        Collection<AggregationBuilder> subAggs = result.getSubAggregations();
        assertNotNull(subAggs);
        assertEquals(2, subAggs.size());

        // Verify sub-aggregation names
        boolean hasMinPrice = false;
        boolean hasMaxQuantity = false;
        for (AggregationBuilder subAgg : subAggs) {
            if ("min_price".equals(subAgg.getName())) {
                hasMinPrice = true;
            } else if ("max_quantity".equals(subAgg.getName())) {
                hasMaxQuantity = true;
            }
        }
        assertTrue("Expected min_price sub-aggregation", hasMinPrice);
        assertTrue("Expected max_quantity sub-aggregation", hasMaxQuantity);
    }

    public void testFromProtoWithNestedSubAggregationsUsingAggsShorthand() {
        // Test using 'aggs' field (shorthand) instead of 'aggregations'
        AggregationContainer minPriceAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("min_price", minPriceAgg)  // Using 'aggregations'
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("categories", proto);

        // Verify sub-aggregation
        Collection<AggregationBuilder> subAggs = result.getSubAggregations();
        assertNotNull(subAggs);
        assertEquals(1, subAggs.size());

        AggregationBuilder subAgg = subAggs.iterator().next();
        assertEquals("min_price", subAgg.getName());
    }

    @Ignore("Test obsolete: proto schema changed to only have aggregations field")
    public void testFromProtoThrowsWhenBothAggregationsAndAggsArePopulated() {
        // Create sub-aggregations for both fields
        AggregationContainer minAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();
        AggregationContainer maxAgg = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();

        // Populate BOTH 'aggregations' and 'aggs' fields (invalid in REST API)
        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("min_price", minAgg)  // Using 'aggregations'
            .putAggregations("max_price", maxAgg)          // AND 'aggs' - should be rejected
            .build();

        // Should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermsAggregationProtoUtils.fromProto("categories", proto)
        );

        assertTrue(
            "Exception message should mention both fields",
            exception.getMessage().contains("aggregations") && exception.getMessage().contains("aggs")
        );
    }

    @Ignore("Test obsolete: proto schema changed to only have aggregations field")
    public void testFromProtoThrowsWhenBothAggregationsAndAggsHaveOverlappingNames() {
        // Test with the SAME name in both fields but different aggregation types
        // This would cause a duplicate name error downstream if we didn't validate upfront
        AggregationContainer minAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();
        AggregationContainer maxAgg = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("price_stat", minAgg)  // Same name 'price_stat'
            .putAggregations("price_stat", maxAgg)          // Same name 'price_stat' - conflict!
            .build();

        // Should throw IllegalArgumentException due to both fields being populated
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermsAggregationProtoUtils.fromProto("categories", proto)
        );

        assertTrue(
            "Exception message should indicate both fields cannot be used together",
            exception.getMessage().contains("Cannot specify both")
        );
    }

    public void testFromProtoAllowsEmptyAggregationsAndAggsFields() {
        // Both fields can be empty (no sub-aggregations) - this should succeed
        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .setSize(10)
            // Both aggregations and aggs maps are empty (default)
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("categories", proto);

        assertNotNull(result);
        assertEquals("categories", result.getName());
        assertEquals("category", result.field());

        // Verify no sub-aggregations
        Collection<AggregationBuilder> subAggs = result.getSubAggregations();
        assertNotNull(subAggs);
        assertEquals(0, subAggs.size());
    }

    public void testFromProtoOnlyAggregationsFieldPopulated() {
        // Only 'aggregations' field populated - should work
        AggregationContainer minAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("min_price", minAgg)
            // 'aggs' field not populated
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("categories", proto);

        assertNotNull(result);
        Collection<AggregationBuilder> subAggs = result.getSubAggregations();
        assertEquals(1, subAggs.size());
        assertEquals("min_price", subAggs.iterator().next().getName());
    }

    public void testFromProtoOnlyAggsFieldPopulated() {
        // Only 'aggs' field populated - should work
        AggregationContainer maxAgg = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("max_price", maxAgg)
            // 'aggregations' field not populated
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("categories", proto);

        assertNotNull(result);
        Collection<AggregationBuilder> subAggs = result.getSubAggregations();
        assertEquals(1, subAggs.size());
        assertEquals("max_price", subAggs.iterator().next().getName());
    }

    @Ignore("Test obsolete: proto schema changed to only have aggregations field")
    public void testFromProtoErrorMessageIsInformative() {
        // Verify the error message provides clear guidance
        AggregationContainer minAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();
        AggregationContainer maxAgg = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("quantity").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("min_price", minAgg)
            .putAggregations("max_quantity", maxAgg)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermsAggregationProtoUtils.fromProto("categories", proto)
        );

        String message = exception.getMessage();

        // Verify error message contains key information
        assertTrue("Message should mention 'aggregations'", message.contains("aggregations"));
        assertTrue("Message should mention 'aggs'", message.contains("aggs"));
        assertTrue("Message should indicate mutual exclusivity",
            message.contains("Cannot specify both") || message.contains("only one"));
    }

    public void testFromProtoMultipleSubAggregationsInAggregationsField() {
        // Test that multiple sub-aggregations work in the 'aggregations' field
        AggregationContainer minAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();
        AggregationContainer maxAgg = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();
        AggregationContainer avgAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("quantity").build())  // Using min as a stand-in for avg
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("min_price", minAgg)
            .putAggregations("max_price", maxAgg)
            .putAggregations("avg_quantity", avgAgg)
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("categories", proto);

        Collection<AggregationBuilder> subAggs = result.getSubAggregations();
        assertEquals(3, subAggs.size());

        // Verify all three sub-aggregations are present
        boolean hasMin = false, hasMax = false, hasAvg = false;
        for (AggregationBuilder subAgg : subAggs) {
            if ("min_price".equals(subAgg.getName())) hasMin = true;
            if ("max_price".equals(subAgg.getName())) hasMax = true;
            if ("avg_quantity".equals(subAgg.getName())) hasAvg = true;
        }
        assertTrue("Should have min_price", hasMin);
        assertTrue("Should have max_price", hasMax);
        assertTrue("Should have avg_quantity", hasAvg);
    }

    public void testFromProtoMultipleSubAggregationsInAggsField() {
        // Test that multiple sub-aggregations work in the 'aggs' field
        AggregationContainer minAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();
        AggregationContainer maxAgg = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("min_price", minAgg)
            .putAggregations("max_price", maxAgg)
            .build();

        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("categories", proto);

        Collection<AggregationBuilder> subAggs = result.getSubAggregations();
        assertEquals(2, subAggs.size());

        // Verify both sub-aggregations are present
        boolean hasMin = false, hasMax = false;
        for (AggregationBuilder subAgg : subAggs) {
            if ("min_price".equals(subAgg.getName())) hasMin = true;
            if ("max_price".equals(subAgg.getName())) hasMax = true;
        }
        assertTrue("Should have min_price", hasMin);
        assertTrue("Should have max_price", hasMax);
    }
}
