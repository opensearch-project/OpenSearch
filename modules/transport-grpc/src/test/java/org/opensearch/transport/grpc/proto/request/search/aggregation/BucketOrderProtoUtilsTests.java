/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.SortOrderMap;
import org.opensearch.protobufs.SortOrderSingleMap;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class BucketOrderProtoUtilsTests extends OpenSearchTestCase {

    // ========================================
    // Single Order Tests - _count
    // ========================================

    public void testParseOrderParamWithSingleCountDescOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals("Order should be COUNT_DESC with key tiebreaker",
            BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true)), builder.order());
    }

    public void testParseOrderParamWithSingleCountAscOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_ASC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals("Order should be COUNT_ASC with key tiebreaker",
            BucketOrder.compound(BucketOrder.count(true), BucketOrder.key(true)), builder.order());
    }

    // ========================================
    // Single Order Tests - _key
    // ========================================

    public void testParseOrderParamWithSingleKeyDescOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_key").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        assertEquals("Order should be KEY_DESC", BucketOrder.key(false), builder.order());
    }

    public void testParseOrderParamWithSingleKeyAscOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_key").setSortOrder(SortOrder.SORT_ORDER_ASC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        assertEquals("Order should be KEY_ASC", BucketOrder.key(true), builder.order());
    }

    // ========================================
    // Single Order Tests - Deprecated _term and _time
    // ========================================

    public void testParseOrderParamWithDeprecatedTermAscOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_term").setSortOrder(SortOrder.SORT_ORDER_ASC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        assertEquals("Order should be KEY_ASC (deprecated _term)", BucketOrder.key(true), builder.order());
    }

    public void testParseOrderParamWithDeprecatedTermDescOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_term").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        assertEquals("Order should be KEY_DESC (deprecated _term)", BucketOrder.key(false), builder.order());
    }

    public void testParseOrderParamWithDeprecatedTimeAscOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_time").setSortOrder(SortOrder.SORT_ORDER_ASC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        assertEquals("Order should be KEY_ASC (deprecated _time)", BucketOrder.key(true), builder.order());
    }

    public void testParseOrderParamWithDeprecatedTimeDescOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_time").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        assertEquals("Order should be KEY_DESC (deprecated _time)", BucketOrder.key(false), builder.order());
    }

    // ========================================
    // Single Order Tests - Sub-aggregation
    // ========================================

    public void testParseOrderParamWithSubAggregationAscOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("my_agg").setSortOrder(SortOrder.SORT_ORDER_ASC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals("Order should be aggregation order ASC with key tiebreaker",
            BucketOrder.compound(BucketOrder.aggregation("my_agg", true), BucketOrder.key(true)), builder.order());
    }

    public void testParseOrderParamWithSubAggregationDescOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("my_agg").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals("Order should be aggregation order DESC with key tiebreaker",
            BucketOrder.compound(BucketOrder.aggregation("my_agg", false), BucketOrder.key(true)), builder.order());
    }

    public void testParseOrderParamWithSubAggregationPathOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("my_agg>nested_metric").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals(
            "Order should be aggregation path order DESC with key tiebreaker",
            BucketOrder.compound(BucketOrder.aggregation("my_agg>nested_metric", false), BucketOrder.key(true)),
            builder.order()
        );
    }

    // ========================================
    // Multiple Orders (Compound) Tests
    // ========================================

    public void testParseOrderParamWithMultipleOrders() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        List<SortOrderSingleMap> orderList = new ArrayList<>();
        orderList.add(SortOrderSingleMap.newBuilder().setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC).build());
        orderList.add(SortOrderSingleMap.newBuilder().setField("_key").setSortOrder(SortOrder.SORT_ORDER_ASC).build());

        BucketOrderProtoUtils.toProto(builder, orderList);

        // Compound order should be created
        BucketOrder expectedCompound = BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true));
        assertEquals("Order should be compound order", expectedCompound, builder.order());
    }

    public void testParseOrderParamWithThreeOrders() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        List<SortOrderSingleMap> orderList = new ArrayList<>();
        orderList.add(SortOrderSingleMap.newBuilder().setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC).build());
        orderList.add(SortOrderSingleMap.newBuilder().setField("my_agg").setSortOrder(SortOrder.SORT_ORDER_DESC).build());
        orderList.add(SortOrderSingleMap.newBuilder().setField("_key").setSortOrder(SortOrder.SORT_ORDER_ASC).build());

        BucketOrderProtoUtils.toProto(builder, orderList);

        // Compound order should be created with all three
        BucketOrder expectedCompound = BucketOrder.compound(
            BucketOrder.count(false),
            BucketOrder.aggregation("my_agg", false),
            BucketOrder.key(true)
        );
        assertEquals("Order should be compound order with three elements", expectedCompound, builder.order());
    }

    // ========================================
    // Empty/Null Tests
    // ========================================

    public void testParseOrderParamWithNullOrderList() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        BucketOrder defaultOrder = builder.order();

        BucketOrderProtoUtils.toProto(builder, null);

        assertEquals("Order should remain default when orderList is null", defaultOrder, builder.order());
    }

    public void testParseOrderParamWithEmptyOrderList() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        BucketOrder defaultOrder = builder.order();

        BucketOrderProtoUtils.toProto(builder, new ArrayList<>());

        assertEquals("Order should remain default when orderList is empty", defaultOrder, builder.order());
    }

    // ========================================
    // Error Cases Tests
    // ========================================

    public void testParseOrderParamWithNullSortOrderMapInList() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        List<SortOrderSingleMap> orderList = new ArrayList<>();
        orderList.add(null);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BucketOrderProtoUtils.toProto(builder, orderList)
        );

        assertTrue(
            "Exception message should mention 'SortOrderSingleMap must not be null'",
            exception.getMessage().contains("SortOrderSingleMap must not be null")
        );
    }

    public void testParseOrderParamWithSortOrderMapWithoutMap() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder().build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BucketOrderProtoUtils.toProto(builder, orderList)
        );

        assertTrue(
            "Exception message should mention 'at least one field'",
            exception.getMessage().contains("at least one field")
        );
    }

    public void testParseOrderParamWithEmptyMapInSortOrderMap() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder().build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BucketOrderProtoUtils.toProto(builder, orderList)
        );

        assertTrue(
            "Exception message should mention 'at least one field'",
            exception.getMessage().contains("at least one field")
        );
    }

    // Note: This test is commented out because protobuf builders don't allow setting multiple values
    // for the same field - each setField() call overwrites the previous value.
    // The actual behavior is that only the last setField() value is retained.
    /*
    public void testParseOrderParamWithMultipleEntriesInSingleSortOrderMap() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .setField("_key").setSortOrder(SortOrder.SORT_ORDER_ASC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BucketOrderProtoUtils.toProto(builder, orderList)
        );

        assertTrue(
            "Exception message should mention 'exactly one entry'",
            exception.getMessage().contains("exactly one entry")
        );
    }
    */

    public void testParseOrderParamWithUnspecifiedDirection() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_UNSPECIFIED)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BucketOrderProtoUtils.toProto(builder, orderList)
        );

        assertTrue(
            "Exception message should mention 'Unknown order direction'",
            exception.getMessage().contains("Unknown order direction")
        );
    }

    // Note: This test is commented out because protobuf builders throw an exception when trying
    // to set UNRECOGNIZED enum value. UNRECOGNIZED is only used when deserializing unknown values,
    // not for explicit setting in builders.
    /*
    public void testParseOrderParamWithUnrecognizedDirection() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.UNRECOGNIZED)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BucketOrderProtoUtils.toProto(builder, orderList)
        );

        assertTrue(
            "Exception message should mention 'Unknown order direction'",
            exception.getMessage().contains("Unknown order direction")
        );
    }
    */

    // ========================================
    // Edge Cases Tests
    // ========================================

    public void testParseOrderParamPreservesBuilderState() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        builder.size(10);
        builder.shardSize(20);

        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        assertEquals("Size should be preserved", 10, builder.size());
        assertEquals("Shard size should be preserved", 20, builder.shardSize());
        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals("Order should be set with key tiebreaker",
            BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true)), builder.order());
    }

    public void testParseOrderParamOverridesPreviousOrder() {
        TermsAggregationBuilder builder = new TermsAggregationBuilder("test");
        builder.order(BucketOrder.key(true));

        SortOrderSingleMap orderMap = SortOrderSingleMap.newBuilder()
            .setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();
        List<SortOrderSingleMap> orderList = List.of(orderMap);

        BucketOrderProtoUtils.toProto(builder, orderList);

        // OpenSearch adds a tiebreaker sort by key for stable ordering
        assertEquals("Order should be overridden to COUNT_DESC with key tiebreaker",
            BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true)), builder.order());
    }
}
