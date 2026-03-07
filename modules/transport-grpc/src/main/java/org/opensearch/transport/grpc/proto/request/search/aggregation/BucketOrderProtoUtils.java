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
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting SortOrderMap Protocol Buffers to BucketOrder objects.
 *
 * <p>This utility mirrors {@link org.opensearch.search.aggregations.InternalOrder.Parser#parseOrderParam}
 * which parses order from XContent.
 *
 * @see org.opensearch.search.aggregations.InternalOrder.Parser#parseOrderParam
 * @see org.opensearch.search.aggregations.BucketOrder
 */
public class BucketOrderProtoUtils {

    private BucketOrderProtoUtils() {
        // Utility class
    }

    /**
     * Parse order from protobuf and apply to TermsAggregationBuilder.
     *
     * <p>Mirrors the REST parser pattern using {@link TermsAggregationBuilder#PARSER} declareObjectArray,
     * where order can be a single object or array. Calls
     * {@link org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder#order(BucketOrder)}
     * for single order or
     * {@link org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder#order(List)}
     * for multiple orders.
     *
     * @param builder The TermsAggregationBuilder to configure
     * @param orderList The list of SortOrderSingleMap from proto.getOrderList()
     * @see org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder#PARSER
     * @see org.opensearch.search.aggregations.InternalOrder.Parser#parseOrderParam
     */
    public static void toProto(TermsAggregationBuilder builder, List<org.opensearch.protobufs.SortOrderSingleMap> orderList) {
        if (orderList == null || orderList.isEmpty()) {
            // No order specified - use default (already set in TermsAggregationBuilder constructor)
            return;
        }

        if (orderList.size() == 1) {
            // Single order - pass directly to avoid unnecessary compound order
            builder.order(parseOrderParam(orderList.get(0)));
        } else {
            // Multiple orders - create compound order
            List<BucketOrder> orders = new ArrayList<>(orderList.size());
            for (org.opensearch.protobufs.SortOrderSingleMap proto : orderList) {
                orders.add(parseOrderParam(proto));
            }
            builder.order(orders);
        }
    }

    /**
     * Parse a {@link BucketOrder} from {@link org.opensearch.protobufs.SortOrderSingleMap}.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.InternalOrder.Parser#parseOrderParam}
     * which parses order from XContent.
     *
     * @param proto The SortOrderSingleMap protobuf message
     * @return A BucketOrder instance
     * @throws IllegalArgumentException if the proto is invalid
     */
    private static BucketOrder parseOrderParam(org.opensearch.protobufs.SortOrderSingleMap proto) {
        if (proto == null) {
            throw new IllegalArgumentException("SortOrderSingleMap must not be null");
        }

        String orderKey = proto.getField();
        org.opensearch.protobufs.SortOrder direction = proto.getSortOrder();

        if (orderKey == null || orderKey.isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one field for [order]");
        }

        boolean orderAsc;
        switch (direction) {
            case SORT_ORDER_ASC:
                orderAsc = true;
                break;
            case SORT_ORDER_DESC:
                orderAsc = false;
                break;
            default:
                throw new IllegalArgumentException("Unknown order direction [" + direction + "]");
        }

        switch (orderKey) {
            case "_term":
            case "_time":
            case "_key":
                return orderAsc ? BucketOrder.key(true) : BucketOrder.key(false);
            case "_count":
                return orderAsc ? BucketOrder.count(true) : BucketOrder.count(false);
            default:
                return BucketOrder.aggregation(orderKey, orderAsc);
        }
    }
}
