/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;

/**
 * Utility class for converting Filter Aggregation Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of filter aggregations
 * into their corresponding OpenSearch FilterAggregationBuilder implementations.
 */
public class FilterAggregationProtoUtils {

    private FilterAggregationProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer QueryContainer (used as filter) to an OpenSearch FilterAggregationBuilder.
     * Filter aggregations limit the documents that are aggregated by applying a query filter.
     *
     * @param name The name of the aggregation
     * @param filterQueryProto The Protocol Buffer QueryContainer representing the filter query
     * @param queryUtils The query utils instance for parsing the filter query
     * @return A configured FilterAggregationBuilder instance
     * @throws IllegalArgumentException if there's an error parsing the filter query
     */
    public static FilterAggregationBuilder fromProto(
        String name,
        QueryContainer filterQueryProto,
        AbstractQueryBuilderProtoUtils queryUtils
    ) {
        // Parse the filter query
        QueryBuilder filterQuery = queryUtils.parseInnerQueryBuilderProto(filterQueryProto);

        // Create the filter aggregation builder
        return new FilterAggregationBuilder(name, filterQuery);
    }
}
