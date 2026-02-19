/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;

/**
 * Utility class for converting AggregationContainer protocol buffers to OpenSearch AggregationBuilder objects.
 * This class serves as a central dispatcher that routes different aggregation types to their specific converters.
 *
 * <p>Similar to how the REST API uses registered parsers with XContentParser, this dispatcher uses a switch-case
 * pattern to delegate proto conversion to type-specific utility classes.
 */
public class AggregationContainerProtoUtils {

    private AggregationContainerProtoUtils() {
        // Utility class - no instances
    }

    /**
     * Converts an AggregationContainer protobuf to an AggregationBuilder.
     *
     * @param name The name of the aggregation
     * @param aggContainer The protobuf aggregation container
     * @return The corresponding AggregationBuilder
     * @throws IllegalArgumentException if the aggregation type is not supported or container is null
     */
    public static AggregationBuilder fromProto(String name, AggregationContainer aggContainer) {
        return fromProto(name, aggContainer, null);
    }

    /**
     * Converts an AggregationContainer protobuf to an AggregationBuilder with query parsing support.
     * The queryUtils parameter enables nested query parsing for aggregations that contain queries
     * (e.g., filter aggregations).
     *
     * @param name The name of the aggregation
     * @param aggContainer The protobuf aggregation container
     * @param queryUtils Query converter for parsing nested queries (optional, required for filter aggregations)
     * @return The corresponding AggregationBuilder
     * @throws IllegalArgumentException if the aggregation type is not supported or container is null
     */
    public static AggregationBuilder fromProto(String name, AggregationContainer aggContainer, AbstractQueryBuilderProtoUtils queryUtils) {
        if (aggContainer == null) {
            throw new IllegalArgumentException("AggregationContainer must not be null");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Aggregation name must not be null or empty");
        }

        AggregationBuilder builder;

        // Dispatch to type-specific converter based on aggregation type
        // This mirrors the REST-side pattern where each aggregation has a registered parser
        switch (aggContainer.getAggregationContainerCase()) {
            case TERMS:
                builder = TermsAggregationProtoUtils.fromProto(name, aggContainer.getTerms());
                break;

            // TODO: Add more aggregation types as they are implemented
            // Example from PR #19894:
            // case CARDINALITY:
            // builder = CardinalityAggregationProtoUtils.fromProto(name, aggContainer.getCardinality());
            // break;
            // case MISSING:
            // builder = MissingAggregationProtoUtils.fromProto(name, aggContainer.getMissing());
            // break;
            // case FILTER:
            // builder = FilterAggregationProtoUtils.fromProto(name, aggContainer.getFilter(), queryUtils);
            // break;

            case AGGREGATIONCONTAINER_NOT_SET:
                throw new IllegalArgumentException("Aggregation type not set in container");

            default:
                throw new IllegalArgumentException("Unsupported aggregation type: " + aggContainer.getAggregationContainerCase());
        }

        // Apply metadata if present (common to all aggregations)
        if (aggContainer.hasMeta()) {
            builder.setMetadata(ObjectMapProtoUtils.fromProto(aggContainer.getMeta()));
        }

        return builder;
    }
}
