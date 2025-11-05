/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;

import java.util.Map;

/**
 * Utility class for converting AggregationContainer Protocol Buffers to OpenSearch AggregationBuilder objects.
 * This class handles the transformation of Protocol Buffer aggregation containers into their corresponding
 * OpenSearch AggregationBuilder implementations. It serves as the main entry point for converting all
 * aggregation types from protobuf format to OpenSearch format.
 *
 * <p>This class is analogous to the REST-side aggregation parsing framework where {@link XContentParser}
 * is used to parse aggregations from JSON/REST requests. Each aggregation type's {@code PARSER} field
 * (which uses {@code fromXContent}) has a corresponding protobuf converter method that performs the same
 * logical transformation but from Protocol Buffer representation.
 *
 * <p>The dispatch logic mirrors how REST aggregations are parsed: by examining the aggregation type field
 * and delegating to the appropriate type-specific parser.
 */
public class AggregationContainerProtoUtils {

    private AggregationContainerProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer AggregationContainer to an OpenSearch AggregationBuilder.
     * This method dispatches to the appropriate aggregation-specific converter based on the
     * aggregation type specified in the container.
     *
     * <p>This is the gRPC equivalent of the REST-side aggregation parsing in
     * {@code AggregationBuilder.parseAggregators}, where each aggregation type has a registered
     * parser that reads from {@link XContentParser} via {@code fromXContent}. This method performs
     * the same role but reads from Protocol Buffer structures instead of JSON.
     *
     * @param name The name of the aggregation
     * @param aggContainer The Protocol Buffer AggregationContainer object
     * @return A configured AggregationBuilder instance
     * @throws IllegalArgumentException if the aggregation type is not supported or unrecognized
     * @see org.opensearch.search.aggregations.AggregationBuilder
     */
    public static AggregationBuilder fromProto(String name, AggregationContainer aggContainer) {
        return fromProto(name, aggContainer, null);
    }

    /**
     * Converts a Protocol Buffer AggregationContainer to an OpenSearch AggregationBuilder.
     * This method dispatches to the appropriate aggregation-specific converter based on the
     * aggregation type specified in the container. It also supports nested queries in aggregations
     * (like filter aggregations) by accepting a queryUtils parameter.
     *
     * <p>This is the gRPC equivalent of the REST-side aggregation parsing in
     * {@code AggregationBuilder.parseAggregators}, where each aggregation type has a registered
     * parser that reads from {@link XContentParser} via {@code fromXContent}. This method performs
     * the same role but reads from Protocol Buffer structures instead of JSON.
     *
     * @param name The name of the aggregation
     * @param aggContainer The Protocol Buffer AggregationContainer object
     * @param queryUtils The query utils instance for parsing nested queries (can be null if not needed)
     * @return A configured AggregationBuilder instance
     * @throws IllegalArgumentException if the aggregation type is not supported or unrecognized
     * @see org.opensearch.search.aggregations.AggregationBuilder
     */
    public static AggregationBuilder fromProto(String name, AggregationContainer aggContainer, AbstractQueryBuilderProtoUtils queryUtils) {
        AggregationBuilder builder;

        // Dispatch based on the aggregation type
        switch (aggContainer.getAggregationCase()) {
            case CARDINALITY:
                builder = CardinalityAggregationProtoUtils.fromProto(name, aggContainer.getCardinality());
                break;

            case MISSING:
                builder = MissingAggregationProtoUtils.fromProto(name, aggContainer.getMissing());
                break;

            case TERMS:
                builder = TermsAggregationProtoUtils.fromProto(name, aggContainer.getTerms());
                break;

            case FILTER:
                if (queryUtils == null) {
                    throw new IllegalArgumentException("Filter aggregation requires queryUtils to be provided for parsing nested queries");
                }
                builder = FilterAggregationProtoUtils.fromProto(name, aggContainer.getFilter(), queryUtils);
                break;

            case AGGREGATION_NOT_SET:
                throw new IllegalArgumentException("Aggregation type not set for aggregation: " + name);

            default:
                throw new IllegalArgumentException(
                    "Unsupported aggregation type: " + aggContainer.getAggregationCase() + " for aggregation: " + name
                );
        }

        // Handle metadata if present
        if (aggContainer.hasMeta()) {
            ObjectMap metaProto = aggContainer.getMeta();
            Map<String, Object> metadata = ObjectMapProtoUtils.fromProto(metaProto);
            builder.setMetadata(metadata);
        }

        return builder;
    }
}
