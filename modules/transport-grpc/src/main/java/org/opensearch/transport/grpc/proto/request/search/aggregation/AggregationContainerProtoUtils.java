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
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms.TermsAggregationProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.metrics.MaxAggregationProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.metrics.MinAggregationProtoUtils;

/**
 * Utility class for converting AggregationContainer protocol buffers to OpenSearch AggregationBuilder objects.
 *
 * <p>This class serves as a central dispatcher that routes different aggregation types to their specific converters,
 * similar to how {@link AggregatorFactories#parseAggregators} uses registered parsers with XContentParser.
 *
 * @see AggregatorFactories#parseAggregators
 */
public class AggregationContainerProtoUtils {

    private AggregationContainerProtoUtils() {
        // Utility class - no instances
    }

    /**
     * Converts an AggregationContainer protobuf to an {@link AggregationBuilder}.
     *
     * <p>Mirrors {@link AggregatorFactories#parseAggregators}, serving as the central dispatcher
     * for all aggregation types. Validates the aggregation name and delegates to type-specific converters.
     *
     * @param name The name of the aggregation
     * @param aggContainer The protobuf aggregation container
     * @return The corresponding {@link AggregationBuilder}
     * @throws IllegalArgumentException if the aggregation type is not supported, container is null,
     *         or aggregation name is invalid
     */
    public static AggregationBuilder fromProto(String name, AggregationContainer aggContainer) {
        if (aggContainer == null) {
            throw new IllegalArgumentException("AggregationContainer must not be null");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Aggregation name must not be null or empty");
        }

        // Validate aggregation name format (mirrors AggregatorFactories.parseAggregators)
        if (!AggregatorFactories.VALID_AGG_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                "Invalid aggregation name ["
                    + name
                    + "]. Aggregation names can contain any character except '[', ']', and '>'"
            );
        }

        AggregationBuilder builder;

        // Dispatch to type-specific converter based on aggregation type
        // This mirrors the REST-side pattern where each aggregation has a registered parser
        switch (aggContainer.getAggregationContainerCase()) {
            case TERMS:
                builder = TermsAggregationProtoUtils.fromProto(name, aggContainer.getTerms());
                break;

            case MIN:
                builder = MinAggregationProtoUtils.fromProto(name, aggContainer.getMin());
                break;

            case MAX:
                builder = MaxAggregationProtoUtils.fromProto(name, aggContainer.getMax());
                break;

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
