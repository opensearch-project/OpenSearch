/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;

import java.util.Map;

/**
 * Utility class for converting MissingAggregation Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of missing aggregations
 * into their corresponding OpenSearch MissingAggregationBuilder implementations.
 */
public class MissingAggregationProtoUtils {

    private MissingAggregationProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MissingAggregation to an OpenSearch MissingAggregationBuilder.
     *
     * <p>This method is the gRPC equivalent of {@link MissingAggregationBuilder#PARSER}, which parses
     * missing aggregations from REST/JSON requests via {@code fromXContent}. Similar to how the parser
     * reads JSON fields, this method extracts values from the Protocol Buffer representation and creates
     * a properly configured MissingAggregationBuilder with nested sub-aggregations.
     *
     * <p>The REST-side serialization via {@link MissingAggregationBuilder#doXContentBody} produces
     * JSON that conceptually mirrors the protobuf structure used here.
     *
     * @param name The name of the aggregation
     * @param missingAggProto The Protocol Buffer MissingAggregation object
     * @return A configured MissingAggregationBuilder instance
     * @throws IllegalArgumentException if there's an error parsing the aggregation
     * @see MissingAggregationBuilder#PARSER
     * @see MissingAggregationBuilder#doXContentBody
     */
    public static MissingAggregationBuilder fromProto(String name, MissingAggregation missingAggProto) {
        MissingAggregationBuilder builder = new MissingAggregationBuilder(name);

        // Set field if present
        if (missingAggProto.hasField()) {
            builder.field(missingAggProto.getField());
        }

        // Handle sub-aggregations if present
        if (missingAggProto.getAggregationsCount() > 0) {
            Map<String, AggregationContainer> subAggs = missingAggProto.getAggregationsMap();
            for (Map.Entry<String, AggregationContainer> entry : subAggs.entrySet()) {
                AggregationBuilder subAggBuilder = AggregationContainerProtoUtils.fromProto(entry.getKey(), entry.getValue());
                builder.subAggregation(subAggBuilder);
            }
        }

        return builder;
    }
}
