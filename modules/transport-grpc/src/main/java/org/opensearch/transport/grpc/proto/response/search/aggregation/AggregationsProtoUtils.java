/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;

/**
 * Converts Aggregations collection to protobuf map.
 */
public class AggregationsProtoUtils {

    private AggregationsProtoUtils() {
        // Utility class
    }

    /**
     * Converts Aggregations to protobuf map entries.
     *
     * <p>Mirrors {@link Aggregations#toXContentInternal} which iterates through
     * aggregations and serializes each one.
     *
     * @param aggregations The aggregations collection (can be null)
     * @param builder The SearchResponse builder to populate
     * @param registry The converter registry for dispatching to the appropriate converter
     * @throws IOException if an error occurs during conversion
     * @see Aggregations#toXContentInternal
     */
    public static void toProto(
        Aggregations aggregations,
        org.opensearch.protobufs.SearchResponse.Builder builder,
        AggregateProtoConverterRegistry registry
    ) throws IOException {
        if (aggregations == null) {
            return;
        }

        for (Aggregation agg : aggregations.asList()) {
            builder.putAggregations(agg.getName(), AggregateProtoUtils.toProto(agg, registry));
        }
    }
}
