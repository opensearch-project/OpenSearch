/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;

import java.io.IOException;

/**
 * Utility class for converting OpenSearch InternalAggregation objects to Protocol Buffer Aggregate messages.
 * This class serves as a central dispatcher that routes different aggregation types to their specific converters.
 *
 * <p>Similar to the request-side AggregationContainerProtoUtils, but for response conversion.
 * Uses instanceof checks to dispatch to type-specific converters since we're working with
 * OpenSearch's internal aggregation class hierarchy rather than protobuf enums.
 *
 * <p>Note: Nested aggregations within buckets are not currently supported due to limitations in the
 * protobuf schema. Bucket messages (StringTermsBucket, LongTermsBucket, DoubleTermsBucket) do not
 * contain an aggregations field in the current schema version.
 */
public class AggregateProtoUtils {

    private AggregateProtoUtils() {
        // Utility class - no instances
    }

    /**
     * Converts an InternalAggregation to its Protocol Buffer Aggregate representation.
     * Uses instanceof checks to dispatch to type-specific converters.
     *
     * @param aggregation The OpenSearch internal aggregation
     * @return The corresponding Protocol Buffer Aggregate
     * @throws IllegalArgumentException if the aggregation type is not supported or aggregation is null
     * @throws IOException if an error occurs during protobuf conversion
     */
    public static Aggregate toProto(InternalAggregation aggregation) throws IOException {
        if (aggregation == null) {
            throw new IllegalArgumentException("InternalAggregation must not be null");
        }

        Aggregate.Builder aggregateBuilder = Aggregate.newBuilder();

        // Dispatch based on runtime type
        if (aggregation instanceof StringTerms) {
            aggregateBuilder.setSterms(TermsAggregateProtoUtils.stringTermsToProto((StringTerms) aggregation));
        } else if (aggregation instanceof LongTerms) {
            aggregateBuilder.setLterms(TermsAggregateProtoUtils.longTermsToProto((LongTerms) aggregation));
        } else if (aggregation instanceof DoubleTerms) {
            aggregateBuilder.setDterms(TermsAggregateProtoUtils.doubleTermsToProto((DoubleTerms) aggregation));
        } else {
            // Future aggregation types will be added here
            throw new IllegalArgumentException("Unsupported aggregation type: " + aggregation.getClass().getName());
        }

        return aggregateBuilder.build();
    }
}
