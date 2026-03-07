/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.UnmappedTermsAggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

/**
 * Utility class for converting UnmappedTerms aggregation results to UnmappedTermsAggregate protocol buffer format.
 *
 * <p>This utility mirrors {@link UnmappedTerms#doXContentBody} but produces Protocol Buffer messages
 * instead of XContent (JSON/YAML). UnmappedTerms is returned when a terms aggregation is executed on an
 * unmapped field, and contains no buckets.
 *
 * @see UnmappedTerms
 * @see UnmappedTermsAggregate
 */
public class UnmappedTermsProtoUtils {

    private UnmappedTermsProtoUtils() {
        // Utility class
    }

    /**
     * Converts an UnmappedTerms aggregation result to an UnmappedTermsAggregate protobuf message.
     *
     * <p>Mirrors {@link UnmappedTerms#doXContentBody}. Unlike other terms aggregations,
     * unmapped terms always have empty buckets and zero error counts.
     *
     * @param terms The UnmappedTerms aggregation result from OpenSearch
     * @return An UnmappedTermsAggregate protobuf message
     */
    public static UnmappedTermsAggregate toProto(UnmappedTerms terms) {
        UnmappedTermsAggregate.Builder builder = UnmappedTermsAggregate.newBuilder();

        // Set aggregate-level error fields (always 0 for unmapped terms)
        builder.setDocCountErrorUpperBound(terms.getDocCountError());  // Always 0
        builder.setSumOtherDocCount(terms.getSumOfOtherDocCounts());   // Always 0

        // Buckets are empty for unmapped terms - no need to add any

        // Set metadata if present
        AggregateProtoUtils.setMetadataIfPresent(terms.getMetadata(), builder::setMeta);

        return builder.build();
    }
}
