/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.DoubleTermsProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.LongTermsProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.StringTermsProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.UnmappedTermsProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.UnsignedLongTermsProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MaxAggregateProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MinAggregateProtoUtils;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Utility class for converting OpenSearch InternalAggregation objects to Protocol Buffer Aggregate messages.
 *
 * <p>This class serves as a central dispatcher that routes different aggregation types to their specific
 * converters, and provides common helper methods for aggregation serialization that are shared across
 * all aggregation types.
 *
 * @see InternalAggregation
 * @see org.opensearch.search.aggregations.Aggregations
 */
public class AggregateProtoUtils {

    private AggregateProtoUtils() {
        // Utility class - no instances
    }

    /**
     * Converts an InternalAggregation to its Protocol Buffer Aggregate representation.
     *
     * <p>This method acts as a central dispatcher that routes different aggregation types
     * to their specific converter utilities using instanceof checks.
     *
     * @param aggregation The OpenSearch internal aggregation (must not be null)
     * @return The corresponding Protocol Buffer Aggregate message
     * @throws IllegalArgumentException if aggregation is null or type is not supported
     * @throws IOException if an error occurs during protobuf conversion
     */
    public static Aggregate toProto(InternalAggregation aggregation) throws IOException {
        if (aggregation == null) {
            throw new IllegalArgumentException("InternalAggregation must not be null");
        }

        Aggregate.Builder aggregateBuilder = Aggregate.newBuilder();

        // Dispatch based on runtime type
        if (aggregation instanceof StringTerms) {
            aggregateBuilder.setSterms(StringTermsProtoUtils.toProto((StringTerms) aggregation));
        } else if (aggregation instanceof LongTerms) {
            aggregateBuilder.setLterms(LongTermsProtoUtils.toProto((LongTerms) aggregation));
        } else if (aggregation instanceof UnsignedLongTerms) {
            aggregateBuilder.setUlterms(UnsignedLongTermsProtoUtils.toProto((UnsignedLongTerms) aggregation));
        } else if (aggregation instanceof DoubleTerms) {
            aggregateBuilder.setDterms(DoubleTermsProtoUtils.toProto((DoubleTerms) aggregation));
        } else if (aggregation instanceof InternalMin) {
            aggregateBuilder.setMin(MinAggregateProtoUtils.toProto((InternalMin) aggregation));
        } else if (aggregation instanceof InternalMax) {
            aggregateBuilder.setMax(MaxAggregateProtoUtils.toProto((InternalMax) aggregation));
        } else if (aggregation instanceof UnmappedTerms) {
            aggregateBuilder.setUmterms(UnmappedTermsProtoUtils.toProto((UnmappedTerms) aggregation));
        } else {
            // Future aggregation types will be added here
            throw new IllegalArgumentException("Unsupported aggregation type: " + aggregation.getClass().getName());
        }

        return aggregateBuilder.build();
    }

    /**
     * Sets the aggregation metadata if present.
     *
     * <p>Mirrors {@link InternalAggregation#toXContent} which serializes metadata when present.
     * This is a common helper method used by all aggregation types since all aggregations
     * inherit from {@link InternalAggregation} and can optionally have metadata.
     *
     * <p>Metadata is only included in the protobuf message when non-null and non-empty.
     *
     * @param metadata The metadata map from InternalAggregation.getMetadata()
     * @param setter Consumer that sets the ObjectMap in the protobuf builder (e.g., builder::setMeta)
     */
    public static void setMetadataIfPresent(
        java.util.Map<String, Object> metadata,
        Consumer<ObjectMap> setter
    ) {
        if (metadata != null && !metadata.isEmpty()) {
            ObjectMap.Value metaValue = ObjectMapProtoUtils.toProto(metadata);
            if (metaValue.hasObjectMap()) {
                setter.accept(metaValue.getObjectMap());
            }
        }
    }

    /**
     * Converts sub-aggregations to protobuf format without outer wrapper.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.Aggregations#toXContentInternal} which iterates
     * through aggregations and serializes each one. This is called by all bucket aggregations when serializing
     * sub-aggregations, as seen in {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms.Bucket#toXContent}.
     *
     * <p>Iterates through sub-aggregations and converts each to protobuf format, then adds to parent bucket's
     * aggregate map. No-op if aggregations is null or empty.
     *
     * @param aggregations The InternalAggregations from a bucket (can be null or empty)
     * @param adder BiConsumer that adds each converted aggregate to the parent bucket builder.
     *              First parameter is aggregation name, second is the converted Aggregate protobuf.
     * @throws IOException if an error occurs during aggregation conversion
     */
    public static void toProtoInternal(
        InternalAggregations aggregations,
        BiConsumerWithException<String, Aggregate> adder
    ) throws IOException {
        if (aggregations != null && !aggregations.asList().isEmpty()) {
            for (org.opensearch.search.aggregations.Aggregation agg : aggregations.asList()) {
                Aggregate protoAgg = AggregateProtoUtils.toProto((InternalAggregation) agg);
                adder.accept(agg.getName(), protoAgg);
            }
        }
    }

    /**
     * Functional interface for consumers that can throw IOException.
     *
     * <p>Used by {@link #toProtoInternal} to add converted aggregates to protobuf bucket builders.
     *
     * @param <T> First argument type (aggregation name)
     * @param <U> Second argument type (Aggregate protobuf)
     */
    @FunctionalInterface
    public interface BiConsumerWithException<T, U> {
        /**
         * Performs this operation on the given arguments.
         *
         * @param t First input argument (aggregation name)
         * @param u Second input argument (Aggregate protobuf)
         * @throws IOException if an I/O error occurs
         */
        void accept(T t, U u) throws IOException;
    }
}
