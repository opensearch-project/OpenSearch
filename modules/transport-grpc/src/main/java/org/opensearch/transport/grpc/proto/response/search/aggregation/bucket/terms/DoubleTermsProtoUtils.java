/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.DoubleTermsAggregate;
import org.opensearch.protobufs.DoubleTermsBucket;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting DoubleTerms aggregation results to DoubleTermsAggregate protocol buffer format.
 *
 * <p>This utility mirrors the REST API serialization logic from
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody} and
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}
 * but produces Protocol Buffer messages instead of XContent (JSON/YAML).
 *
 * @see DoubleTerms
 * @see DoubleTermsAggregate
 */
public class DoubleTermsProtoUtils {

    private DoubleTermsProtoUtils() {
        // Utility class
    }

    /**
     * Converts a DoubleTerms aggregation result to a DoubleTermsAggregate protobuf message.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody}
     * which delegates to {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}.
     *
     * @param terms The DoubleTerms aggregation result from OpenSearch
     * @return A DoubleTermsAggregate protobuf message
     * @throws IOException if an error occurs during conversion
     */
    public static DoubleTermsAggregate toProto(DoubleTerms terms) throws IOException {
        DoubleTermsAggregate.Builder builder = DoubleTermsAggregate.newBuilder();

        doProtoCommon(builder, terms);
        AggregateProtoUtils.setMetadataIfPresent(terms.getMetadata(), builder::setMeta);

        return builder.build();
    }

    /**
     * Sets common terms-level fields in the protobuf builder.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}
     * which sets doc_count_error_upper_bound, sum_other_doc_count, and iterates buckets.
     *
     * @param builder The DoubleTermsAggregate builder to populate
     * @param terms The DoubleTerms aggregation result
     * @throws IOException if an error occurs during bucket conversion
     */
    private static void doProtoCommon(DoubleTermsAggregate.Builder builder, DoubleTerms terms) throws IOException {
        InternalTermsProtoUtils.setDocCountErrorUpperBound(
            terms.getDocCountError(),
            builder::setDocCountErrorUpperBound
        );

        InternalTermsProtoUtils.setSumOtherDocCount(
            terms.getSumOfOtherDocCounts(),
            builder::setSumOtherDocCount
        );

        for (InternalTerms.Bucket<?> bucket : terms.getBuckets()) {
            builder.addBuckets(toProtoBucket(bucket));
        }
    }

    /**
     * Converts a single DoubleTerms bucket to protobuf format.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms.Bucket#toXContent}.
     *
     * @param bucket The bucket to convert
     * @return A DoubleTermsBucket protobuf message
     * @throws IOException if an error occurs during bucket conversion
     */
    private static DoubleTermsBucket toProtoBucket(InternalTerms.Bucket<?> bucket) throws IOException {
        DoubleTermsBucket.Builder bucketBuilder = DoubleTermsBucket.newBuilder();

        keyToProto(bucketBuilder, bucket);
        InternalTermsProtoUtils.setDocCount(bucket.getDocCount(), bucketBuilder::setDocCount);
        InternalTermsProtoUtils.setDocCountErrorUpperBoundIfApplicable(bucket, bucketBuilder::setDocCountErrorUpperBound);

        AggregateProtoUtils.toProtoInternal(
            (InternalAggregations) bucket.getAggregations(),
            bucketBuilder::putAggregate
        );

        return bucketBuilder.build();
    }

    /**
     * Sets the key and key_as_string fields for a DoubleTerms bucket.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.DoubleTerms.Bucket#keyToXContent}
     * which sets key and conditionally sets key_as_string when format is not RAW.
     *
     * @param bucketBuilder The DoubleTermsBucket builder to populate
     * @param bucket The bucket to convert
     */
    private static void keyToProto(DoubleTermsBucket.Builder bucketBuilder, InternalTerms.Bucket<?> bucket) {
        double keyValue = bucket.getKeyAsNumber().doubleValue();
        bucketBuilder.setKey(keyValue);

        DocValueFormat format = bucket.getFormat();
        if (format != DocValueFormat.RAW) {
            String keyAsString = bucket.getKeyAsString();
            if (keyAsString != null) {
                bucketBuilder.setKeyAsString(keyAsString);
            }
        }
    }
}
