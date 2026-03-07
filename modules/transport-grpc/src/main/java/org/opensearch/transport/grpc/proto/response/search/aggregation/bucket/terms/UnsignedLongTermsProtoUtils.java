/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.UnsignedLongTermsAggregate;
import org.opensearch.protobufs.UnsignedLongTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Utility class for converting UnsignedLongTerms aggregation results to UnsignedLongTermsAggregate protocol buffer format.
 *
 * <p>This utility mirrors the REST API serialization logic from
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody} and
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}
 * but produces Protocol Buffer messages instead of XContent (JSON/YAML).
 *
 * @see UnsignedLongTerms
 * @see UnsignedLongTermsAggregate
 */
public class UnsignedLongTermsProtoUtils {

    private UnsignedLongTermsProtoUtils() {
        // Utility class
    }

    /**
     * Converts an UnsignedLongTerms aggregation result to an UnsignedLongTermsAggregate protobuf message.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody}
     * which delegates to {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}.
     *
     * @param terms The UnsignedLongTerms aggregation result from OpenSearch
     * @return An UnsignedLongTermsAggregate protobuf message
     * @throws IOException if an error occurs during conversion
     */
    public static UnsignedLongTermsAggregate toProto(UnsignedLongTerms terms) throws IOException {
        UnsignedLongTermsAggregate.Builder builder = UnsignedLongTermsAggregate.newBuilder();

        // Set common terms-level fields (mirrors InternalTerms.doXContentCommon lines 593-608)
        doProtoCommon(builder, terms);

        // Set metadata if present (mirrors InternalAggregation.toXContent line 372)
        AggregateProtoUtils.setMetadataIfPresent(terms.getMetadata(), builder::setMeta);

        return builder.build();
    }

    /**
     * Sets common terms-level fields in the protobuf builder.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}
     * which sets doc_count_error_upper_bound, sum_other_doc_count, and iterates buckets.
     *
     * @param builder The UnsignedLongTermsAggregate builder to populate
     * @param terms The UnsignedLongTerms aggregation result
     * @throws IOException if an error occurs during bucket conversion
     */
    private static void doProtoCommon(UnsignedLongTermsAggregate.Builder builder, UnsignedLongTerms terms) throws IOException {
        // Line 600: doc_count_error_upper_bound
        InternalTermsProtoUtils.setDocCountErrorUpperBound(
            terms.getDocCountError(),
            builder::setDocCountErrorUpperBound
        );

        // Line 601: sum_other_doc_count
        InternalTermsProtoUtils.setSumOtherDocCount(
            terms.getSumOfOtherDocCounts(),
            builder::setSumOtherDocCount
        );

        // Lines 602-606: buckets array
        for (InternalTerms.Bucket<?> bucket : terms.getBuckets()) {
            builder.addBuckets(toProtoBucket(bucket));
        }
    }

    /**
     * Converts a single UnsignedLongTerms bucket to protobuf format.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms.Bucket#toXContent}.
     *
     * @param bucket The bucket to convert
     * @return An UnsignedLongTermsBucket protobuf message
     * @throws IOException if an error occurs during bucket conversion
     */
    private static UnsignedLongTermsBucket toProtoBucket(InternalTerms.Bucket<?> bucket) throws IOException {
        UnsignedLongTermsBucket.Builder bucketBuilder = UnsignedLongTermsBucket.newBuilder();

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
     * Sets the key and key_as_string fields for an UnsignedLongTerms bucket.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms.Bucket#keyToXContent}
     * which handles UNSIGNED_LONG_SHIFTED format and conditionally sets key_as_string.
     *
     * @param bucketBuilder The UnsignedLongTermsBucket builder to populate
     * @param bucket The bucket to convert
     */
    private static void keyToProto(UnsignedLongTermsBucket.Builder bucketBuilder, InternalTerms.Bucket<?> bucket) {
        Object key = bucket.getKey();
        BigInteger keyValue = (BigInteger) key;
        bucketBuilder.setKey(keyValue.longValue());

        DocValueFormat format = bucket.getFormat();
        if (format != DocValueFormat.RAW
            && format != DocValueFormat.UNSIGNED_LONG_SHIFTED
            && format != DocValueFormat.UNSIGNED_LONG) {
            String keyAsString = bucket.getKeyAsString();
            if (keyAsString != null) {
                bucketBuilder.setKeyAsString(keyAsString);
            }
        }
    }

}
