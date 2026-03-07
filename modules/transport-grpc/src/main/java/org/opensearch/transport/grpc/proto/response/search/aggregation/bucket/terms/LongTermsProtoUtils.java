/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.LongTermsAggregate;
import org.opensearch.protobufs.LongTermsBucket;
import org.opensearch.protobufs.LongTermsBucketKey;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting LongTerms aggregation results to LongTermsAggregate protocol buffer format.
 *
 * <p>This utility mirrors the REST API serialization logic from
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody} and
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}
 * but produces Protocol Buffer messages instead of XContent (JSON/YAML).
 *
 * @see LongTerms
 * @see LongTermsAggregate
 */
public class LongTermsProtoUtils {

    private LongTermsProtoUtils() {
        // Utility class
    }

    /**
     * Converts a LongTerms aggregation result to a LongTermsAggregate protobuf message.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody}
     * which delegates to {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}.
     *
     * @param terms The LongTerms aggregation result from OpenSearch
     * @return A LongTermsAggregate protobuf message
     * @throws IOException if an error occurs during conversion
     */
    public static LongTermsAggregate toProto(LongTerms terms) throws IOException {
        LongTermsAggregate.Builder builder = LongTermsAggregate.newBuilder();

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
     * @param builder The LongTermsAggregate builder to populate
     * @param terms The LongTerms aggregation result
     * @throws IOException if an error occurs during bucket conversion
     */
    private static void doProtoCommon(LongTermsAggregate.Builder builder, LongTerms terms) throws IOException {
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
     * Converts a single LongTerms bucket to protobuf format.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms.Bucket#toXContent}.
     *
     * @param bucket The bucket to convert
     * @return A LongTermsBucket protobuf message
     * @throws IOException if an error occurs during bucket conversion
     */
    private static LongTermsBucket toProtoBucket(InternalTerms.Bucket<?> bucket) throws IOException {
        LongTermsBucket.Builder bucketBuilder = LongTermsBucket.newBuilder();

        // Line 191: keyToXContent() - sets key and key_as_string
        keyToProto(bucketBuilder, bucket);

        // Line 192: doc_count
        InternalTermsProtoUtils.setDocCount(bucket.getDocCount(), bucketBuilder::setDocCount);

        // Lines 193-195: doc_count_error_upper_bound (conditional)
        InternalTermsProtoUtils.setDocCountErrorUpperBoundIfApplicable(bucket, bucketBuilder::setDocCountErrorUpperBound);

        // Line 196: aggregations.toXContentInternal() - serialize sub-aggregations without wrapper
        AggregateProtoUtils.toProtoInternal(
            (InternalAggregations) bucket.getAggregations(),
            bucketBuilder::putAggregate
        );

        return bucketBuilder.build();
    }

    /**
     * Sets the key and key_as_string fields for a LongTerms bucket.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.LongTerms.Bucket#keyToXContent}
     * which handles UNSIGNED_LONG_SHIFTED format and conditionally sets key_as_string.
     *
     * @param bucketBuilder The LongTermsBucket builder to populate
     * @param bucket The bucket to convert
     */
    private static void keyToProto(LongTermsBucket.Builder bucketBuilder, InternalTerms.Bucket<?> bucket) {
        long term = bucket.getKeyAsNumber().longValue();
        DocValueFormat format = bucket.getFormat();

        LongTermsBucketKey protoKey;
        if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
            Object formatted = format.format(term);
            String unsignedValue = formatted.toString();
            protoKey = LongTermsBucketKey.newBuilder().setUnsigned(unsignedValue).build();
        } else {
            protoKey = LongTermsBucketKey.newBuilder().setSigned(term).build();
        }

        bucketBuilder.setKey(protoKey);

        if (format != DocValueFormat.RAW && format != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
            String keyAsString = bucket.getKeyAsString();
            if (keyAsString != null) {
                bucketBuilder.setKeyAsString(keyAsString);
            }
        }
    }
}
