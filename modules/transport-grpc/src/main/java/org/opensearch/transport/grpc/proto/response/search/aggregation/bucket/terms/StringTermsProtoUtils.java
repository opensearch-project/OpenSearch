/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.StringTermsAggregate;
import org.opensearch.protobufs.StringTermsBucket;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting StringTerms aggregation results to StringTermsAggregate protocol buffer format.
 *
 * <p>This utility mirrors the REST API serialization logic from
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody} and
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}
 * but produces Protocol Buffer messages instead of XContent (JSON/YAML).
 *
 * @see StringTerms
 * @see StringTermsAggregate
 */
public class StringTermsProtoUtils {

    private StringTermsProtoUtils() {
        // Utility class
    }

    /**
     * Converts a StringTerms aggregation result to a StringTermsAggregate protobuf message.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms#doXContentBody}
     * which delegates to {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms#doXContentCommon}.
     *
     * @param terms The StringTerms aggregation result from OpenSearch
     * @return A StringTermsAggregate protobuf message
     * @throws IOException if an error occurs during conversion
     */
    public static StringTermsAggregate toProto(StringTerms terms) throws IOException {
        StringTermsAggregate.Builder builder = StringTermsAggregate.newBuilder();

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
     * @param builder The StringTermsAggregate builder to populate
     * @param terms The StringTerms aggregation result
     * @throws IOException if an error occurs during bucket conversion
     */
    private static void doProtoCommon(StringTermsAggregate.Builder builder, StringTerms terms) throws IOException {
        InternalTermsProtoUtils.setDocCountErrorUpperBound(
            terms.getDocCountError(),
            builder::setDocCountErrorUpperBound
        );

        InternalTermsProtoUtils.setSumOtherDocCount(
            terms.getSumOfOtherDocCounts(),
            builder::setSumOtherDocCount
        );

        for (StringTerms.Bucket bucket : terms.getBuckets()) {
            builder.addBuckets(toProtoBucket(bucket));
        }
    }

    /**
     * Converts a single StringTerms bucket to protobuf format.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms.Bucket#toXContent}.
     *
     * @param bucket The bucket to convert
     * @return A StringTermsBucket protobuf message
     * @throws IOException if an error occurs during bucket conversion
     */
    private static StringTermsBucket toProtoBucket(StringTerms.Bucket bucket) throws IOException {
        StringTermsBucket.Builder bucketBuilder = StringTermsBucket.newBuilder();

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
     * Sets the key field for a StringTerms bucket.
     *
     * <p>Mirrors {@link org.opensearch.search.aggregations.bucket.terms.StringTerms.Bucket#keyToXContent}
     * which handles BytesRef to String conversion.
     *
     * @param bucketBuilder The StringTermsBucket builder to populate
     * @param bucket The StringTerms bucket
     */
    private static void keyToProto(StringTermsBucket.Builder bucketBuilder, StringTerms.Bucket bucket) {
        String keyValue = bucket.getKeyAsString();
        bucketBuilder.setKey(keyValue);
    }
}
