/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.UnsignedLongTermsAggregate;
import org.opensearch.protobufs.UnsignedLongTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting {@link UnsignedLongTerms} to {@link UnsignedLongTermsAggregate} protobuf.
 */
class UnsignedLongTermsAggregateProtoUtils {

    private UnsignedLongTermsAggregateProtoUtils() {}

    /**
     * Converts an UnsignedLongTerms aggregation result to UnsignedLongTermsAggregate proto.
     *
     * @param unsignedLongTerms The UnsignedLongTerms aggregation result
     * @return UnsignedLongTermsAggregate proto
     */
    static UnsignedLongTermsAggregate toProto(UnsignedLongTerms unsignedLongTerms) throws IOException {
        UnsignedLongTermsAggregate.Builder termsBuilder = UnsignedLongTermsAggregate.newBuilder();

        termsBuilder.setDocCountErrorUpperBound(unsignedLongTerms.getDocCountError());
        termsBuilder.setSumOtherDocCount(unsignedLongTerms.getSumOfOtherDocCounts());

        for (UnsignedLongTerms.Bucket bucket : unsignedLongTerms.getBuckets()) {
            termsBuilder.addBuckets(convertBucket(bucket));
        }

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, unsignedLongTerms);

        return termsBuilder.build();
    }

    /**
     * Mirroring {@link UnsignedLongTerms.Bucket#keyToXContent(XContentBuilder)}
     */
    private static UnsignedLongTermsBucket convertBucket(UnsignedLongTerms.Bucket bucket) throws IOException {
        UnsignedLongTermsBucket.Builder builder = UnsignedLongTermsBucket.newBuilder();

        builder.setKey(((Number) bucket.getKey()).longValue());

        if (bucket.getFormat() != DocValueFormat.RAW
            && bucket.getFormat() != DocValueFormat.UNSIGNED_LONG
            && bucket.getFormat() != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
            builder.setKeyAsString(bucket.getKeyAsString());
        }

        builder.setDocCount(bucket.getDocCount());
        if (bucket.showDocCountError()) {
            builder.setDocCountErrorUpperBound(bucket.getDocCountError());
        }

        for (Aggregation subAgg : bucket.getAggregations()) {
            if (subAgg instanceof InternalAggregation internalAgg) {
                builder.getMutableAggregate().put(subAgg.getName(), AggregateProtoUtils.toProto(internalAgg));
            } else {
                throw new IllegalStateException(
                    "Unexpected aggregation type in terms bucket sub-aggregations: "
                        + subAgg.getClass().getName()
                        + " (name="
                        + subAgg.getName()
                        + ")"
                );
            }
        }

        return builder.build();
    }
}
