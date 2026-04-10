/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.StringTermsAggregate;
import org.opensearch.protobufs.StringTermsBucket;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting {@link StringTerms} to {@link StringTermsAggregate} protobuf.
 */
class StringTermsAggregateProtoUtils {

    private StringTermsAggregateProtoUtils() {}

    /**
     * Converts a StringTerms aggregation result to StringTermsAggregate proto.
     *
     * @param stringTerms The StringTerms aggregation result
     * @return StringTermsAggregate proto
     */
    static StringTermsAggregate toProto(StringTerms stringTerms) throws IOException {
        StringTermsAggregate.Builder termsBuilder = StringTermsAggregate.newBuilder();

        termsBuilder.setDocCountErrorUpperBound(stringTerms.getDocCountError());
        termsBuilder.setSumOtherDocCount(stringTerms.getSumOfOtherDocCounts());

        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            termsBuilder.addBuckets(convertBucket(bucket));
        }

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, stringTerms);

        return termsBuilder.build();
    }

    /**
     * Mirroring {@link StringTerms.Bucket#keyToXContent(XContentBuilder)}
     */
    private static StringTermsBucket convertBucket(StringTerms.Bucket bucket) throws IOException {
        StringTermsBucket.Builder builder = StringTermsBucket.newBuilder();

        builder.setKey(bucket.getKeyAsString());

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
