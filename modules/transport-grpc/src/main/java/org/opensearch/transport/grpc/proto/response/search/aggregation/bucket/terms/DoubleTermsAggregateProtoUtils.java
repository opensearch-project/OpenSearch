/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.DoubleTermsAggregate;
import org.opensearch.protobufs.DoubleTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting {@link DoubleTerms} to {@link DoubleTermsAggregate} protobuf.
 */
class DoubleTermsAggregateProtoUtils {

    private DoubleTermsAggregateProtoUtils() {}

    /**
     * Converts a DoubleTerms aggregation result to DoubleTermsAggregate proto.
     *
     * @param doubleTerms The DoubleTerms aggregation result
     * @return DoubleTermsAggregate proto
     */
    static DoubleTermsAggregate toProto(DoubleTerms doubleTerms) throws IOException {
        DoubleTermsAggregate.Builder termsBuilder = DoubleTermsAggregate.newBuilder();

        termsBuilder.setDocCountErrorUpperBound(doubleTerms.getDocCountError());
        termsBuilder.setSumOtherDocCount(doubleTerms.getSumOfOtherDocCounts());

        for (DoubleTerms.Bucket bucket : doubleTerms.getBuckets()) {
            termsBuilder.addBuckets(convertBucket(bucket));
        }

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, doubleTerms);

        return termsBuilder.build();
    }

    /**
     * Mirroring {@link DoubleTerms.Bucket#keyToXContent(XContentBuilder)}
     */
    private static DoubleTermsBucket convertBucket(DoubleTerms.Bucket bucket) throws IOException {
        DoubleTermsBucket.Builder builder = DoubleTermsBucket.newBuilder();

        builder.setKey((double) bucket.getKey());

        if (bucket.getFormat() != DocValueFormat.RAW) {
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
