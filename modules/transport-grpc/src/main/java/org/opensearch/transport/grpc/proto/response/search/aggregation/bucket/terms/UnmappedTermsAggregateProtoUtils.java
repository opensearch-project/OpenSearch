/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.UnmappedTermsAggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting {@link UnmappedTerms} to {@link UnmappedTermsAggregate} protobuf.
 * Mirroring {@link UnmappedTerms#doXContentBody(XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)}
 * which writes zero doc_count_error_upper_bound, zero sum_other_doc_count, and an empty buckets array.
 */
class UnmappedTermsAggregateProtoUtils {

    private UnmappedTermsAggregateProtoUtils() {}

    /**
     * Converts an UnmappedTerms aggregation result to UnmappedTermsAggregate proto.
     *
     * @param aggregation The UnmappedTerms aggregation result
     * @return UnmappedTermsAggregate proto
     */
    static UnmappedTermsAggregate toProto(InternalAggregation aggregation) throws IOException {
        UnmappedTermsAggregate.Builder termsBuilder = UnmappedTermsAggregate.newBuilder();
        termsBuilder.setDocCountErrorUpperBound(0);
        termsBuilder.setSumOtherDocCount(0);

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, aggregation);

        return termsBuilder.build();
    }
}
