/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.apache.lucene.util.BytesRef;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AggregateProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithStringTerms() throws IOException {
        // Create StringTerms with one bucket
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new StringTerms.Bucket(new BytesRef("category1"), 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));

        org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds thresholds =
            new org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        StringTerms stringTerms = new StringTerms(
            "my_terms",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            100,
            buckets,
            0,
            thresholds
        );

        Aggregate result = AggregateProtoUtils.toProto(stringTerms);

        assertNotNull(result);
        assertTrue(result.hasSterms());
        assertEquals(1, result.getSterms().getBuckets().getStringTermsBucketCount());
    }

    public void testToProtoWithLongTerms() throws IOException {
        // Create LongTerms with one bucket
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new LongTerms.Bucket(123L, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));

        org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds thresholds =
            new org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        LongTerms longTerms = new LongTerms(
            "my_long_terms",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            50,
            buckets,
            0,
            thresholds
        );

        Aggregate result = AggregateProtoUtils.toProto(longTerms);

        assertNotNull(result);
        assertTrue(result.hasLterms());
        assertEquals(1, result.getLterms().getBuckets().getLongTermsBucketCount());
    }

    public void testToProtoWithNullThrowsException() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> AggregateProtoUtils.toProto(null));
        assertTrue(ex.getMessage().contains("must not be null"));
    }

    public void testToProtoWithUnsupportedTypeThrowsException() {
        // Create a mock unsupported aggregation type
        InternalAggregation unsupported = new InternalAggregation("unsupported", Collections.emptyMap()) {
            @Override
            public String getWriteableName() {
                return "unsupported";
            }

            @Override
            protected void doWriteTo(org.opensearch.core.common.io.stream.StreamOutput out) throws IOException {}

            @Override
            public Object getProperty(List<String> path) {
                return null;
            }

            @Override
            public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
                return null;
            }

            @Override
            protected boolean mustReduceOnSingleInternalAgg() {
                return false;
            }

            @Override
            public org.opensearch.core.xcontent.XContentBuilder doXContentBody(
                org.opensearch.core.xcontent.XContentBuilder builder,
                Params params
            ) throws IOException {
                return builder;
            }
        };

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> AggregateProtoUtils.toProto(unsupported));
        assertTrue(ex.getMessage().contains("Unsupported aggregation type"));
    }
}
