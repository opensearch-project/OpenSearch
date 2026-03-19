/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link StringTermsAggregateConverter}.
 */
public class StringTermsAggregateConverterTests extends OpenSearchTestCase {

    private final StringTermsAggregateConverter converter = new StringTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(StringTerms.class, converter.getHandledAggregationType());
    }

    public void testEmptyBuckets() throws IOException {
        StringTerms stringTerms = createStringTerms("test", Collections.emptyList(), 0, 0);

        Aggregate.Builder result = converter.toProto(stringTerms);
        Aggregate aggregate = result.build();

        assertEquals(0, aggregate.getDocCountErrorUpperBound());
        assertEquals(0, aggregate.getSumOtherDocCount());
        assertEquals(0, aggregate.getBucketsCount());
    }

    public void testSingleBucket() throws IOException {
        StringTerms.Bucket bucket = new StringTerms.Bucket(
            new BytesRef("active"), 25, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW
        );
        StringTerms stringTerms = createStringTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(stringTerms);
        Aggregate aggregate = result.build();

        assertEquals(1, aggregate.getBucketsCount());
        Map<String, ObjectMap.Value> fields = aggregate.getBuckets(0).getFieldsMap();

        assertEquals("active", fields.get(Aggregation.CommonFields.KEY.getPreferredName()).getString());
        assertEquals(25L, fields.get(Aggregation.CommonFields.DOC_COUNT.getPreferredName()).getInt64());
        assertFalse(fields.containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
    }

    public void testMultipleBuckets() throws IOException {
        StringTerms.Bucket bucket1 = new StringTerms.Bucket(
            new BytesRef("active"), 100, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW
        );
        StringTerms.Bucket bucket2 = new StringTerms.Bucket(
            new BytesRef("inactive"), 50, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW
        );
        StringTerms stringTerms = createStringTerms("test", List.of(bucket1, bucket2), 3, 150);

        Aggregate.Builder result = converter.toProto(stringTerms);
        Aggregate aggregate = result.build();

        assertEquals(3, aggregate.getDocCountErrorUpperBound());
        assertEquals(150, aggregate.getSumOtherDocCount());
        assertEquals(2, aggregate.getBucketsCount());

        assertEquals("active", aggregate.getBuckets(0).getFieldsMap().get(Aggregation.CommonFields.KEY.getPreferredName()).getString());
        assertEquals("inactive", aggregate.getBuckets(1).getFieldsMap().get(Aggregation.CommonFields.KEY.getPreferredName()).getString());
    }

    public void testBucketWithDocCountError() throws IOException {
        StringTerms.Bucket bucket = new StringTerms.Bucket(
            new BytesRef("error_test"), 10, InternalAggregations.EMPTY, true, 2, DocValueFormat.RAW
        );
        StringTerms stringTerms = createStringTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(stringTerms);
        Map<String, ObjectMap.Value> fields = result.build().getBuckets(0).getFieldsMap();

        assertTrue(fields.containsKey("doc_count_error_upper_bound"));
        assertEquals(2L, fields.get("doc_count_error_upper_bound").getInt64());
    }

    private static StringTerms createStringTerms(String name, List<StringTerms.Bucket> buckets, long docCountError, long otherDocCount) {
        return new StringTerms(
            name,
            BucketOrder.count(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            10,
            false,
            otherDocCount,
            buckets,
            docCountError,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, -1)
        );
    }
}
