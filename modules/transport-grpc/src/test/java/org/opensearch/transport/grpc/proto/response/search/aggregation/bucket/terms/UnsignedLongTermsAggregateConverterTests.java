/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link UnsignedLongTermsAggregateConverter}.
 */
public class UnsignedLongTermsAggregateConverterTests extends OpenSearchTestCase {

    private final UnsignedLongTermsAggregateConverter converter = new UnsignedLongTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(UnsignedLongTerms.class, converter.getHandledAggregationType());
    }

    public void testEmptyBuckets() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0);

        Aggregate.Builder result = converter.toProto(terms);
        Aggregate aggregate = result.build();

        assertEquals(0, aggregate.getDocCountErrorUpperBound());
        assertEquals(0, aggregate.getSumOtherDocCount());
        assertEquals(0, aggregate.getBucketsCount());
    }

    public void testSingleBucket() throws IOException {
        UnsignedLongTerms.Bucket bucket = new UnsignedLongTerms.Bucket(
            BigInteger.valueOf(100), 15, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(terms);
        Aggregate aggregate = result.build();

        assertEquals(1, aggregate.getBucketsCount());
        Map<String, ObjectMap.Value> fields = aggregate.getBuckets(0).getFieldsMap();

        assertEquals("100", fields.get(Aggregation.CommonFields.KEY.getPreferredName()).getString());
        assertEquals(15L, fields.get(Aggregation.CommonFields.DOC_COUNT.getPreferredName()).getInt64());
        assertFalse(fields.containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
    }

    public void testLargeUnsignedValue() throws IOException {
        BigInteger largeValue = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        UnsignedLongTerms.Bucket bucket = new UnsignedLongTerms.Bucket(
            largeValue, 5, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(terms);
        Map<String, ObjectMap.Value> fields = result.build().getBuckets(0).getFieldsMap();

        assertEquals(largeValue.toString(), fields.get(Aggregation.CommonFields.KEY.getPreferredName()).getString());
    }

    public void testMultipleBuckets() throws IOException {
        UnsignedLongTerms.Bucket bucket1 = new UnsignedLongTerms.Bucket(
            BigInteger.valueOf(1), 100, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW
        );
        UnsignedLongTerms.Bucket bucket2 = new UnsignedLongTerms.Bucket(
            BigInteger.valueOf(2), 50, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket1, bucket2), 7, 300);

        Aggregate.Builder result = converter.toProto(terms);
        Aggregate aggregate = result.build();

        assertEquals(7, aggregate.getDocCountErrorUpperBound());
        assertEquals(300, aggregate.getSumOtherDocCount());
        assertEquals(2, aggregate.getBucketsCount());
    }

    public void testBucketWithDocCountError() throws IOException {
        UnsignedLongTerms.Bucket bucket = new UnsignedLongTerms.Bucket(
            BigInteger.TEN, 20, InternalAggregations.EMPTY, true, 4, DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(terms);
        Map<String, ObjectMap.Value> fields = result.build().getBuckets(0).getFieldsMap();

        assertTrue(fields.containsKey("doc_count_error_upper_bound"));
        assertEquals(4L, fields.get("doc_count_error_upper_bound").getInt64());
    }

    private static UnsignedLongTerms createUnsignedLongTerms(
        String name,
        List<UnsignedLongTerms.Bucket> buckets,
        long docCountError,
        long otherDocCount
    ) {
        return new UnsignedLongTerms(
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
