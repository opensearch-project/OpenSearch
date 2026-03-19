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
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link LongTermsAggregateConverter}.
 */
public class LongTermsAggregateConverterTests extends OpenSearchTestCase {

    private final LongTermsAggregateConverter converter = new LongTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(LongTerms.class, converter.getHandledAggregationType());
    }

    public void testEmptyBuckets() throws IOException {
        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0);

        Aggregate.Builder result = converter.toProto(longTerms);
        assertNotNull(result);

        Aggregate aggregate = result.build();
        assertEquals(0, aggregate.getDocCountErrorUpperBound());
        assertEquals(0, aggregate.getSumOtherDocCount());
        assertEquals(0, aggregate.getBucketsCount());
    }

    public void testSingleBucket() throws IOException {
        LongTerms.Bucket bucket = new LongTerms.Bucket(42L, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(longTerms);
        Aggregate aggregate = result.build();

        assertEquals(1, aggregate.getBucketsCount());
        ObjectMap bucketMap = aggregate.getBuckets(0);
        Map<String, ObjectMap.Value> fields = bucketMap.getFieldsMap();

        assertEquals(42L, fields.get(Aggregation.CommonFields.KEY.getPreferredName()).getInt64());
        assertEquals(10L, fields.get(Aggregation.CommonFields.DOC_COUNT.getPreferredName()).getInt64());
        assertFalse(fields.containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
    }

    public void testMultipleBuckets() throws IOException {
        LongTerms.Bucket bucket1 = new LongTerms.Bucket(1L, 100, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms.Bucket bucket2 = new LongTerms.Bucket(2L, 50, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket1, bucket2), 5, 200);

        Aggregate.Builder result = converter.toProto(longTerms);
        Aggregate aggregate = result.build();

        assertEquals(5, aggregate.getDocCountErrorUpperBound());
        assertEquals(200, aggregate.getSumOtherDocCount());
        assertEquals(2, aggregate.getBucketsCount());

        assertEquals(1L, aggregate.getBuckets(0).getFieldsMap().get(Aggregation.CommonFields.KEY.getPreferredName()).getInt64());
        assertEquals(2L, aggregate.getBuckets(1).getFieldsMap().get(Aggregation.CommonFields.KEY.getPreferredName()).getInt64());
    }

    public void testBucketWithDocCountError() throws IOException {
        LongTerms.Bucket bucket = new LongTerms.Bucket(10L, 100, InternalAggregations.EMPTY, true, 3, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(longTerms);
        ObjectMap bucketMap = result.build().getBuckets(0);
        Map<String, ObjectMap.Value> fields = bucketMap.getFieldsMap();

        assertTrue(fields.containsKey("doc_count_error_upper_bound"));
        assertEquals(3L, fields.get("doc_count_error_upper_bound").getInt64());
    }

    public void testBucketWithFormattedKey() throws IOException {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        LongTerms.Bucket bucket = new LongTerms.Bucket(1000L, 5, InternalAggregations.EMPTY, false, 0, format);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(longTerms);
        ObjectMap bucketMap = result.build().getBuckets(0);
        Map<String, ObjectMap.Value> fields = bucketMap.getFieldsMap();

        assertEquals(1000L, fields.get(Aggregation.CommonFields.KEY.getPreferredName()).getInt64());
        assertTrue(fields.containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
        assertEquals("1000.00", fields.get(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()).getString());
    }

    public void testBucketWithRawFormatNoKeyAsString() throws IOException {
        LongTerms.Bucket bucket = new LongTerms.Bucket(42L, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(longTerms);
        ObjectMap bucketMap = result.build().getBuckets(0);

        assertFalse(bucketMap.getFieldsMap().containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
    }

    private static LongTerms createLongTerms(String name, List<LongTerms.Bucket> buckets, long docCountError, long otherDocCount) {
        return new LongTerms(
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
