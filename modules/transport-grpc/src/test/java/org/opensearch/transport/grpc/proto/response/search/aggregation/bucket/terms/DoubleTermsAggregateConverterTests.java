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
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link DoubleTermsAggregateConverter}.
 */
public class DoubleTermsAggregateConverterTests extends OpenSearchTestCase {

    private final DoubleTermsAggregateConverter converter = new DoubleTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(DoubleTerms.class, converter.getHandledAggregationType());
    }

    public void testEmptyBuckets() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", Collections.emptyList(), 0, 0);

        Aggregate.Builder result = converter.toProto(doubleTerms);
        Aggregate aggregate = result.build();

        assertEquals(0, aggregate.getDocCountErrorUpperBound());
        assertEquals(0, aggregate.getSumOtherDocCount());
        assertEquals(0, aggregate.getBucketsCount());
    }

    public void testSingleBucket() throws IOException {
        DoubleTerms.Bucket bucket = new DoubleTerms.Bucket(3.14, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(doubleTerms);
        Aggregate aggregate = result.build();

        assertEquals(1, aggregate.getBucketsCount());
        Map<String, ObjectMap.Value> fields = aggregate.getBuckets(0).getFieldsMap();

        assertEquals(3.14, fields.get(Aggregation.CommonFields.KEY.getPreferredName()).getDouble(), 0.001);
        assertEquals(10L, fields.get(Aggregation.CommonFields.DOC_COUNT.getPreferredName()).getInt64());
        assertFalse(fields.containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
    }

    public void testMultipleBuckets() throws IOException {
        DoubleTerms.Bucket bucket1 = new DoubleTerms.Bucket(1.0, 100, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        DoubleTerms.Bucket bucket2 = new DoubleTerms.Bucket(2.5, 50, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(bucket1, bucket2), 5, 200);

        Aggregate.Builder result = converter.toProto(doubleTerms);
        Aggregate aggregate = result.build();

        assertEquals(5, aggregate.getDocCountErrorUpperBound());
        assertEquals(200, aggregate.getSumOtherDocCount());
        assertEquals(2, aggregate.getBucketsCount());

        assertEquals(1.0, aggregate.getBuckets(0).getFieldsMap().get(Aggregation.CommonFields.KEY.getPreferredName()).getDouble(), 0.001);
        assertEquals(2.5, aggregate.getBuckets(1).getFieldsMap().get(Aggregation.CommonFields.KEY.getPreferredName()).getDouble(), 0.001);
    }

    public void testBucketWithDocCountError() throws IOException {
        DoubleTerms.Bucket bucket = new DoubleTerms.Bucket(9.99, 100, InternalAggregations.EMPTY, true, 3, DocValueFormat.RAW);
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(doubleTerms);
        Map<String, ObjectMap.Value> fields = result.build().getBuckets(0).getFieldsMap();

        assertTrue(fields.containsKey("doc_count_error_upper_bound"));
        assertEquals(3L, fields.get("doc_count_error_upper_bound").getInt64());
    }

    public void testBucketWithFormattedKey() throws IOException {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        DoubleTerms.Bucket bucket = new DoubleTerms.Bucket(3.14159, 5, InternalAggregations.EMPTY, false, 0, format);
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(doubleTerms);
        Map<String, ObjectMap.Value> fields = result.build().getBuckets(0).getFieldsMap();

        assertEquals(3.14159, fields.get(Aggregation.CommonFields.KEY.getPreferredName()).getDouble(), 0.00001);
        assertTrue(fields.containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
        assertEquals("3.14", fields.get(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()).getString());
    }

    public void testBucketWithRawFormatNoKeyAsString() throws IOException {
        DoubleTerms.Bucket bucket = new DoubleTerms.Bucket(42.0, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(bucket), 0, 0);

        Aggregate.Builder result = converter.toProto(doubleTerms);
        ObjectMap bucketMap = result.build().getBuckets(0);

        assertFalse(bucketMap.getFieldsMap().containsKey(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName()));
    }

    private static DoubleTerms createDoubleTerms(String name, List<DoubleTerms.Bucket> buckets, long docCountError, long otherDocCount) {
        return new DoubleTerms(
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
