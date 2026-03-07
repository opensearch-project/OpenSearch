/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.DoubleTermsAggregate;
import org.opensearch.protobufs.DoubleTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DoubleTermsProtoUtilsTests extends OpenSearchTestCase {

    public void testDoubleTermsBasicConversion() throws IOException {
        List<DoubleTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new DoubleTerms.Bucket(99.5, 100L, InternalAggregations.EMPTY, false, 0L, DocValueFormat.RAW));
        buckets.add(new DoubleTerms.Bucket(45.2, 75L, InternalAggregations.EMPTY, false, 0L, DocValueFormat.RAW));
        buckets.add(new DoubleTerms.Bucket(12.8, 50L, InternalAggregations.EMPTY, false, 0L, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        DoubleTerms terms = new DoubleTerms(
            "prices",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            false,
            100,
            buckets,
            100,
            thresholds
        );

        DoubleTermsAggregate result = DoubleTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(100, result.getDocCountErrorUpperBound());
        assertEquals(100, result.getSumOtherDocCount());
        assertEquals(3, result.getBucketsCount());

        DoubleTermsBucket bucket0 = result.getBuckets(0);
        assertEquals(100, bucket0.getDocCount());
        assertEquals(99.5, bucket0.getKey(), 0.001);
        assertFalse(bucket0.hasDocCountErrorUpperBound());
    }

    public void testDoubleTermsWithDocCountError() throws IOException {
        List<DoubleTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new DoubleTerms.Bucket(3.14159, 200L, InternalAggregations.EMPTY, true, 7L, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        DoubleTerms terms = new DoubleTerms(
            "test_double",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            true,
            0,
            buckets,
            20,
            thresholds
        );

        DoubleTermsAggregate result = DoubleTermsProtoUtils.toProto(terms);

        assertEquals(20, result.getDocCountErrorUpperBound());
        DoubleTermsBucket bucket = result.getBuckets(0);
        assertTrue(bucket.hasDocCountErrorUpperBound());
        assertEquals(7, bucket.getDocCountErrorUpperBound());
    }

    public void testDoubleTermsWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("unit", "USD");
        metadata.put("precision", 2);

        List<DoubleTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new DoubleTerms.Bucket(19.99, 50L, InternalAggregations.EMPTY, false, 0L, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        DoubleTerms terms = new DoubleTerms(
            "double_with_meta",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            metadata,
            DocValueFormat.RAW,
            25,
            false,
            0,
            buckets,
            0,
            thresholds
        );

        DoubleTermsAggregate result = DoubleTermsProtoUtils.toProto(terms);

        assertTrue(result.hasMeta());
        assertTrue(result.getMeta().getFieldsMap().containsKey("unit"));
        assertTrue(result.getMeta().getFieldsMap().containsKey("precision"));
    }

    public void testDoubleTermsEmptyBuckets() throws IOException {
        List<DoubleTerms.Bucket> buckets = new ArrayList<>();

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        DoubleTerms terms = new DoubleTerms(
            "empty_double_agg",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            false,
            0,
            buckets,
            0,
            thresholds
        );

        DoubleTermsAggregate result = DoubleTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getBucketsCount());
    }

    public void testDoubleTermsWithNestedAggregations() throws IOException {
        // Create nested aggregations
        InternalMax maxAgg = new InternalMax("max_quantity", 1000.0, DocValueFormat.RAW, null);
        InternalMin minAgg = new InternalMin("min_quantity", 10.0, DocValueFormat.RAW, null);
        InternalAggregations nestedAggs = InternalAggregations.from(List.of(maxAgg, minAgg));

        List<DoubleTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new DoubleTerms.Bucket(
            49.99,
            150L,
            nestedAggs,
            false,
            0L,
            DocValueFormat.RAW
        ));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        DoubleTerms terms = new DoubleTerms(
            "price_ranges",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            false,
            0,
            buckets,
            0,
            thresholds
        );

        DoubleTermsAggregate result = DoubleTermsProtoUtils.toProto(terms);

        assertEquals(1, result.getBucketsCount());
        DoubleTermsBucket bucket = result.getBuckets(0);
        assertEquals(2, bucket.getAggregateCount());
        assertTrue(bucket.containsAggregate("max_quantity"));
        assertTrue(bucket.containsAggregate("min_quantity"));

        Aggregate nestedMax = bucket.getAggregateOrThrow("max_quantity");
        assertTrue(nestedMax.hasMax());
        assertEquals(1000.0, nestedMax.getMax().getValue().getDouble(), 0.001);

        Aggregate nestedMin = bucket.getAggregateOrThrow("min_quantity");
        assertTrue(nestedMin.hasMin());
        assertEquals(10.0, nestedMin.getMin().getValue().getDouble(), 0.001);
    }
}
