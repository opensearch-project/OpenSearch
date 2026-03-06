/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.LongTermsAggregate;
import org.opensearch.protobufs.LongTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LongTermsProtoUtilsTests extends OpenSearchTestCase {

    public void testLongTermsBasicConversion() throws IOException {
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new LongTerms.Bucket(100L, 50, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
        buckets.add(new LongTerms.Bucket(200L, 30, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        LongTerms terms = new LongTerms(
            "numbers",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            false,
            50,
            buckets,
            0,
            thresholds
        );

        LongTermsAggregate result = LongTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(50, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());

        LongTermsBucket bucket0 = result.getBuckets(0);
        assertEquals(50, bucket0.getDocCount());
        assertEquals(100L, bucket0.getKey().getSigned());
        assertFalse(bucket0.hasKeyAsString());
    }

    public void testLongTermsWithDocCountError() throws IOException {
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new LongTerms.Bucket(42L, 100, InternalAggregations.EMPTY, true, 3, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        LongTerms terms = new LongTerms(
            "test_long",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            true,
            0,
            buckets,
            5,
            thresholds
        );

        LongTermsAggregate result = LongTermsProtoUtils.toProto(terms);

        LongTermsBucket bucket = result.getBuckets(0);
        assertTrue(bucket.hasDocCountErrorUpperBound());
        assertEquals(3, bucket.getDocCountErrorUpperBound());
    }

    public void testLongTermsWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("unit", "count");

        List<LongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new LongTerms.Bucket(999L, 25, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        LongTerms terms = new LongTerms(
            "long_with_meta",
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

        LongTermsAggregate result = LongTermsProtoUtils.toProto(terms);

        assertTrue(result.hasMeta());
        assertTrue(result.getMeta().getFieldsMap().containsKey("unit"));
    }

    public void testLongTermsEmptyBuckets() throws IOException {
        List<LongTerms.Bucket> buckets = new ArrayList<>();

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        LongTerms terms = new LongTerms(
            "empty_agg",
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

        LongTermsAggregate result = LongTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getBucketsCount());
    }

    public void testLongTermsWithNestedAggregations() throws IOException {
        // Create nested aggregations
        InternalMax maxAgg = new InternalMax("max_value", 999.99, DocValueFormat.RAW, null);
        InternalMin minAgg = new InternalMin("min_value", 1.0, DocValueFormat.RAW, null);
        InternalAggregations nestedAggs = InternalAggregations.from(List.of(maxAgg, minAgg));

        List<LongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new LongTerms.Bucket(
            123L,
            75,
            nestedAggs,
            false,
            0,
            DocValueFormat.RAW
        ));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        LongTerms terms = new LongTerms(
            "values_with_stats",
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

        LongTermsAggregate result = LongTermsProtoUtils.toProto(terms);

        assertEquals(1, result.getBucketsCount());
        LongTermsBucket bucket = result.getBuckets(0);
        assertEquals(2, bucket.getAggregateCount());
        assertTrue(bucket.containsAggregate("max_value"));
        assertTrue(bucket.containsAggregate("min_value"));

        Aggregate nestedMax = bucket.getAggregateOrThrow("max_value");
        assertTrue(nestedMax.hasMax());
        assertEquals(999.99, nestedMax.getMax().getValue().getDouble(), 0.001);

        Aggregate nestedMin = bucket.getAggregateOrThrow("min_value");
        assertTrue(nestedMin.hasMin());
        assertEquals(1.0, nestedMin.getMin().getValue().getDouble(), 0.001);
    }
}
