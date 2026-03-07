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
import org.opensearch.protobufs.StringTermsAggregate;
import org.opensearch.protobufs.StringTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringTermsProtoUtilsTests extends OpenSearchTestCase {

    public void testStringTermsBasicConversion() throws IOException {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new StringTerms.Bucket(new BytesRef("apple"), 100, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
        buckets.add(new StringTerms.Bucket(new BytesRef("banana"), 75, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
        buckets.add(new StringTerms.Bucket(new BytesRef("cherry"), 50, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        StringTerms terms = new StringTerms(
            "products",
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

        StringTermsAggregate result = StringTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(100, result.getDocCountErrorUpperBound());
        assertEquals(100, result.getSumOtherDocCount());
        assertEquals(3, result.getBucketsCount());

        StringTermsBucket bucket0 = result.getBuckets(0);
        assertEquals(100, bucket0.getDocCount());
        assertEquals("apple", bucket0.getKey());
        assertFalse(bucket0.hasDocCountErrorUpperBound());
    }

    public void testStringTermsWithDocCountError() throws IOException {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new StringTerms.Bucket(new BytesRef("test"), 10, InternalAggregations.EMPTY, true, 5, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        StringTerms terms = new StringTerms(
            "test_agg",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            true,
            0,
            buckets,
            10,
            thresholds
        );

        StringTermsAggregate result = StringTermsProtoUtils.toProto(terms);

        StringTermsBucket bucket = result.getBuckets(0);
        assertTrue(bucket.hasDocCountErrorUpperBound());
        assertEquals(5, bucket.getDocCountErrorUpperBound());
    }

    public void testStringTermsWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("color", "blue");
        metadata.put("size", 42);

        List<StringTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new StringTerms.Bucket(new BytesRef("item"), 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        StringTerms terms = new StringTerms(
            "test_with_meta",
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

        StringTermsAggregate result = StringTermsProtoUtils.toProto(terms);

        assertTrue(result.hasMeta());
        assertTrue(result.getMeta().getFieldsMap().containsKey("color"));
    }

    public void testStringTermsEmptyBuckets() throws IOException {
        List<StringTerms.Bucket> buckets = new ArrayList<>();

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        StringTerms terms = new StringTerms(
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

        StringTermsAggregate result = StringTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getBucketsCount());
    }

    public void testStringTermsWithNestedAggregations() throws IOException {
        // Create nested max aggregation
        InternalMax maxAgg = new InternalMax("max_price", 99.99, DocValueFormat.RAW, null);
        InternalMin minAgg = new InternalMin("min_price", 10.50, DocValueFormat.RAW, null);
        InternalAggregations nestedAggs = InternalAggregations.from(List.of(maxAgg, minAgg));

        // Create bucket with nested aggregations
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new StringTerms.Bucket(
            new BytesRef("electronics"),
            50,
            nestedAggs,
            false,
            0,
            DocValueFormat.RAW
        ));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        StringTerms terms = new StringTerms(
            "categories",
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

        StringTermsAggregate result = StringTermsProtoUtils.toProto(terms);

        assertEquals(1, result.getBucketsCount());
        StringTermsBucket bucket = result.getBuckets(0);
        assertEquals(2, bucket.getAggregateCount());
        assertTrue(bucket.containsAggregate("max_price"));
        assertTrue(bucket.containsAggregate("min_price"));

        Aggregate nestedMax = bucket.getAggregateOrThrow("max_price");
        assertTrue(nestedMax.hasMax());
        assertEquals(99.99, nestedMax.getMax().getValue().getDouble(), 0.001);

        Aggregate nestedMin = bucket.getAggregateOrThrow("min_price");
        assertTrue(nestedMin.hasMin());
        assertEquals(10.50, nestedMin.getMin().getValue().getDouble(), 0.001);
    }
}
