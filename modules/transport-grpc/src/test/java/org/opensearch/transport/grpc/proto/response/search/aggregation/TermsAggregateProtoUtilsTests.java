/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.apache.lucene.util.BytesRef;
import org.opensearch.protobufs.LongTermsAggregate;
import org.opensearch.protobufs.LongTermsBucket;
import org.opensearch.protobufs.StringTermsAggregate;
import org.opensearch.protobufs.StringTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TermsAggregateProtoUtilsTests extends OpenSearchTestCase {

    // ===== StringTerms Tests =====

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

        StringTermsAggregate result = TermsAggregateProtoUtils.stringTermsToProto(terms);

        assertNotNull(result);
        assertEquals(100, result.getDocCountErrorUpperBound());
        assertEquals(100, result.getSumOtherDocCount());
        assertEquals(3, result.getBuckets().getStringTermsBucketCount());

        Map<String, StringTermsBucket> bucketMap = result.getBuckets().getStringTermsBucketMap();
        StringTermsBucket bucket0 = bucketMap.get("0");
        assertEquals(100, bucket0.getDocCount());
        assertEquals("apple", bucket0.getKey());
        assertFalse(bucket0.hasDocCountError());
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

        StringTermsAggregate result = TermsAggregateProtoUtils.stringTermsToProto(terms);

        StringTermsBucket bucket = result.getBuckets().getStringTermsBucketMap().get("0");
        assertTrue(bucket.hasDocCountError());
        assertEquals(5, bucket.getDocCountError());
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

        StringTermsAggregate result = TermsAggregateProtoUtils.stringTermsToProto(terms);

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

        StringTermsAggregate result = TermsAggregateProtoUtils.stringTermsToProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getBuckets().getStringTermsBucketCount());
    }

    // ===== LongTerms Tests =====

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

        LongTermsAggregate result = TermsAggregateProtoUtils.longTermsToProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(50, result.getSumOtherDocCount());
        assertEquals(2, result.getBuckets().getLongTermsBucketCount());

        Map<String, LongTermsBucket> bucketMap = result.getBuckets().getLongTermsBucketMap();
        LongTermsBucket bucket0 = bucketMap.get("0");
        assertEquals(50, bucket0.getDocCount());
        assertEquals(100L, bucket0.getKey().getSigned());
        assertEquals("100", bucket0.getKeyAsString());
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

        LongTermsAggregate result = TermsAggregateProtoUtils.longTermsToProto(terms);

        LongTermsBucket bucket = result.getBuckets().getLongTermsBucketMap().get("0");
        assertTrue(bucket.hasDocCountError());
        assertEquals(3, bucket.getDocCountError());
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

        LongTermsAggregate result = TermsAggregateProtoUtils.longTermsToProto(terms);

        assertTrue(result.hasMeta());
        assertTrue(result.getMeta().getFieldsMap().containsKey("unit"));
    }
}
