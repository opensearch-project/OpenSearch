/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.UnsignedLongTermsAggregate;
import org.opensearch.protobufs.UnsignedLongTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnsignedLongTermsProtoUtilsTests extends OpenSearchTestCase {

    public void testUnsignedLongTermsBasicConversion() throws IOException {
        List<UnsignedLongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new UnsignedLongTerms.Bucket(
            new BigInteger("18446744073709551615"),  // Max unsigned long
            100,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        ));
        buckets.add(new UnsignedLongTerms.Bucket(
            new BigInteger("9223372036854775808"),  // min signed long + 1 (tests sign bit)
            75,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        ));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        UnsignedLongTerms terms = new UnsignedLongTerms(
            "unsigned_values",
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

        UnsignedLongTermsAggregate result = UnsignedLongTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());

        UnsignedLongTermsBucket bucket0 = result.getBuckets(0);
        assertEquals(100, bucket0.getDocCount());
        assertTrue(bucket0.getKey() != 0);  // Proto stores as uint64
    }

    public void testUnsignedLongTermsWithNestedAggregations() throws IOException {
        // Create nested max aggregation
        InternalMax maxAgg = new InternalMax("max_value", 12345.67, DocValueFormat.RAW, null);
        InternalAggregations nestedAggs = InternalAggregations.from(Collections.singletonList(maxAgg));

        List<UnsignedLongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new UnsignedLongTerms.Bucket(
            new BigInteger("12345678901234567890"),
            200,
            nestedAggs,
            false,
            0,
            DocValueFormat.RAW
        ));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        UnsignedLongTerms terms = new UnsignedLongTerms(
            "metric_ids",
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

        UnsignedLongTermsAggregate result = UnsignedLongTermsProtoUtils.toProto(terms);

        assertEquals(1, result.getBucketsCount());
        UnsignedLongTermsBucket bucket = result.getBuckets(0);
        assertEquals(1, bucket.getAggregateCount());
        assertTrue(bucket.containsAggregate("max_value"));

        Aggregate nestedMax = bucket.getAggregateOrThrow("max_value");
        assertTrue(nestedMax.hasMax());
        assertEquals(12345.67, nestedMax.getMax().getValue().getDouble(), 0.001);
    }

    public void testUnsignedLongTermsWithDocCountError() throws IOException {
        List<UnsignedLongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new UnsignedLongTerms.Bucket(
            new BigInteger("1000"),
            50,
            InternalAggregations.EMPTY,
            true,
            10,
            DocValueFormat.RAW
        ));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        UnsignedLongTerms terms = new UnsignedLongTerms(
            "test_unsigned",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            org.opensearch.search.aggregations.BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            25,
            true,
            0,
            buckets,
            15,
            thresholds
        );

        UnsignedLongTermsAggregate result = UnsignedLongTermsProtoUtils.toProto(terms);

        assertEquals(15, result.getDocCountErrorUpperBound());
        UnsignedLongTermsBucket bucket = result.getBuckets(0);
        assertTrue(bucket.hasDocCountErrorUpperBound());
        assertEquals(10, bucket.getDocCountErrorUpperBound());
    }

    public void testUnsignedLongTermsWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("type", "unsigned");
        metadata.put("range", "0-18446744073709551615");

        List<UnsignedLongTerms.Bucket> buckets = new ArrayList<>();
        buckets.add(new UnsignedLongTerms.Bucket(
            new BigInteger("5000"),
            30,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        ));

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        UnsignedLongTerms terms = new UnsignedLongTerms(
            "unsigned_with_meta",
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

        UnsignedLongTermsAggregate result = UnsignedLongTermsProtoUtils.toProto(terms);

        assertTrue(result.hasMeta());
        assertTrue(result.getMeta().getFieldsMap().containsKey("type"));
        assertTrue(result.getMeta().getFieldsMap().containsKey("range"));
    }

    public void testUnsignedLongTermsEmptyBuckets() throws IOException {
        List<UnsignedLongTerms.Bucket> buckets = new ArrayList<>();

        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        UnsignedLongTerms terms = new UnsignedLongTerms(
            "empty_unsigned",
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

        UnsignedLongTermsAggregate result = UnsignedLongTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getBucketsCount());
    }
}
