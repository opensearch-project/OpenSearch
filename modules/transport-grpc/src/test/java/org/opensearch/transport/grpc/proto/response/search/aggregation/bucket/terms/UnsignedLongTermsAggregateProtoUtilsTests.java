/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.UnsignedLongTermsAggregate;
import org.opensearch.protobufs.UnsignedLongTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link UnsignedLongTermsAggregateProtoUtils} verifying it correctly mirrors
 * {@link UnsignedLongTerms#doXContentBody} behavior.
 */
public class UnsignedLongTermsAggregateProtoUtilsTests extends OpenSearchTestCase {

    public void testEmptyBuckets() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, null);

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertEquals(0, result.getBucketsCount());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testSingleBucket() throws IOException {
        UnsignedLongTerms.Bucket bucket = new UnsignedLongTerms.Bucket(
            BigInteger.valueOf(100),
            15,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket), 0, 0, null);

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);

        assertEquals(1, result.getBucketsCount());
        UnsignedLongTermsBucket protoBucket = result.getBuckets(0);
        assertEquals(100L, protoBucket.getKey());
        assertEquals(15L, protoBucket.getDocCount());
        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testLargeUnsignedValue() throws IOException {
        BigInteger largeValue = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        UnsignedLongTerms.Bucket bucket = new UnsignedLongTerms.Bucket(
            largeValue,
            5,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket), 0, 0, null);

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);
        UnsignedLongTermsBucket protoBucket = result.getBuckets(0);

        assertEquals(largeValue.longValue(), protoBucket.getKey());
    }

    public void testMultipleBuckets() throws IOException {
        UnsignedLongTerms.Bucket bucket1 = new UnsignedLongTerms.Bucket(
            BigInteger.valueOf(1),
            100,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        );
        UnsignedLongTerms.Bucket bucket2 = new UnsignedLongTerms.Bucket(
            BigInteger.valueOf(2),
            50,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket1, bucket2), 7, 300, null);

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);

        assertEquals(7, result.getDocCountErrorUpperBound());
        assertEquals(300, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());
    }

    public void testBucketWithDocCountError() throws IOException {
        UnsignedLongTerms.Bucket bucket = new UnsignedLongTerms.Bucket(
            BigInteger.TEN,
            20,
            InternalAggregations.EMPTY,
            true,
            4,
            DocValueFormat.RAW
        );
        UnsignedLongTerms terms = createUnsignedLongTerms("test", List.of(bucket), 0, 0, null);

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);
        UnsignedLongTermsBucket protoBucket = result.getBuckets(0);

        assertEquals(4L, protoBucket.getDocCountErrorUpperBound());
    }

    public void testWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("type", "unsigned");
        metadata.put("max_bits", 64);

        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, metadata);

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain type", result.getMeta().getFieldsMap().containsKey("type"));
        assertTrue("Metadata should contain max_bits", result.getMeta().getFieldsMap().containsKey("max_bits"));
    }

    public void testWithEmptyMetadata() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, new HashMap<>());

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);

        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testWithNullMetadata() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, null);

        UnsignedLongTermsAggregate result = UnsignedLongTermsAggregateProtoUtils.toProto(terms);

        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    private static UnsignedLongTerms createUnsignedLongTerms(
        String name,
        List<UnsignedLongTerms.Bucket> buckets,
        long docCountError,
        long otherDocCount,
        Map<String, Object> metadata
    ) {
        return new UnsignedLongTerms(
            name,
            BucketOrder.count(false),
            BucketOrder.count(false),
            metadata != null ? metadata : Collections.emptyMap(),
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
