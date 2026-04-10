/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.LongTermsAggregate;
import org.opensearch.protobufs.LongTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link LongTermsAggregateProtoUtils} verifying it correctly mirrors
 * {@link LongTerms#doXContentBody} behavior.
 */
public class LongTermsAggregateProtoUtilsTests extends OpenSearchTestCase {

    public void testEmptyBuckets() throws IOException {
        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, null);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);

        assertNotNull(result);
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertEquals(0, result.getBucketsCount());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testSingleBucket() throws IOException {
        LongTerms.Bucket bucket = new LongTerms.Bucket(42L, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0, null);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);

        assertEquals(1, result.getBucketsCount());
        LongTermsBucket protoBucket = result.getBuckets(0);
        assertEquals(42L, protoBucket.getKey().getSigned());
        assertEquals(10L, protoBucket.getDocCount());
        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testMultipleBuckets() throws IOException {
        LongTerms.Bucket bucket1 = new LongTerms.Bucket(1L, 100, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms.Bucket bucket2 = new LongTerms.Bucket(2L, 50, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket1, bucket2), 5, 200, null);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);

        assertEquals(5, result.getDocCountErrorUpperBound());
        assertEquals(200, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());
        assertEquals(1L, result.getBuckets(0).getKey().getSigned());
        assertEquals(2L, result.getBuckets(1).getKey().getSigned());
    }

    public void testBucketWithDocCountError() throws IOException {
        LongTerms.Bucket bucket = new LongTerms.Bucket(10L, 100, InternalAggregations.EMPTY, true, 3, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0, null);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);
        LongTermsBucket protoBucket = result.getBuckets(0);

        assertEquals(3L, protoBucket.getDocCountErrorUpperBound());
    }

    public void testBucketWithFormattedKey() throws IOException {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        LongTerms.Bucket bucket = new LongTerms.Bucket(1000L, 5, InternalAggregations.EMPTY, false, 0, format);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0, null);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);
        LongTermsBucket protoBucket = result.getBuckets(0);

        assertEquals(1000L, protoBucket.getKey().getSigned());
        assertTrue("key_as_string should be set with custom format", protoBucket.hasKeyAsString());
        assertEquals("1000.00", protoBucket.getKeyAsString());
    }

    public void testBucketWithRawFormatNoKeyAsString() throws IOException {
        LongTerms.Bucket bucket = new LongTerms.Bucket(42L, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms longTerms = createLongTerms("test", List.of(bucket), 0, 0, null);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);
        LongTermsBucket protoBucket = result.getBuckets(0);

        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("color", "blue");
        metadata.put("priority", 5);

        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, metadata);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain color", result.getMeta().getFieldsMap().containsKey("color"));
        assertTrue("Metadata should contain priority", result.getMeta().getFieldsMap().containsKey("priority"));
    }

    public void testWithEmptyMetadata() throws IOException {
        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, new HashMap<>());

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);

        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testWithNullMetadata() throws IOException {
        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, null);

        LongTermsAggregate result = LongTermsAggregateProtoUtils.toProto(longTerms);

        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    private static LongTerms createLongTerms(
        String name,
        List<LongTerms.Bucket> buckets,
        long docCountError,
        long otherDocCount,
        Map<String, Object> metadata
    ) {
        return new LongTerms(
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
