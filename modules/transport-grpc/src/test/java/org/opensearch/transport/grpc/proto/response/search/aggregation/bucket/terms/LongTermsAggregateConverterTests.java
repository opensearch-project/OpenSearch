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
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link LongTermsAggregateConverter}.
 */
public class LongTermsAggregateConverterTests extends OpenSearchTestCase {

    private final AggregateProtoConverterRegistry registry = new AggregateProtoConverterRegistryImpl();
    private final LongTermsAggregateConverter converter = createConverter();

    public void testGetHandledAggregationType() {
        assertEquals(LongTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsLterms() throws IOException {
        LongTerms longTerms = createLongTerms("test", List.of(createBucket(42L, 10, false, 0, DocValueFormat.RAW)), 0, 0, null);

        Aggregate aggregate = converter.toProto(longTerms).build();

        assertTrue("Should have lterms set", aggregate.hasLterms());
        assertEquals(1, aggregate.getLterms().getBucketsCount());
    }

    public void testEmptyBuckets() throws IOException {
        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, null);

        LongTermsAggregate result = converter.toProto(longTerms).build().getLterms();

        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertEquals(0, result.getBucketsCount());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testSingleBucket() throws IOException {
        LongTerms longTerms = createLongTerms("test", List.of(createBucket(42L, 10, false, 0, DocValueFormat.RAW)), 0, 0, null);

        LongTermsAggregate result = converter.toProto(longTerms).build().getLterms();

        assertEquals(1, result.getBucketsCount());
        LongTermsBucket protoBucket = result.getBuckets(0);
        assertEquals(42L, protoBucket.getKey().getSigned());
        assertEquals(10L, protoBucket.getDocCount());
        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testMultipleBuckets() throws IOException {
        LongTerms longTerms = createLongTerms(
            "test",
            List.of(createBucket(1L, 100, false, 0, DocValueFormat.RAW), createBucket(2L, 50, false, 0, DocValueFormat.RAW)),
            5,
            200,
            null
        );

        LongTermsAggregate result = converter.toProto(longTerms).build().getLterms();

        assertEquals(5, result.getDocCountErrorUpperBound());
        assertEquals(200, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());
        assertEquals(1L, result.getBuckets(0).getKey().getSigned());
        assertEquals(2L, result.getBuckets(1).getKey().getSigned());
    }

    public void testBucketWithDocCountError() throws IOException {
        LongTerms longTerms = createLongTerms("test", List.of(createBucket(10L, 100, true, 3, DocValueFormat.RAW)), 0, 0, null);

        LongTermsBucket protoBucket = converter.toProto(longTerms).build().getLterms().getBuckets(0);

        assertEquals(3L, protoBucket.getDocCountErrorUpperBound());
    }

    public void testBucketWithFormattedKey() throws IOException {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        LongTerms longTerms = createLongTerms("test", List.of(createBucket(1000L, 5, false, 0, format)), 0, 0, null);

        LongTermsBucket protoBucket = converter.toProto(longTerms).build().getLterms().getBuckets(0);

        assertEquals(1000L, protoBucket.getKey().getSigned());
        assertTrue("key_as_string should be set with custom format", protoBucket.hasKeyAsString());
        assertEquals("1000.00", protoBucket.getKeyAsString());
    }

    public void testBucketWithRawFormatNoKeyAsString() throws IOException {
        LongTerms longTerms = createLongTerms("test", List.of(createBucket(42L, 10, false, 0, DocValueFormat.RAW)), 0, 0, null);

        LongTermsBucket protoBucket = converter.toProto(longTerms).build().getLterms().getBuckets(0);

        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("color", "blue");
        metadata.put("priority", 5);

        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, metadata);

        LongTermsAggregate result = converter.toProto(longTerms).build().getLterms();

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain color", result.getMeta().getFieldsMap().containsKey("color"));
        assertTrue("Metadata should contain priority", result.getMeta().getFieldsMap().containsKey("priority"));
    }

    public void testWithEmptyMetadata() throws IOException {
        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, new HashMap<>());

        LongTermsAggregate result = converter.toProto(longTerms).build().getLterms();

        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testWithNullMetadata() throws IOException {
        LongTerms longTerms = createLongTerms("test", Collections.emptyList(), 0, 0, null);

        LongTermsAggregate result = converter.toProto(longTerms).build().getLterms();

        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    private LongTermsAggregateConverter createConverter() {
        LongTermsAggregateConverter c = new LongTermsAggregateConverter();
        c.setRegistry(registry);
        return c;
    }

    private static LongTerms.Bucket createBucket(
        long key,
        long docCount,
        boolean showDocCountError,
        long docCountError,
        DocValueFormat format
    ) {
        return new LongTerms.Bucket(key, docCount, InternalAggregations.EMPTY, showDocCountError, docCountError, format);
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
