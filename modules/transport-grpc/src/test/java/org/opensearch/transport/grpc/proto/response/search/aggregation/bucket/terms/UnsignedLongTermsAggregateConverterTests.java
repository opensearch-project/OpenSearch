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
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link UnsignedLongTermsAggregateConverter}.
 */
public class UnsignedLongTermsAggregateConverterTests extends OpenSearchTestCase {

    private final AggregateProtoConverterRegistry registry = new AggregateProtoConverterRegistryImpl();
    private final UnsignedLongTermsAggregateConverter converter = createConverter();

    public void testGetHandledAggregationType() {
        assertEquals(UnsignedLongTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsUlterms() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms(
            "test",
            List.of(createBucket(BigInteger.valueOf(100), 15, false, 0, DocValueFormat.RAW)),
            0,
            0,
            null
        );

        Aggregate aggregate = converter.toProto(terms).build();

        assertTrue("Should have ulterms set", aggregate.hasUlterms());
        assertEquals(1, aggregate.getUlterms().getBucketsCount());
    }

    public void testEmptyBuckets() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, null);

        UnsignedLongTermsAggregate result = converter.toProto(terms).build().getUlterms();

        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertEquals(0, result.getBucketsCount());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testSingleBucket() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms(
            "test",
            List.of(createBucket(BigInteger.valueOf(100), 15, false, 0, DocValueFormat.RAW)),
            0,
            0,
            null
        );

        UnsignedLongTermsAggregate result = converter.toProto(terms).build().getUlterms();

        assertEquals(1, result.getBucketsCount());
        UnsignedLongTermsBucket protoBucket = result.getBuckets(0);
        assertEquals(100L, protoBucket.getKey());
        assertEquals(15L, protoBucket.getDocCount());
        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testLargeUnsignedValue() throws IOException {
        BigInteger largeValue = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        UnsignedLongTerms terms = createUnsignedLongTerms(
            "test",
            List.of(createBucket(largeValue, 5, false, 0, DocValueFormat.RAW)),
            0,
            0,
            null
        );

        UnsignedLongTermsBucket protoBucket = converter.toProto(terms).build().getUlterms().getBuckets(0);

        assertEquals(largeValue.longValue(), protoBucket.getKey());
    }

    public void testMultipleBuckets() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms(
            "test",
            List.of(
                createBucket(BigInteger.valueOf(1), 100, false, 0, DocValueFormat.RAW),
                createBucket(BigInteger.valueOf(2), 50, false, 0, DocValueFormat.RAW)
            ),
            7,
            300,
            null
        );

        UnsignedLongTermsAggregate result = converter.toProto(terms).build().getUlterms();

        assertEquals(7, result.getDocCountErrorUpperBound());
        assertEquals(300, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());
    }

    public void testBucketWithDocCountError() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms(
            "test",
            List.of(createBucket(BigInteger.TEN, 20, true, 4, DocValueFormat.RAW)),
            0,
            0,
            null
        );

        UnsignedLongTermsBucket protoBucket = converter.toProto(terms).build().getUlterms().getBuckets(0);

        assertEquals(4L, protoBucket.getDocCountErrorUpperBound());
    }

    public void testWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("type", "unsigned");
        metadata.put("max_bits", 64);

        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, metadata);

        UnsignedLongTermsAggregate result = converter.toProto(terms).build().getUlterms();

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain type", result.getMeta().getFieldsMap().containsKey("type"));
        assertTrue("Metadata should contain max_bits", result.getMeta().getFieldsMap().containsKey("max_bits"));
    }

    public void testWithEmptyMetadata() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, new HashMap<>());

        UnsignedLongTermsAggregate result = converter.toProto(terms).build().getUlterms();

        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testWithNullMetadata() throws IOException {
        UnsignedLongTerms terms = createUnsignedLongTerms("test", Collections.emptyList(), 0, 0, null);

        UnsignedLongTermsAggregate result = converter.toProto(terms).build().getUlterms();

        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    private UnsignedLongTermsAggregateConverter createConverter() {
        UnsignedLongTermsAggregateConverter c = new UnsignedLongTermsAggregateConverter();
        c.setRegistry(registry);
        return c;
    }

    private static UnsignedLongTerms.Bucket createBucket(
        BigInteger key,
        long docCount,
        boolean showDocCountError,
        long docCountError,
        DocValueFormat format
    ) {
        return new UnsignedLongTerms.Bucket(key, docCount, InternalAggregations.EMPTY, showDocCountError, docCountError, format);
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
