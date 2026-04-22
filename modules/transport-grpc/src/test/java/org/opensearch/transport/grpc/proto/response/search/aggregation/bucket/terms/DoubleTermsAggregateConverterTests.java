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
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
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
 * Tests for {@link DoubleTermsAggregateConverter}.
 */
public class DoubleTermsAggregateConverterTests extends OpenSearchTestCase {

    private final AggregateProtoConverterRegistry registry = new AggregateProtoConverterRegistryImpl();
    private final DoubleTermsAggregateConverter converter = createConverter();

    public void testGetHandledAggregationType() {
        assertEquals(DoubleTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsDterms() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(createBucket(3.14, 10, false, 0, DocValueFormat.RAW)), 0, 0, null);

        Aggregate aggregate = converter.toProto(doubleTerms).build();

        assertTrue("Should have dterms set", aggregate.hasDterms());
        assertEquals(1, aggregate.getDterms().getBucketsCount());
    }

    public void testEmptyBuckets() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", Collections.emptyList(), 0, 0, null);

        DoubleTermsAggregate result = converter.toProto(doubleTerms).build().getDterms();

        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertEquals(0, result.getBucketsCount());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testSingleBucket() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(createBucket(3.14, 10, false, 0, DocValueFormat.RAW)), 0, 0, null);

        DoubleTermsAggregate result = converter.toProto(doubleTerms).build().getDterms();

        assertEquals(1, result.getBucketsCount());
        DoubleTermsBucket protoBucket = result.getBuckets(0);
        assertEquals(3.14, protoBucket.getKey(), 0.001);
        assertEquals(10L, protoBucket.getDocCount());
        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testMultipleBuckets() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms(
            "test",
            List.of(createBucket(1.0, 100, false, 0, DocValueFormat.RAW), createBucket(2.5, 50, false, 0, DocValueFormat.RAW)),
            5,
            200,
            null
        );

        DoubleTermsAggregate result = converter.toProto(doubleTerms).build().getDterms();

        assertEquals(5, result.getDocCountErrorUpperBound());
        assertEquals(200, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());
        assertEquals(1.0, result.getBuckets(0).getKey(), 0.001);
        assertEquals(2.5, result.getBuckets(1).getKey(), 0.001);
    }

    public void testBucketWithDocCountError() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(createBucket(9.99, 100, true, 3, DocValueFormat.RAW)), 0, 0, null);

        DoubleTermsBucket protoBucket = converter.toProto(doubleTerms).build().getDterms().getBuckets(0);

        assertEquals(3L, protoBucket.getDocCountErrorUpperBound());
    }

    public void testBucketWithFormattedKey() throws IOException {
        DocValueFormat format = new DocValueFormat.Decimal("0.00");
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(createBucket(3.14159, 5, false, 0, format)), 0, 0, null);

        DoubleTermsBucket protoBucket = converter.toProto(doubleTerms).build().getDterms().getBuckets(0);

        assertEquals(3.14159, protoBucket.getKey(), 0.00001);
        assertTrue("key_as_string should be set with custom format", protoBucket.hasKeyAsString());
        assertEquals("3.14", protoBucket.getKeyAsString());
    }

    public void testBucketWithRawFormatNoKeyAsString() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", List.of(createBucket(42.0, 10, false, 0, DocValueFormat.RAW)), 0, 0, null);

        DoubleTermsBucket protoBucket = converter.toProto(doubleTerms).build().getDterms().getBuckets(0);

        assertFalse("key_as_string should not be set with RAW format", protoBucket.hasKeyAsString());
    }

    public void testWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("color", "red");
        metadata.put("count", 42);

        DoubleTerms doubleTerms = createDoubleTerms("test", Collections.emptyList(), 0, 0, metadata);

        DoubleTermsAggregate result = converter.toProto(doubleTerms).build().getDterms();

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain color", result.getMeta().getFieldsMap().containsKey("color"));
        assertTrue("Metadata should contain count", result.getMeta().getFieldsMap().containsKey("count"));
    }

    public void testWithEmptyMetadata() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", Collections.emptyList(), 0, 0, new HashMap<>());

        DoubleTermsAggregate result = converter.toProto(doubleTerms).build().getDterms();

        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testWithNullMetadata() throws IOException {
        DoubleTerms doubleTerms = createDoubleTerms("test", Collections.emptyList(), 0, 0, null);

        DoubleTermsAggregate result = converter.toProto(doubleTerms).build().getDterms();

        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    private DoubleTermsAggregateConverter createConverter() {
        DoubleTermsAggregateConverter c = new DoubleTermsAggregateConverter();
        c.setRegistry(registry);
        return c;
    }

    private static DoubleTerms.Bucket createBucket(
        double key,
        long docCount,
        boolean showDocCountError,
        long docCountError,
        DocValueFormat format
    ) {
        return new DoubleTerms.Bucket(key, docCount, InternalAggregations.EMPTY, showDocCountError, docCountError, format);
    }

    private static DoubleTerms createDoubleTerms(
        String name,
        List<DoubleTerms.Bucket> buckets,
        long docCountError,
        long otherDocCount,
        Map<String, Object> metadata
    ) {
        return new DoubleTerms(
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
