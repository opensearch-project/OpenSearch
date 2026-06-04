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
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
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
 * Tests for {@link StringTermsAggregateConverter}.
 */
public class StringTermsAggregateConverterTests extends OpenSearchTestCase {

    private final AggregateProtoConverterRegistry registry = new AggregateProtoConverterRegistryImpl();
    private final StringTermsAggregateConverter converter = createConverter();

    public void testGetHandledAggregationType() {
        assertEquals(StringTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsSterms() throws IOException {
        StringTerms stringTerms = createStringTerms("test", List.of(createBucket("active", 25)), 0, 0, null);

        Aggregate aggregate = converter.toProto(stringTerms).build();

        assertTrue("Should have sterms set", aggregate.hasSterms());
        assertEquals(1, aggregate.getSterms().getBucketsCount());
    }

    public void testEmptyBuckets() throws IOException {
        StringTerms stringTerms = createStringTerms("test", Collections.emptyList(), 0, 0, null);

        StringTermsAggregate result = converter.toProto(stringTerms).build().getSterms();

        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertEquals(0, result.getBucketsCount());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testSingleBucket() throws IOException {
        StringTerms stringTerms = createStringTerms("test", List.of(createBucket("active", 25)), 0, 0, null);

        StringTermsAggregate result = converter.toProto(stringTerms).build().getSterms();

        assertEquals(1, result.getBucketsCount());
        StringTermsBucket protoBucket = result.getBuckets(0);
        assertEquals("active", protoBucket.getKey());
        assertEquals(25L, protoBucket.getDocCount());
    }

    public void testMultipleBuckets() throws IOException {
        StringTerms stringTerms = createStringTerms(
            "test",
            List.of(createBucket("active", 100), createBucket("inactive", 50)),
            3,
            150,
            null
        );

        StringTermsAggregate result = converter.toProto(stringTerms).build().getSterms();

        assertEquals(3, result.getDocCountErrorUpperBound());
        assertEquals(150, result.getSumOtherDocCount());
        assertEquals(2, result.getBucketsCount());
        assertEquals("active", result.getBuckets(0).getKey());
        assertEquals("inactive", result.getBuckets(1).getKey());
    }

    public void testBucketWithDocCountError() throws IOException {
        StringTerms.Bucket bucket = new StringTerms.Bucket(
            new BytesRef("error_test"),
            10,
            InternalAggregations.EMPTY,
            true,
            2,
            DocValueFormat.RAW
        );
        StringTerms stringTerms = createStringTerms("test", List.of(bucket), 0, 0, null);

        StringTermsBucket protoBucket = converter.toProto(stringTerms).build().getSterms().getBuckets(0);

        assertEquals(2L, protoBucket.getDocCountErrorUpperBound());
    }

    public void testWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("source", "test");
        metadata.put("version", 2);

        StringTerms stringTerms = createStringTerms("test", Collections.emptyList(), 0, 0, metadata);

        StringTermsAggregate result = converter.toProto(stringTerms).build().getSterms();

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain source", result.getMeta().getFieldsMap().containsKey("source"));
        assertTrue("Metadata should contain version", result.getMeta().getFieldsMap().containsKey("version"));
    }

    public void testWithEmptyMetadata() throws IOException {
        StringTerms stringTerms = createStringTerms("test", Collections.emptyList(), 0, 0, new HashMap<>());

        StringTermsAggregate result = converter.toProto(stringTerms).build().getSterms();

        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testWithNullMetadata() throws IOException {
        StringTerms stringTerms = createStringTerms("test", Collections.emptyList(), 0, 0, null);

        StringTermsAggregate result = converter.toProto(stringTerms).build().getSterms();

        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    private StringTermsAggregateConverter createConverter() {
        StringTermsAggregateConverter c = new StringTermsAggregateConverter();
        c.setRegistry(registry);
        return c;
    }

    private static StringTerms.Bucket createBucket(String key, long docCount) {
        return new StringTerms.Bucket(new BytesRef(key), docCount, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
    }

    private static StringTerms createStringTerms(
        String name,
        List<StringTerms.Bucket> buckets,
        long docCountError,
        long otherDocCount,
        Map<String, Object> metadata
    ) {
        return new StringTerms(
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
