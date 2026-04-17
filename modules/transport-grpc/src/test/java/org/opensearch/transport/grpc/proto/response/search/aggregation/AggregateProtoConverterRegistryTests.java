/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.apache.lucene.util.BytesRef;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MaxAggregateProtoConverter;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MinAggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link AggregateProtoConverterSpiRegistry} and {@link AggregateProtoConverterRegistryImpl}
 * verifying the registry pattern works correctly for aggregate converters.
 */
public class AggregateProtoConverterRegistryTests extends OpenSearchTestCase {

    public void testRegistryLoadsBuiltInConverters() {
        AggregateProtoConverterRegistryImpl registry = new AggregateProtoConverterRegistryImpl();

        assertEquals("Should have 7 built-in converters", 7, registry.size());
    }

    public void testRegisterConverterSucceeds() {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();
        MinAggregateProtoConverter converter = new MinAggregateProtoConverter();

        registry.registerConverter(converter);

        assertEquals("Should have 1 converter registered", 1, registry.size());
    }

    public void testRegisterNullConverterThrowsException() {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(null));
        assertTrue("Exception message should mention null converter", ex.getMessage().contains("Converter cannot be null"));
    }

    public void testRegisterConverterWithNullTypeThrowsException() {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();

        AggregateProtoConverter converter = new AggregateProtoConverter() {
            @Override
            public Class<? extends InternalAggregation> getHandledAggregationType() {
                return null;
            }

            @Override
            public Aggregate.Builder toProto(InternalAggregation aggregation) {
                return Aggregate.newBuilder();
            }
        };

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(converter));
        assertTrue("Exception message should mention null type", ex.getMessage().contains("Handled aggregation type cannot be null"));
    }

    public void testRegisterAndConvertMinAggregation() throws IOException {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();
        registry.registerConverter(new MinAggregateProtoConverter());

        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = registry.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have min set", result.hasMin());
        assertTrue("Value should be double", result.getMin().getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getMin().getValue().getDouble(), 0.001);
        assertFalse("Should not have metadata", result.getMin().hasMeta());
    }

    public void testRegisterAndConvertMaxAggregation() throws IOException {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();
        registry.registerConverter(new MaxAggregateProtoConverter());

        InternalMax internalMax = new InternalMax("max_price", 99.9, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = registry.toProto(internalMax);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have max set", result.hasMax());
        assertTrue("Value should be double", result.getMax().getValue().hasDouble());
        assertEquals("Value should match", 99.9, result.getMax().getValue().getDouble(), 0.001);
        assertFalse("Should not have metadata", result.getMax().hasMeta());
    }

    public void testConvertNullAggregationThrowsException() {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> registry.toProto(null));
        assertTrue("Exception message should mention null", ex.getMessage().contains("must not be null"));
    }

    public void testConvertUnregisteredTypeThrowsException() {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();

        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, Collections.emptyMap());

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> registry.toProto(internalMin));
        assertTrue("Exception message should mention unsupported type", ex.getMessage().contains("Unsupported aggregation type"));
        assertTrue("Exception message should include class name", ex.getMessage().contains("InternalMin"));
    }

    public void testConverterHandlesMetadata() throws IOException {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();
        registry.registerConverter(new MinAggregateProtoConverter());

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", 42);

        InternalMin internalMin = new InternalMin("min_with_meta", 15.5, DocValueFormat.RAW, metadata);

        Aggregate result = registry.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have metadata", result.getMin().hasMeta());
        ObjectMap metaMap = result.getMin().getMeta();
        assertTrue("Metadata should contain key1", metaMap.getFieldsMap().containsKey("key1"));
        assertTrue("Metadata should contain key2", metaMap.getFieldsMap().containsKey("key2"));

        assertTrue("Should have value set", result.getMin().hasValue());
        assertEquals("Value should match", 15.5, result.getMin().getValue().getDouble(), 0.001);
    }

    public void testConverterWithEmptyMetadataDoesNotSetMeta() throws IOException {
        AggregateProtoConverterSpiRegistry registry = new AggregateProtoConverterSpiRegistry();
        registry.registerConverter(new MinAggregateProtoConverter());

        InternalMin internalMin = new InternalMin("min_no_meta", 20.0, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = registry.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertFalse("Should not have metadata for empty map", result.getMin().hasMeta());
    }

    public void testPublicRegistryConvertsMinAggregation() throws IOException {
        AggregateProtoConverterRegistryImpl registry = new AggregateProtoConverterRegistryImpl();

        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = registry.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have min set", result.hasMin());
        assertEquals("Value should match", 10.5, result.getMin().getValue().getDouble(), 0.001);
    }

    public void testPublicRegistryConvertsMaxAggregation() throws IOException {
        AggregateProtoConverterRegistryImpl registry = new AggregateProtoConverterRegistryImpl();

        InternalMax internalMax = new InternalMax("max_price", 99.9, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = registry.toProto(internalMax);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have max set", result.hasMax());
        assertEquals("Value should match", 99.9, result.getMax().getValue().getDouble(), 0.001);
    }

    public void testNestedAggregation() throws IOException {
        AggregateProtoConverterRegistryImpl registry = new AggregateProtoConverterRegistryImpl();

        InternalMax subAgg = new InternalMax("max_price", 42.0, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations subAggs = new InternalAggregations(List.of(subAgg));

        StringTerms.Bucket bucket = new StringTerms.Bucket(new BytesRef("active"), 10, subAggs, false, 0, DocValueFormat.RAW);
        StringTerms terms = new StringTerms(
            "status_terms",
            BucketOrder.count(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            10,
            false,
            0,
            List.of(bucket),
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, -1)
        );

        Aggregate result = registry.toProto(terms);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have sterms set", result.hasSterms());
        assertEquals("Should have 1 bucket", 1, result.getSterms().getBucketsCount());

        var protoBucket = result.getSterms().getBuckets(0);
        assertEquals("active", protoBucket.getKey());
        assertEquals(10L, protoBucket.getDocCount());
        assertTrue("Bucket should have sub-aggregations", protoBucket.getAggregateCount() > 0);

        Aggregate subResult = protoBucket.getAggregateMap().get("max_price");
        assertNotNull("Sub-aggregation max_price should exist", subResult);
        assertTrue("Sub-aggregation should be max", subResult.hasMax());
        assertEquals("Sub-aggregation value should match", 42.0, subResult.getMax().getValue().getDouble(), 0.001);
    }
}
