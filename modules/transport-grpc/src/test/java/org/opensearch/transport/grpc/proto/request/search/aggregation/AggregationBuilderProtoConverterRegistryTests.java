/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.protobufs.MinAggregation;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link AggregationBuilderProtoConverterRegistryImpl}.
 * Verifies converter registration, lookup, metadata handling, and basic aggregation conversion.
 */
public class AggregationBuilderProtoConverterRegistryTests extends OpenSearchTestCase {

    /**
     * Test that built-in converters (Min, Max) are registered by default.
     */
    public void testBuiltInConvertersRegistered() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        // Verify Min converter is registered
        AggregationContainer minContainer = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();
        AggregationBuilder minBuilder = registry.fromProto("min_price", minContainer);
        assertNotNull("Min aggregation should be converted", minBuilder);
        assertTrue("Should be MinAggregationBuilder", minBuilder instanceof MinAggregationBuilder);
        assertEquals("Aggregation name should match", "min_price", minBuilder.getName());

        // Verify Max converter is registered
        AggregationContainer maxContainer = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();
        AggregationBuilder maxBuilder = registry.fromProto("max_price", maxContainer);
        assertNotNull("Max aggregation should be converted", maxBuilder);
        assertTrue("Should be MaxAggregationBuilder", maxBuilder instanceof MaxAggregationBuilder);
        assertEquals("Aggregation name should match", "max_price", maxBuilder.getName());
    }

    /**
     * Test that external converters can be registered.
     * Note: Duplicate registrations are allowed and replace the existing converter with a warning.
     */
    public void testExternalConverterRegistration() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        // Create a mock converter that replaces the built-in Min converter
        AggregationBuilderProtoConverter mockConverter = new AggregationBuilderProtoConverter() {
            @Override
            public AggregationContainer.AggregationContainerCase getHandledAggregationCase() {
                return AggregationContainer.AggregationContainerCase.MIN;
            }

            @Override
            public AggregationBuilder fromProto(String name, AggregationContainer container) {
                return new MinAggregationBuilder("external_" + name);
            }
        };

        // Registering duplicate converter should succeed (replaces with warning)
        registry.registerConverter(mockConverter);
        registry.updateRegistryOnAllConverters();

        // Verify that the new converter is used
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();
        AggregationBuilder builder = registry.fromProto("test", container);
        assertEquals("Should use external converter name", "external_test", builder.getName());
    }

    /**
     * Test that unsupported aggregation types throw appropriate exception.
     *
     * Note: Temporarily disabled as TermsAggregation is not yet implemented in the proto.
     * TODO: Re-enable when TermsAggregation support is added.
     */
    public void testUnsupportedAggregationType() {
        // Skip this test until TermsAggregation is added to the proto
        // AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();
        //
        // // Create a container with Terms aggregation (not implemented yet)
        // AggregationContainer termsContainer = AggregationContainer.newBuilder()
        //     .setTerms(
        //         org.opensearch.protobufs.TermsAggregationFields.newBuilder()
        //             .setField("category")
        //             .build()
        //     )
        //     .build();
        //
        // IllegalArgumentException exception = expectThrows(
        //     IllegalArgumentException.class,
        //     () -> registry.fromProto("category_terms", termsContainer)
        // );
        // assertTrue(
        //     "Exception should mention unsupported aggregation type",
        //     exception.getMessage().contains("Unsupported aggregation type")
        // );
    }

    /**
     * Test that metadata is applied at the container level.
     */
    public void testMetadataHandling() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        // Create metadata
        Map<String, Object> expectedMetadata = new HashMap<>();
        expectedMetadata.put("key1", "value1");
        expectedMetadata.put("key2", 42L);

        ObjectMap.Builder metaBuilder = ObjectMap.newBuilder();
        metaBuilder.putFields("key1", ObjectMap.Value.newBuilder().setString("value1").build());
        metaBuilder.putFields("key2", ObjectMap.Value.newBuilder().setInt64(42L).build());

        // Create aggregation with metadata
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .setMeta(metaBuilder.build())
            .build();

        AggregationBuilder builder = registry.fromProto("min_price", container);
        assertNotNull("Aggregation should have metadata", builder.getMetadata());
        assertEquals("Metadata should match", expectedMetadata, builder.getMetadata());
    }

    /**
     * Test that null aggregation name throws exception.
     */
    public void testNullAggregationName() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> registry.fromProto(null, container)
        );
        assertTrue(
            "Exception should mention null or empty name",
            exception.getMessage().contains("cannot be null or empty")
        );
    }

    /**
     * Test that empty aggregation name throws exception.
     */
    public void testEmptyAggregationName() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> registry.fromProto("", container)
        );
        assertTrue(
            "Exception should mention null or empty name",
            exception.getMessage().contains("cannot be null or empty")
        );
    }

    /**
     * Test that null aggregation container throws exception.
     */
    public void testNullAggregationContainer() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> registry.fromProto("test_agg", null)
        );
        assertTrue(
            "Exception should mention null container",
            exception.getMessage().contains("cannot be null")
        );
    }

    /**
     * Test that aggregation container with no type set throws exception.
     */
    public void testAggregationContainerNotSet() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        AggregationContainer emptyContainer = AggregationContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> registry.fromProto("test_agg", emptyContainer)
        );
        assertTrue(
            "Exception should mention unsupported aggregation type",
            exception.getMessage().contains("Unsupported aggregation type")
        );
    }

    /**
     * Test that Min aggregation conversion works correctly.
     */
    public void testMinAggregationConversion() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        AggregationBuilder builder = registry.fromProto("min_price", container);
        assertNotNull("Aggregation builder should not be null", builder);
        assertTrue("Should be MinAggregationBuilder", builder instanceof MinAggregationBuilder);
        assertEquals("Name should match", "min_price", builder.getName());

        MinAggregationBuilder minBuilder = (MinAggregationBuilder) builder;
        assertEquals("Field should match", "price", minBuilder.field());
    }

    /**
     * Test that Max aggregation conversion works correctly.
     */
    public void testMaxAggregationConversion() {
        AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();

        AggregationBuilder builder = registry.fromProto("max_price", container);
        assertNotNull("Aggregation builder should not be null", builder);
        assertTrue("Should be MaxAggregationBuilder", builder instanceof MaxAggregationBuilder);
        assertEquals("Name should match", "max_price", builder.getName());

        MaxAggregationBuilder maxBuilder = (MaxAggregationBuilder) builder;
        assertEquals("Field should match", "price", maxBuilder.field());
    }

    /**
     * Test that nested aggregations infrastructure is in place.
     * Note: Min/Max are metric aggregations and explicitly reject subaggregations.
     * This test verifies that the registry TRIES to add them (proving the infrastructure works),
     * even though Min will throw an exception rejecting them.
     * Full nested aggregation support will be tested when bucket aggregations are implemented.
     *
     * Temporarily disabled as nested aggregations are not yet supported in the proto.
     * TODO: Re-enable when nested aggregations support is added.
     */
    public void testNestedAggregationsInfrastructure() {
        // Skip this test until nested aggregations are added to the proto
        // AggregationBuilderProtoConverterRegistryImpl registry = new AggregationBuilderProtoConverterRegistryImpl();
        //
        // // Create a nested aggregation structure
        // AggregationContainer subAgg = AggregationContainer.newBuilder()
        //     .setMax(MaxAggregation.newBuilder().setField("price").build())
        //     .build();
        //
        // Map<String, AggregationContainer> subAggMap = new HashMap<>();
        // subAggMap.put("max_price", subAgg);
        //
        // AggregationContainer container = AggregationContainer.newBuilder()
        //     .setMin(MinAggregation.newBuilder().setField("quantity").build())
        //     .putAllAggregations(subAggMap)
        //     .build();
        //
        // // Min explicitly rejects subaggregations, so this should throw
        // // This proves that the registry infrastructure is correctly attempting to add them
        // org.opensearch.search.aggregations.AggregationInitializationException exception = expectThrows(
        //     org.opensearch.search.aggregations.AggregationInitializationException.class,
        //     () -> registry.fromProto("min_quantity", container)
        // );
        // assertTrue(
        //     "Exception should mention that min cannot accept sub-aggregations",
        //     exception.getMessage().contains("cannot accept sub-aggregations")
        // );
    }
}
