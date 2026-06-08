/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.metrics;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.MinAggregation;
import org.opensearch.protobufs.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link MinAggregationBuilderProtoConverter}.
 * Verifies that the converter properly handles the MIN case and delegates to MinAggregationProtoUtils.
 */
public class MinAggregationBuilderProtoConverterTests extends OpenSearchTestCase {

    private MinAggregationBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MinAggregationBuilderProtoConverter();
    }

    /**
     * Test that the converter reports the correct aggregation case.
     */
    public void testGetHandledAggregationCase() {
        assertEquals("Should handle MIN case", AggregationContainer.AggregationContainerCase.MIN, converter.getHandledAggregationCase());
    }

    /**
     * Test basic Min aggregation conversion with field only.
     */
    public void testBasicMinAggregation() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        AggregationBuilder builder = converter.fromProto("min_price", container);
        assertNotNull("Builder should not be null", builder);
        assertTrue("Should be MinAggregationBuilder", builder instanceof MinAggregationBuilder);
        assertEquals("Name should match", "min_price", builder.getName());

        MinAggregationBuilder minBuilder = (MinAggregationBuilder) builder;
        assertEquals("Field should match", "price", minBuilder.field());
    }

    /**
     * Test Min aggregation with format.
     */
    public void testMinAggregationWithFormat() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").setFormat("###.00").build())
            .build();

        AggregationBuilder builder = converter.fromProto("min_price", container);
        assertNotNull("Builder should not be null", builder);
        assertTrue("Should be MinAggregationBuilder", builder instanceof MinAggregationBuilder);

        MinAggregationBuilder minBuilder = (MinAggregationBuilder) builder;
        assertEquals("Format should match", "###.00", minBuilder.format());
    }

    /**
     * Test Min aggregation with script.
     */
    public void testMinAggregationWithScript() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(
                MinAggregation.newBuilder()
                    .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['price'].value * 2").build()).build())
                    .build()
            )
            .build();

        AggregationBuilder builder = converter.fromProto("min_price", container);
        assertNotNull("Builder should not be null", builder);
        assertTrue("Should be MinAggregationBuilder", builder instanceof MinAggregationBuilder);

        MinAggregationBuilder minBuilder = (MinAggregationBuilder) builder;
        assertNotNull("Script should not be null", minBuilder.script());
        assertEquals("Script source should match", "doc['price'].value * 2", minBuilder.script().getIdOrCode());
    }

    /**
     * Test that container without Min aggregation throws exception.
     */
    public void testContainerWithoutMin() {
        AggregationContainer container = AggregationContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto("test", container));
        assertTrue(
            "Exception should mention missing Min aggregation",
            exception.getMessage().contains("Container does not contain Min aggregation")
        );
    }

    /**
     * Test that converter does not implement setRegistry (metric aggregation).
     */
    public void testSetRegistryNotImplemented() {
        // Should not throw - default no-op implementation
        converter.setRegistry(null);
    }
}
