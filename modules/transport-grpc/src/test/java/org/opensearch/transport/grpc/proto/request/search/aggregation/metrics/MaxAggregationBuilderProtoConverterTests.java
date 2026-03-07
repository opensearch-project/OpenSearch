/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.metrics;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ValueType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link MaxAggregationBuilderProtoConverter}.
 * Verifies that the converter properly handles the MAX case and delegates to MaxAggregationProtoUtils.
 */
public class MaxAggregationBuilderProtoConverterTests extends OpenSearchTestCase {

    private MaxAggregationBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MaxAggregationBuilderProtoConverter();
    }

    /**
     * Test that the converter reports the correct aggregation case.
     */
    public void testGetHandledAggregationCase() {
        assertEquals(
            "Should handle MAX case",
            AggregationContainer.AggregationContainerCase.MAX,
            converter.getHandledAggregationCase()
        );
    }

    /**
     * Test basic Max aggregation conversion with field only.
     */
    public void testBasicMaxAggregation() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMax(MaxAggregation.newBuilder().setField("price").build())
            .build();

        AggregationBuilder builder = converter.fromProto("max_price", container);
        assertNotNull("Builder should not be null", builder);
        assertTrue("Should be MaxAggregationBuilder", builder instanceof MaxAggregationBuilder);
        assertEquals("Name should match", "max_price", builder.getName());

        MaxAggregationBuilder maxBuilder = (MaxAggregationBuilder) builder;
        assertEquals("Field should match", "price", maxBuilder.field());
    }

    /**
     * Test Max aggregation with format.
     */
    public void testMaxAggregationWithFormat() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMax(
                MaxAggregation.newBuilder()
                    .setField("price")
                    .setFormat("###.00")
                    .build()
            )
            .build();

        AggregationBuilder builder = converter.fromProto("max_price", container);
        assertNotNull("Builder should not be null", builder);
        assertTrue("Should be MaxAggregationBuilder", builder instanceof MaxAggregationBuilder);

        MaxAggregationBuilder maxBuilder = (MaxAggregationBuilder) builder;
        assertEquals("Format should match", "###.00", maxBuilder.format());
    }

    /**
     * Test Max aggregation with script.
     */
    public void testMaxAggregationWithScript() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMax(
                MaxAggregation.newBuilder()
                    .setScript(
                        Script.newBuilder()
                            .setInline(
                                InlineScript.newBuilder()
                                    .setSource("doc['price'].value * 2")
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        AggregationBuilder builder = converter.fromProto("max_price", container);
        assertNotNull("Builder should not be null", builder);
        assertTrue("Should be MaxAggregationBuilder", builder instanceof MaxAggregationBuilder);

        MaxAggregationBuilder maxBuilder = (MaxAggregationBuilder) builder;
        assertNotNull("Script should not be null", maxBuilder.script());
        assertEquals("Script source should match", "doc['price'].value * 2", maxBuilder.script().getIdOrCode());
    }

    /**
     * Test that container without Max aggregation throws exception.
     */
    public void testContainerWithoutMax() {
        AggregationContainer container = AggregationContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> converter.fromProto("test", container)
        );
        assertTrue(
            "Exception should mention missing Max aggregation",
            exception.getMessage().contains("Container does not contain Max aggregation")
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
