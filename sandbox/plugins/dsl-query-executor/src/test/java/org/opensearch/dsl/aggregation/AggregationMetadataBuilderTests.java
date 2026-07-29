/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AggregationMetadataBuilderTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testResolvesFieldGroupingToCorrectIndex() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        // brand is the 3rd field (index 2) in TestUtils schema: name, price, brand, rating
        builder.addGrouping(new FieldGrouping(List.of("brand")));
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(metadata.getGroupByBitSet().get(2));
        assertEquals(1, metadata.getGroupByBitSet().cardinality());
    }

    public void testResolvesMultipleFieldGroupings() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping(List.of("brand")));
        builder.addGrouping(new FieldGrouping(List.of("name")));
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(metadata.getGroupByBitSet().get(0)); // name is index 0
        assertTrue(metadata.getGroupByBitSet().get(2)); // brand is index 2
        assertEquals(2, metadata.getGroupByBitSet().cardinality());
    }

    public void testThrowsForUnknownField() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping(List.of("nonexistent")));
        builder.requestImplicitCount();

        expectThrows(ConversionException.class, () -> builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory()));
    }

    public void testLiteralColumnAllocatorAssignsSequentialPositionsAfterBase() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        LiteralColumnAllocator allocator = builder.literalColumnAllocator(4);

        assertEquals(4, allocator.columnFor(50.0));
        assertEquals(5, allocator.columnFor(95.0));
        assertEquals(6, allocator.integerColumnFor(200));
        assertEquals(7, allocator.coalescedColumnFor(1, 0.0));
    }

    public void testLiteralColumnAllocatorDeduplicatesEqualColumns() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        LiteralColumnAllocator allocator = builder.literalColumnAllocator(4);

        assertEquals(4, allocator.columnFor(50.0));
        assertEquals(4, allocator.columnFor(50.0));
        assertEquals(5, allocator.coalescedColumnFor(1, 0.0));
        assertEquals(5, allocator.coalescedColumnFor(1, 0.0));
        // Same value, different kind or different field: distinct columns.
        assertEquals(6, allocator.integerColumnFor(50));
        assertEquals(7, allocator.coalescedColumnFor(2, 0.0));
    }

    public void testLiteralColumnsTravelIntoMetadataInAllocationOrder() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        LiteralColumnAllocator allocator = builder.literalColumnAllocator(4);
        allocator.coalescedColumnFor(1, 0.0);
        allocator.columnFor(99.0);
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(List.of(LiteralColumn.coalesced(1, 0.0), LiteralColumn.constant(99.0)), metadata.getLiteralColumns());
    }
}
