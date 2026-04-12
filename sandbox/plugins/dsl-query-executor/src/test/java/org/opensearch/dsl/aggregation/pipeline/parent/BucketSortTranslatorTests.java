/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline.parent;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.pipeline.PipelineTranslator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class BucketSortTranslatorTests extends OpenSearchTestCase {

    private final BucketSortTranslator translator = new BucketSortTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetBuilderClass() {
        assertEquals(BucketSortPipelineAggregationBuilder.class, translator.getBuilderClass());
    }

    public void testToInternalAggregationReturnsNull() {
        var builder = new BucketSortPipelineAggregationBuilder("sort", List.of());
        builder.size(5);
        assertNull(translator.toInternalAggregation(builder, new Object[]{}));
    }

    public void testTranslateProducesLogicalSort() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("total_revenue").order(SortOrder.DESC)));

        RelNode result = translator.translate(builder, input, ctx);

        assertTrue(result instanceof LogicalSort);
    }

    public void testTranslateSortDescending() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("total_revenue").order(SortOrder.DESC)));

        LogicalSort sort = (LogicalSort) translator.translate(builder, input, ctx);

        assertFalse(sort.getCollation().getFieldCollations().isEmpty());
        assertEquals(
            RelFieldCollation.Direction.DESCENDING,
            sort.getCollation().getFieldCollations().get(0).getDirection()
        );
    }

    public void testTranslateWithSize() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("total_revenue").order(SortOrder.DESC)));
        builder.size(3);

        LogicalSort sort = (LogicalSort) translator.translate(builder, input, ctx);

        assertNotNull(sort.fetch);
    }

    public void testTranslateWithFrom() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("total_revenue").order(SortOrder.DESC)));
        builder.from(2);

        LogicalSort sort = (LogicalSort) translator.translate(builder, input, ctx);

        assertNotNull(sort.offset);
    }

    public void testTranslateMultipleSortFields() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort", List.of(
            new FieldSortBuilder("total_revenue").order(SortOrder.DESC),
            new FieldSortBuilder("_count").order(SortOrder.ASC)
        ));

        LogicalSort sort = (LogicalSort) translator.translate(builder, input, ctx);

        assertEquals(2, sort.getCollation().getFieldCollations().size());
    }

    public void testTranslateThrowsForUnknownSortField() {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("nonexistent").order(SortOrder.DESC)));

        expectThrows(ConversionException.class, () -> translator.translate(builder, input, ctx));
    }

    public void testTranslateWithSkipGapPolicy() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("total_revenue").order(SortOrder.DESC)));
        // Default gap policy is SKIP — should add IS NOT NULL filter before sort

        LogicalSort sort = (LogicalSort) translator.translate(builder, input, ctx);

        // Input to sort should be a LogicalFilter (SKIP filters nulls)
        assertTrue(sort.getInput() instanceof LogicalFilter);
    }

    public void testTranslateWithInsertZerosGapPolicy() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("total_revenue").order(SortOrder.DESC)));
        builder.gapPolicy(org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy.INSERT_ZEROS);

        LogicalSort sort = (LogicalSort) translator.translate(builder, input, ctx);

        // Input to sort should be a LogicalProject (INSERT_ZEROS wraps with COALESCE)
        assertTrue(sort.getInput() instanceof LogicalProject);
    }

    public void testTranslatePreservesInputRowType() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new BucketSortPipelineAggregationBuilder("sort",
            List.of(new FieldSortBuilder("total_revenue").order(SortOrder.DESC)));
        builder.size(3);

        RelNode result = translator.translate(builder, input, ctx);

        assertEquals(input.getRowType(), result.getRowType());
    }

    private RelNode buildBaseAggregate() {
        LogicalTableScan scan = TestUtils.createTestRelNode();
        RelDataType bigintType = ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RelDataType intType = scan.getRowType().getFieldList().get(1).getType();

        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false,
            ImmutableList.of(), List.of(1), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, intType, "total_revenue"
        );
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, false, false,
            ImmutableList.of(), List.of(), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, bigintType, "_count"
        );

        return LogicalAggregate.create(scan, ImmutableList.of(), ImmutableBitSet.of(2), null, List.of(sumCall, countCall));
    }
}
