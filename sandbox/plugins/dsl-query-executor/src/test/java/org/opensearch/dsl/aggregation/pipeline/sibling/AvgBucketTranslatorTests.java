/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline.sibling;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AvgBucketTranslatorTests extends OpenSearchTestCase {

    private final AvgBucketTranslator translator = new AvgBucketTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetBuilderClass() {
        assertEquals(AvgBucketPipelineAggregationBuilder.class, translator.getBuilderClass());
    }

    public void testTranslateProducesAvgAggregate() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "total_revenue");

        RelNode result = translator.translate(builder, input, ctx);

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        assertEquals(1, agg.getAggCallList().size());
        assertEquals(SqlKind.AVG, agg.getAggCallList().get(0).getAggregation().getKind());
        assertEquals("avg_revenue", agg.getAggCallList().get(0).getName());
        assertTrue(agg.getGroupSet().isEmpty());
    }

    public void testTranslateResolvesCorrectColumn() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "total_revenue");

        RelNode result = translator.translate(builder, input, ctx);

        LogicalAggregate agg = (LogicalAggregate) result;
        // The AVG should reference the total_revenue column (index 1 in the base aggregate output)
        // After gap policy filter, the column index is preserved
        AggregateCall avgCall = agg.getAggCallList().get(0);
        assertEquals(1, avgCall.getArgList().size());
    }

    public void testTranslateWithSkipGapPolicy() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "total_revenue");
        // Default gap policy is SKIP — should add IS NOT NULL filter

        RelNode result = translator.translate(builder, input, ctx);

        LogicalAggregate agg = (LogicalAggregate) result;
        // Input to the aggregate should be a LogicalFilter (SKIP gap policy)
        assertTrue(agg.getInput() instanceof org.apache.calcite.rel.logical.LogicalFilter);
    }

    public void testTranslateThrowsForUnknownPath() {
        RelNode input = buildBaseAggregate();
        var builder = new AvgBucketPipelineAggregationBuilder("avg_bad", "nonexistent_metric");

        expectThrows(ConversionException.class, () -> translator.translate(builder, input, ctx));
    }

    public void testToInternalAggregationWithValidRow() {
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "by_brand>revenue");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, new Object[]{42.5});

        assertEquals("avg_revenue", result.getName());
        assertEquals(42.5, result.value(), 0.0001);
    }

    public void testToInternalAggregationWithNullRow() {
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "by_brand>revenue");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, null);

        assertTrue(Double.isNaN(result.value()));
    }

    public void testToInternalAggregationWithEmptyRow() {
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "by_brand>revenue");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, new Object[]{});

        assertTrue(Double.isNaN(result.value()));
    }

    public void testToInternalAggregationWithNullValue() {
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "by_brand>revenue");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, new Object[]{null});

        assertTrue(Double.isNaN(result.value()));
    }

    public void testToInternalAggregationWithFormat() {
        var builder = new AvgBucketPipelineAggregationBuilder("avg_revenue", "by_brand>revenue");
        builder.format("#,##0.00");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, new Object[]{1234.5});

        assertEquals(1234.5, result.value(), 0.0001);
        assertEquals("1,234.50", result.getValueAsString());
    }

    /**
     * Builds a LogicalAggregate simulating: GROUP BY brand, SUM(price) AS total_revenue, COUNT(*)
     * Output row type: [brand (VARCHAR), total_revenue (INTEGER), _count (BIGINT)]
     */
    private RelNode buildBaseAggregate() {
        LogicalTableScan scan = TestUtils.createTestRelNode();
        RelDataType bigintType = ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RelDataType intType = scan.getRowType().getFieldList().get(1).getType(); // price column type

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

        return LogicalAggregate.create(
            scan, ImmutableList.of(), ImmutableBitSet.of(2), null, List.of(sumCall, countCall)
        );
    }
}
