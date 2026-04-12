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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.pipeline.InternalPercentilesBucket;
import org.opensearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class PercentilesBucketTranslatorTests extends OpenSearchTestCase {

    private final PercentilesBucketTranslator translator = new PercentilesBucketTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetBuilderClass() {
        assertEquals(PercentilesBucketPipelineAggregationBuilder.class, translator.getBuilderClass());
    }

    public void testTranslateProducesPercentileContCalls() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new PercentilesBucketPipelineAggregationBuilder("pcts", "total_revenue");
        builder.setPercents(new double[]{25.0, 50.0, 75.0});

        RelNode result = translator.translate(builder, input, ctx);

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        // One PERCENTILE_CONT call per percent
        assertEquals(3, agg.getAggCallList().size());
        for (AggregateCall call : agg.getAggCallList()) {
            assertSame(SqlStdOperatorTable.PERCENTILE_CONT, call.getAggregation());
        }
    }

    public void testTranslateHasPreProjectForFractionLiterals() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new PercentilesBucketPipelineAggregationBuilder("pcts", "total_revenue");
        builder.setPercents(new double[]{50.0});

        RelNode result = translator.translate(builder, input, ctx);

        LogicalAggregate agg = (LogicalAggregate) result;
        // Input should be a LogicalProject with fraction literal columns
        assertTrue(agg.getInput() instanceof LogicalProject);
        LogicalProject project = (LogicalProject) agg.getInput();
        // Should have original columns + 1 fraction column
        assertTrue(project.getRowType().getFieldCount() > input.getRowType().getFieldCount());
    }

    public void testTranslateWithDefaultPercents() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new PercentilesBucketPipelineAggregationBuilder("pcts", "total_revenue");
        // Don't set percents — uses defaults: 1, 5, 25, 50, 75, 95, 99

        RelNode result = translator.translate(builder, input, ctx);

        LogicalAggregate agg = (LogicalAggregate) result;
        assertEquals(7, agg.getAggCallList().size());
    }

    public void testTranslateThrowsForUnknownPath() {
        RelNode input = buildBaseAggregate();
        var builder = new PercentilesBucketPipelineAggregationBuilder("bad", "nonexistent");

        expectThrows(ConversionException.class, () -> translator.translate(builder, input, ctx));
    }

    // --- toInternalAggregation tests ---

    public void testToInternalAggregationWithValidRow() {
        var builder = new PercentilesBucketPipelineAggregationBuilder("pcts", "by_brand>revenue");
        builder.setPercents(new double[]{25.0, 50.0, 75.0});
        var result = (InternalPercentilesBucket) translator.toInternalAggregation(builder,
            new Object[]{60.0, 230.0, 380.0});

        assertEquals(60.0, result.percentile(25.0), 0.0001);
        assertEquals(230.0, result.percentile(50.0), 0.0001);
        assertEquals(380.0, result.percentile(75.0), 0.0001);
    }

    public void testToInternalAggregationWithNullRow() {
        var builder = new PercentilesBucketPipelineAggregationBuilder("pcts", "by_brand>revenue");
        builder.setPercents(new double[]{50.0});
        var result = (InternalPercentilesBucket) translator.toInternalAggregation(builder, null);

        assertTrue(Double.isNaN(result.percentile(50.0)));
    }

    public void testToInternalAggregationWithShortRow() {
        var builder = new PercentilesBucketPipelineAggregationBuilder("pcts", "by_brand>revenue");
        builder.setPercents(new double[]{25.0, 50.0, 75.0});
        var result = (InternalPercentilesBucket) translator.toInternalAggregation(builder,
            new Object[]{60.0, 230.0});

        assertTrue(Double.isNaN(result.percentile(25.0)));
    }

    public void testToInternalAggregationWithDefaultPercents() {
        var builder = new PercentilesBucketPipelineAggregationBuilder("pcts", "by_brand>revenue");
        var result = (InternalPercentilesBucket) translator.toInternalAggregation(builder,
            new Object[]{10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0});

        assertEquals(10.0, result.percentile(1.0), 0.0001);
        assertEquals(40.0, result.percentile(50.0), 0.0001);
        assertEquals(70.0, result.percentile(99.0), 0.0001);
    }

    private RelNode buildBaseAggregate() {
        LogicalTableScan scan = TestUtils.createTestRelNode();
        var typeFactory = ctx.getCluster().getTypeFactory();
        RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        // SUM over a nullable column infers a nullable return type
        RelDataType nullableDoubleType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.DOUBLE), true);

        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false,
            ImmutableList.of(), List.of(3), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, nullableDoubleType, "total_revenue"
        );
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, false, false,
            ImmutableList.of(), List.of(), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, bigintType, "_count"
        );

        // Group by brand (index 2), metric on rating (index 3, DOUBLE type)
        return LogicalAggregate.create(scan, ImmutableList.of(), ImmutableBitSet.of(2), null, List.of(sumCall, countCall));
    }
}
