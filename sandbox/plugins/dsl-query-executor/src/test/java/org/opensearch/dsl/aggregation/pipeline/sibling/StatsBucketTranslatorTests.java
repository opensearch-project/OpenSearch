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
import org.opensearch.search.aggregations.pipeline.InternalStatsBucket;
import org.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class StatsBucketTranslatorTests extends OpenSearchTestCase {

    private final StatsBucketTranslator translator = new StatsBucketTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetBuilderClass() {
        assertEquals(StatsBucketPipelineAggregationBuilder.class, translator.getBuilderClass());
    }

    public void testTranslateProducesMultipleAggregateCalls() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new StatsBucketPipelineAggregationBuilder("rev_stats", "total_revenue");

        RelNode result = translator.translate(builder, input, ctx);

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        // Should have COUNT, MIN, MAX, SUM = 4 calls
        assertEquals(4, agg.getAggCallList().size());
        assertEquals(SqlKind.COUNT, agg.getAggCallList().get(0).getAggregation().getKind());
        assertEquals(SqlKind.MIN, agg.getAggCallList().get(1).getAggregation().getKind());
        assertEquals(SqlKind.MAX, agg.getAggCallList().get(2).getAggregation().getKind());
        assertEquals(SqlKind.SUM, agg.getAggCallList().get(3).getAggregation().getKind());
        assertTrue(agg.getGroupSet().isEmpty());
    }

    public void testTranslateOutputFieldNames() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new StatsBucketPipelineAggregationBuilder("rev_stats", "total_revenue");

        RelNode result = translator.translate(builder, input, ctx);

        LogicalAggregate agg = (LogicalAggregate) result;
        assertEquals("rev_stats_count", agg.getAggCallList().get(0).getName());
        assertEquals("rev_stats_min", agg.getAggCallList().get(1).getName());
        assertEquals("rev_stats_max", agg.getAggCallList().get(2).getName());
        assertEquals("rev_stats_sum", agg.getAggCallList().get(3).getName());
    }

    public void testTranslateThrowsForUnknownPath() {
        RelNode input = buildBaseAggregate();
        var builder = new StatsBucketPipelineAggregationBuilder("bad", "nonexistent");

        expectThrows(ConversionException.class, () -> translator.translate(builder, input, ctx));
    }

    public void testToInternalAggregationWithValidRow() {
        var builder = new StatsBucketPipelineAggregationBuilder("rev_stats", "by_brand>revenue");
        var result = (InternalStatsBucket) translator.toInternalAggregation(builder,
            new Object[]{3L, 60.0, 380.0, 670.0});

        assertEquals(3, result.getCount());
        assertEquals(60.0, result.getMin(), 0.0001);
        assertEquals(380.0, result.getMax(), 0.0001);
        assertEquals(670.0, result.getSum(), 0.0001);
        assertEquals(223.3333, result.getAvg(), 0.01);
    }

    public void testToInternalAggregationWithNullRow() {
        var builder = new StatsBucketPipelineAggregationBuilder("rev_stats", "by_brand>revenue");
        var result = (InternalStatsBucket) translator.toInternalAggregation(builder, null);

        assertEquals(0, result.getCount());
        assertEquals(0.0, result.getSum(), 0.0001);
    }

    public void testToInternalAggregationWithNullValues() {
        var builder = new StatsBucketPipelineAggregationBuilder("rev_stats", "by_brand>revenue");
        var result = (InternalStatsBucket) translator.toInternalAggregation(builder,
            new Object[]{null, null, null, null});

        assertEquals(0, result.getCount());
        assertEquals(0.0, result.getSum(), 0.0001);
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
