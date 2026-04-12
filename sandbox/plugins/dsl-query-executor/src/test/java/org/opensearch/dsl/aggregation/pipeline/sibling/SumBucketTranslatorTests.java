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
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SumBucketTranslatorTests extends OpenSearchTestCase {

    private final SumBucketTranslator translator = new SumBucketTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetBuilderClass() {
        assertEquals(SumBucketPipelineAggregationBuilder.class, translator.getBuilderClass());
    }

    public void testTranslateProducesSumAggregate() throws ConversionException {
        RelNode input = buildBaseAggregate();
        var builder = new SumBucketPipelineAggregationBuilder("total", "total_revenue");

        RelNode result = translator.translate(builder, input, ctx);

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        assertEquals(1, agg.getAggCallList().size());
        assertEquals(SqlKind.SUM, agg.getAggCallList().get(0).getAggregation().getKind());
        assertEquals("total", agg.getAggCallList().get(0).getName());
    }

    public void testTranslateThrowsForUnknownPath() {
        RelNode input = buildBaseAggregate();
        var builder = new SumBucketPipelineAggregationBuilder("bad", "nonexistent");

        expectThrows(ConversionException.class, () -> translator.translate(builder, input, ctx));
    }

    public void testToInternalAggregationWithValidRow() {
        var builder = new SumBucketPipelineAggregationBuilder("total", "by_brand>revenue");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, new Object[]{670.0});

        assertEquals("total", result.getName());
        assertEquals(670.0, result.value(), 0.0001);
    }

    public void testToInternalAggregationWithNullRow() {
        var builder = new SumBucketPipelineAggregationBuilder("total", "by_brand>revenue");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, null);

        assertEquals(0.0, result.value(), 0.0001);
    }

    public void testToInternalAggregationWithNullValue() {
        var builder = new SumBucketPipelineAggregationBuilder("total", "by_brand>revenue");
        var result = (InternalSimpleValue) translator.toInternalAggregation(builder, new Object[]{null});

        assertEquals(0.0, result.value(), 0.0001);
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
