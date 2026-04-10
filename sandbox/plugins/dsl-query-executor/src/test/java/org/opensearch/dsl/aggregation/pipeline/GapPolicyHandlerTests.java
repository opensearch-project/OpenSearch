/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.search.aggregations.pipeline.BucketHelpers;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class GapPolicyHandlerTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    // --- SKIP policy tests ---

    public void testSkipFilterIsNotNull() {
        RelNode input = buildBaseAggregate();
        RelNode result = GapPolicyHandler.apply(BucketHelpers.GapPolicy.SKIP, input, 1, ctx);
        assertTrue(result instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) result;

        RexNode condition = filter.getCondition();
        assertTrue(condition instanceof RexCall);
        assertEquals(SqlKind.IS_NOT_NULL, condition.getKind());
    }

    public void testSkipPreservesRowType() {
        RelNode input = buildBaseAggregate();
        RelNode result = GapPolicyHandler.apply(BucketHelpers.GapPolicy.SKIP, input, 1, ctx);

        assertEquals(input.getRowType(), result.getRowType());
    }

    // --- INSERT_ZEROS policy tests ---

    public void testInsertZerosProducesLogicalProject() {
        RelNode input = buildBaseAggregate();
        RelNode result = GapPolicyHandler.apply(BucketHelpers.GapPolicy.INSERT_ZEROS, input, 1, ctx);

        assertTrue(result instanceof LogicalProject);
    }

    public void testInsertZerosPreservesRow() {
        RelNode input = buildBaseAggregate();
        RelNode result = GapPolicyHandler.apply(BucketHelpers.GapPolicy.INSERT_ZEROS, input, 1, ctx);

        assertEquals(input.getRowType().getFieldCount(), result.getRowType().getFieldCount());
        assertEquals(input.getRowType().getFieldNames(), result.getRowType().getFieldNames());
    }

    public void testInsertZerosWrapsTargetColumnWithCoalesce() {
        RelNode input = buildBaseAggregate();
        LogicalProject project = (LogicalProject) GapPolicyHandler.apply(
            BucketHelpers.GapPolicy.INSERT_ZEROS, input, 1, ctx);

        // Column at index 1 (total_revenue) should be wrapped with COALESCE
        RexNode targetExpr = project.getProjects().get(1);
        assertTrue(targetExpr instanceof RexCall);
        assertEquals(SqlKind.COALESCE, targetExpr.getKind());

        // Column at index 0 (brand) should be a simple input ref, not wrapped
        RexNode otherExpr = project.getProjects().get(0);
        assertFalse(otherExpr instanceof RexCall);
    }

    // --- helper ---

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
