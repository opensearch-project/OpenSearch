/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OpenSearchMultiValueGroupByRewriter} — the planner-level replacement for
 * the old backend-local {@code MultiValueRelRewriter}/{@code MultiValueExpandRel} pair. Runs
 * BEFORE {@code PlannerImpl.decomposeAggregates}, so these tests build a raw
 * {@link LogicalAggregate} directly (no marking) and assert on the pre-marking
 * {@link OpenSearchMultiValueExpand} it inserts. Full-pipeline coverage (marking, CBO, multi-shard
 * PARTIAL/FINAL) lives in {@code MultiValueGroupByPlanShapeTests}.
 */
public class OpenSearchMultiValueGroupByRewriterTests extends BasePlannerRulesTests {

    public void testListGroupByExpandsAndPreservesSourceList() {
        TableScan scan = stubScan(listTable("test_index"));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(count));

        RelNode rewritten = OpenSearchMultiValueGroupByRewriter.rewrite(aggregate);
        assertTrue(rewritten instanceof LogicalProject);
        LogicalProject output = (LogicalProject) rewritten;
        assertEquals(List.of("tags", "count"), output.getRowType().getFieldNames());
        assertTrue(output.getInput() instanceof org.apache.calcite.rel.core.Aggregate);

        org.apache.calcite.rel.core.Aggregate grouped = (org.apache.calcite.rel.core.Aggregate) output.getInput();
        // The expanded key is re-projected to the FRONT so the groupSet is a prefix — required for
        // OpenSearchAggregateSplitRule to allow the PARTIAL/FINAL split (see the rewriter).
        assertEquals("GROUP BY must be a prefix over the reordered input", ImmutableBitSet.of(0), grouped.getGroupSet());
        assertTrue(grouped.getInput() instanceof LogicalProject);
        LogicalProject keysFirst = (LogicalProject) grouped.getInput();
        assertEquals(
            "reordered input puts the scalar element first",
            List.of("___mvexpand_0", "tags"),
            keysFirst.getRowType().getFieldNames()
        );
        assertNull("group key must be the scalar element", keysFirst.getRowType().getFieldList().get(0).getType().getComponentType());
        assertNotNull("source LIST column must be preserved", keysFirst.getRowType().getFieldList().get(1).getType().getComponentType());

        assertTrue(keysFirst.getInput() instanceof OpenSearchMultiValueExpand);
        OpenSearchMultiValueExpand expand = (OpenSearchMultiValueExpand) keysFirst.getInput();
        assertEquals(0, expand.getFieldIndex());
        assertEquals(1, expand.getExpandedFieldIndex());
        assertTrue("pre-marking: no viable backends yet", expand.getViableBackends().isEmpty());
        // Expand output: source LIST at 0 (unchanged), appended scalar element at 1.
        assertNotNull(expand.getRowType().getFieldList().get(0).getType().getComponentType());
        assertNull(expand.getRowType().getFieldList().get(1).getType().getComponentType());
    }

    public void testListGroupByRemapsGroupingSetsAndRestoresOutputOrder() {
        TableScan scan = stubScan(listPlusScalarTable("test_index"));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0, 1),
            ImmutableBitSet.ORDERING.immutableSortedCopy(List.of(ImmutableBitSet.of(0), ImmutableBitSet.of(1), ImmutableBitSet.of(0, 1))),
            List.of(count)
        );

        LogicalProject output = (LogicalProject) OpenSearchMultiValueGroupByRewriter.rewrite(aggregate);
        org.apache.calcite.rel.core.Aggregate grouped = (org.apache.calcite.rel.core.Aggregate) output.getInput();
        // Keys re-projected to the front in original order: [expanded tags, category, <source tags>].
        assertEquals(ImmutableBitSet.of(0, 1), grouped.getGroupSet());
        assertEquals(
            ImmutableBitSet.ORDERING.immutableSortedCopy(List.of(ImmutableBitSet.of(0), ImmutableBitSet.of(1), ImmutableBitSet.of(0, 1))),
            grouped.getGroupSets()
        );
        LogicalProject keysFirst = (LogicalProject) grouped.getInput();
        assertEquals(List.of("___mvexpand_0", "category", "tags"), keysFirst.getRowType().getFieldNames());
        assertEquals(List.of("tags", "category", "count"), output.getRowType().getFieldNames());
        // Output ordering already matches — the restoring Project is an identity rename.
        assertEquals(0, ((RexInputRef) output.getProjects().get(0)).getIndex());
        assertEquals(1, ((RexInputRef) output.getProjects().get(1)).getIndex());
        assertEquals(2, ((RexInputRef) output.getProjects().get(2)).getIndex());
    }

    public void testScalarGroupByFieldIsUntouched() {
        TableScan scan = stubScan(mockTable("test_index", "category"));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(count));

        RelNode rewritten = OpenSearchMultiValueGroupByRewriter.rewrite(aggregate);
        assertSame("no LIST group key present — must be a strict no-op", aggregate, rewritten);
    }

    private RelOptTable listTable(String tableName) {
        RelDataType element = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType list = typeFactory.createTypeWithNullability(typeFactory.createArrayType(element, -1), true);
        RelDataType rowType = typeFactory.builder().add("tags", list).build();
        return mockTableWithRowType(tableName, rowType);
    }

    private RelOptTable listPlusScalarTable(String tableName) {
        RelDataType element = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType list = typeFactory.createTypeWithNullability(typeFactory.createArrayType(element, -1), true);
        RelDataType rowType = typeFactory.builder()
            .add("tags", list)
            .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .build();
        return mockTableWithRowType(tableName, rowType);
    }

    private RelOptTable mockTableWithRowType(String tableName, RelDataType rowType) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(tableName));
        when(table.getRowType()).thenReturn(rowType);
        return table;
    }
}
