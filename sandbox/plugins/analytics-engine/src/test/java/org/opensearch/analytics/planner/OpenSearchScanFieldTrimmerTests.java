/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link org.opensearch.analytics.planner.rules.OpenSearchScanFieldTrimmer}
 * exercised end-to-end via {@link PlannerImpl#markAndOptimize}.
 *
 * <p>The trimmer's contract is to narrow each {@code TableScan}'s row type to the columns
 * the plan actually requires (collected via Calcite's required-field propagation through
 * Project / Filter / Aggregate / Sort / Join / SetOp / Project-over-RexOver) before
 * {@code OpenSearchTableScanRule} resolves backend viability. The high-leverage scenario
 * is an index that contains a column whose mapping type has no scan-capable backend
 * (e.g. {@code geo_point} on a parquet-only cluster) — without the trimmer, the scan
 * rule rejects the whole index even when the query never reads that column.
 */
public class OpenSearchScanFieldTrimmerTests extends BasePlannerRulesTests {

    public void testProjectOverScanNarrowsToReferencedColumn() {
        // Project reads only `status`; `loc` (geo_point) is unread. Without trimming the
        // unread geo_point would block viability for the whole index.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        RelOptTable table = mockThreeColTable();
        TableScan scan = stubScan(table);

        RelDataType intType = scan.getRowType().getFieldList().get(0).getType();
        LogicalProject project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0)),
            (List<String>) null,
            Set.of()
        );

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(project, context)));
        assertNotNull(narrowedScan);
        assertEquals(1, narrowedScan.getOutputFieldStorage().size());
        assertEquals("status", narrowedScan.getOutputFieldStorage().get(0).getFieldName());
    }

    public void testProjectOverFilterPropagatesFilterRefsThroughToScan() {
        // SELECT status FROM t WHERE size > 0 — filter refers to `size`, project to `status`,
        // both must survive the trim. `loc` (geo_point) must be dropped.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        RelOptTable table = mockThreeColTable();
        TableScan scan = stubScan(table);

        RelDataType intType = scan.getRowType().getFieldList().get(0).getType();
        RexNode sizeGtZero = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeLiteral(0, intType, true)
        );
        LogicalFilter filter = LogicalFilter.create(scan, sizeGtZero);
        LogicalProject project = LogicalProject.create(
            filter,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0)),
            (List<String>) null,
            Set.of()
        );

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(project, context)));
        assertNotNull(narrowedScan);
        assertEquals(2, narrowedScan.getOutputFieldStorage().size());
        // Original column order is preserved (status before size).
        assertEquals("status", narrowedScan.getOutputFieldStorage().get(0).getFieldName());
        assertEquals("size", narrowedScan.getOutputFieldStorage().get(1).getFieldName());
    }

    public void testAggregateCountStarDropsAllScanColumnsButLeavesPlaceholder() {
        // SELECT count(*) FROM t — aggregate references zero scan columns. The trimmer
        // must leave at least one readable column so the scan still has a row type.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        RelOptTable table = mockThreeColTable();
        TableScan scan = stubScan(table);

        AggregateCall countStar = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count_star"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(countStar));

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(aggregate, context)));
        assertNotNull(narrowedScan);
        // Exactly one placeholder column survives — and it must be a readable scalar (status
        // or size), never the unreadable geo_point `loc`.
        assertEquals(1, narrowedScan.getOutputFieldStorage().size());
        String placeholder = narrowedScan.getOutputFieldStorage().get(0).getFieldName();
        assertTrue(
            "Placeholder must be a readable column, was [" + placeholder + "]",
            placeholder.equals("status") || placeholder.equals("size")
        );
    }

    public void testAggregateGroupByStatusSumSizeNarrowsToReferencedColumns() {
        // SELECT status, sum(size) FROM t GROUP BY status — both `status` (group key) and
        // `size` (aggregate input) must survive trimming. `loc` (geo_point) is unread and
        // must be dropped before the scan rule's viability check runs.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        RelOptTable table = mockThreeColTable();
        TableScan scan = stubScan(table);

        AggregateCall sumSize = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum_size"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sumSize));

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(aggregate, context)));
        assertNotNull(narrowedScan);
        assertEquals(2, narrowedScan.getOutputFieldStorage().size());
        // Original column order is preserved.
        assertEquals("status", narrowedScan.getOutputFieldStorage().get(0).getFieldName());
        assertEquals("size", narrowedScan.getOutputFieldStorage().get(1).getFieldName());
    }

    public void testProjectOverSortOverFilterCollectsFromAllThreeOperators() {
        // SELECT status FROM t WHERE size > 0 ORDER BY status — three layers; the sort
        // collation references `status`, the filter `size`, the project `status`. Trim
        // should keep both scalars and drop `loc`.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        RelOptTable table = mockThreeColTable();
        TableScan scan = stubScan(table);

        RelDataType intType = scan.getRowType().getFieldList().get(0).getType();
        RexNode sizeGtZero = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeLiteral(0, intType, true)
        );
        LogicalFilter filter = LogicalFilter.create(scan, sizeGtZero);
        LogicalSort sort = LogicalSort.create(
            filter,
            org.apache.calcite.rel.RelCollations.of(new org.apache.calcite.rel.RelFieldCollation(0)),
            null,
            null
        );
        LogicalProject project = LogicalProject.create(
            sort,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0)),
            (List<String>) null,
            Set.of()
        );

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(project, context)));
        assertNotNull(narrowedScan);
        assertEquals(2, narrowedScan.getOutputFieldStorage().size());
    }

    public void testJoinOverTwoScansTrimEachIndependently() {
        // Self-join on `status`; project picks `size` from one side. Each scan should be
        // trimmed independently to only the columns referenced by its branch + the join
        // condition.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        TableScan left = stubScan(mockThreeColTable());
        TableScan right = stubScan(mockThreeColTable());

        RelDataType intType = left.getRowType().getFieldList().get(0).getType();
        // Join condition: left.status = right.status (left fields 0..2, right fields 3..5).
        RexNode joinCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, 3)
        );
        LogicalJoin join = LogicalJoin.create(left, right, ImmutableList.of(), joinCond, Set.of(), JoinRelType.INNER);
        // Project left.size only.
        LogicalProject project = LogicalProject.create(
            join,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 1)),
            (List<String>) null,
            Set.of()
        );

        RelNode result = runPlanner(project, context);
        // Two scans must remain in the marked tree, each with `loc` trimmed away.
        long scanCount = countScans(result);
        assertEquals("Expected two scans after marking the joined plan", 2L, scanCount);
        forEachScan(result, narrowed -> {
            for (var info : narrowed.getOutputFieldStorage()) {
                assertNotEquals("geo_point column `loc` must not survive trimming", "loc", info.getFieldName());
            }
        });
    }

    public void testProjectReadingEveryColumnDoesNotNarrow() {
        // SELECT status, size FROM t — the project reads every column on a 2-col index.
        // The trimmer must leave the scan as-is (no synthetic narrowing).
        PlannerContext context = buildContext("parquet", intFields());
        RelOptTable table = mockTable("test_index", "status", "size");
        TableScan scan = stubScan(table);

        RelDataType intType = scan.getRowType().getFieldList().get(0).getType();
        LogicalProject project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1)),
            (List<String>) null,
            Set.of()
        );

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(project, context)));
        assertNotNull(narrowedScan);
        assertEquals(2, narrowedScan.getOutputFieldStorage().size());
    }

    public void testLiteralOnlyProjectPicksReadablePlaceholderNotGeoPoint() {
        // SELECT 1 FROM t — no input refs at all. Trimmer must pick a *readable* column
        // (status or size), not the geo_point — otherwise it just relocates the same
        // viability failure to the placeholder.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        RelOptTable table = mockThreeColTable();
        TableScan scan = stubScan(table);

        RelDataType intType = scan.getRowType().getFieldList().get(0).getType();
        LogicalProject project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeLiteral(1, intType, true)),
            (List<String>) null,
            Set.of()
        );

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(project, context)));
        assertNotNull(narrowedScan);
        assertEquals(1, narrowedScan.getOutputFieldStorage().size());
        String placeholder = narrowedScan.getOutputFieldStorage().get(0).getFieldName();
        assertTrue(
            "Placeholder must be a readable scalar (status/size), was [" + placeholder + "]",
            placeholder.equals("status") || placeholder.equals("size")
        );
    }

    public void testProjectExpressionStillCollectsTheReferencedColumn() {
        // eval x = status + 1 — the project's only RexNode is a RexCall, but its leaf
        // RexInputRef must still be discovered by the trimmer's input-finder traversal.
        PlannerContext context = buildContext("parquet", twoIntsPlusGeoPoint());
        RelOptTable table = mockThreeColTable();
        TableScan scan = stubScan(table);

        RelDataType intType = scan.getRowType().getFieldList().get(0).getType();
        RexNode statusPlusOne = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeLiteral(1, intType, true)
        );
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(statusPlusOne), (List<String>) null, Set.of());

        OpenSearchTableScan narrowedScan = findScan(unwrapExchange(runPlanner(project, context)));
        assertNotNull(narrowedScan);
        assertEquals(1, narrowedScan.getOutputFieldStorage().size());
        assertEquals("status", narrowedScan.getOutputFieldStorage().get(0).getFieldName());
    }

    // ---- helpers ----

    private OpenSearchTableScan findScan(RelNode node) {
        return RelNodeUtils.findNode(node, OpenSearchTableScan.class);
    }

    private long countScans(RelNode root) {
        long[] counter = { 0L };
        walkScans(root, s -> counter[0]++);
        return counter[0];
    }

    private void forEachScan(RelNode root, java.util.function.Consumer<OpenSearchTableScan> fn) {
        walkScans(root, fn);
    }

    private static void walkScans(RelNode node, java.util.function.Consumer<OpenSearchTableScan> fn) {
        if (node instanceof OpenSearchTableScan scan) {
            fn.accept(scan);
            return;
        }
        for (RelNode input : node.getInputs()) {
            walkScans(input, fn);
        }
    }

    /**
     * Three-column index: two readable INTEGERs + one geo_point that the parquet backend
     * cannot scan. This is the canonical scan-viability shape — the trimmer must drop
     * `loc` from the scan whenever the query does not actually read it.
     */
    private static Map<String, Map<String, Object>> twoIntsPlusGeoPoint() {
        return Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer"), "loc", Map.of("type", "geo_point"));
    }

    private RelOptTable mockThreeColTable() {
        return mockTable(
            "test_index",
            new String[] { "status", "size", "loc" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.VARCHAR }
        );
    }
}
