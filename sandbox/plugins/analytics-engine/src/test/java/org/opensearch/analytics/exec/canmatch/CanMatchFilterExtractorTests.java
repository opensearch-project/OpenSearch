/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CanMatchFilterExtractorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelNode scan;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner planner = new VolcanoPlanner();
        cluster = RelOptCluster.create(planner, rexBuilder);

        // Build a scan with fields: @timestamp (BIGINT), port (INTEGER), host (VARCHAR)
        RelDataType rowType = typeFactory.builder()
            .add("@timestamp", SqlTypeName.BIGINT)
            .add("port", SqlTypeName.INTEGER)
            .add("host", SqlTypeName.VARCHAR)
            .build();

        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("test_table"));
        when(table.getRowType()).thenReturn(rowType);

        scan = new StubTableScan(cluster, cluster.traitSet(), table);
    }

    private static class StubTableScan extends TableScan {
        protected StubTableScan(RelOptCluster cluster, org.apache.calcite.plan.RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, List.of(), table);
        }
    }

    public void testSimpleGreaterThan() {
        // @timestamp > 1000
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RelNode plan = LogicalFilter.create(scan, condition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals("@timestamp", range.column());
        assertEquals(1001L, range.min()); // GT → min = value + 1
        assertEquals(Long.MAX_VALUE, range.max());
    }

    public void testGreaterThanOrEqual() {
        // @timestamp >= 1000
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RelNode plan = LogicalFilter.create(scan, condition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals(1000L, range.min()); // GTE → min = value
        assertEquals(Long.MAX_VALUE, range.max());
    }

    public void testLessThan() {
        // port < 443
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(443, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode plan = LogicalFilter.create(scan, condition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals("port", range.column());
        assertEquals(Long.MIN_VALUE, range.min());
        assertEquals(442L, range.max()); // LT → max = value - 1
    }

    public void testMultipleAndPredicates() {
        // @timestamp >= 1000 AND port < 443
        RexNode tsCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RexNode portCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(443, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode andCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsCondition, portCondition);
        RelNode plan = LogicalFilter.create(scan, andCondition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(2, filters.size());
    }

    public void testMultipleRangesOnSameColumn() {
        // @timestamp >= 1000 AND port > 80 AND port < 443 (flat AND with 3 children)
        RexNode tsCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RexNode portGt = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(80, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode portLt = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(443, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        // Flat AND with 3 operands (Calcite flattens nested ANDs into this form)
        RexNode flatAnd = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsCondition, portGt, portLt);
        RelNode plan = LogicalFilter.create(scan, flatAnd);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(3, filters.size()); // ts>=1000, port>80, port<443
    }

    public void testTopLevelOrReturnsEmpty() {
        // @timestamp >= 1000 OR port < 443
        RexNode tsCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RexNode portCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(443, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode orCondition = rexBuilder.makeCall(SqlStdOperatorTable.OR, tsCondition, portCondition);
        RelNode plan = LogicalFilter.create(scan, orCondition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertTrue(filters.isEmpty());
    }

    public void testNonRangePredicateIgnored() {
        // host = 'foo' (equality — not a range, should be ignored)
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2),
            rexBuilder.makeLiteral("foo")
        );
        RelNode plan = LogicalFilter.create(scan, condition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertTrue(filters.isEmpty());
    }

    public void testNoFilterInPlanReturnsEmpty() {
        // Just a scan, no filter
        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(scan);
        assertTrue(filters.isEmpty());
    }

    public void testLiteralOnLeftSide() {
        // 1000 < @timestamp (reversed operands)
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        RelNode plan = LogicalFilter.create(scan, condition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals("@timestamp", range.column());
        // 1000 < @timestamp → @timestamp > 1000 → min = 1001
        assertEquals(1001L, range.min());
        assertEquals(Long.MAX_VALUE, range.max());
    }

    public void testBoundaryMaxValue() {
        // @timestamp > Long.MAX_VALUE - 1 (should saturate at MAX_VALUE)
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(Long.MAX_VALUE, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RelNode plan = LogicalFilter.create(scan, condition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals(Long.MAX_VALUE, range.min()); // saturated
    }

    public void testTimestampFunctionWrapper() {
        // Simulates CAST('2026-07-13':TIMESTAMP) — a RexCall wrapping a string literal.
        // This is what real PPL queries produce: >($0, TIMESTAMP('2026-07-13':VARCHAR))
        RexNode castToTimestamp = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3),
            rexBuilder.makeLiteral("2026-07-13")
        );
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            castToTimestamp
        );
        RelNode plan = LogicalFilter.create(scan, condition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals("@timestamp", range.column());
        // 2026-07-13 00:00:00 UTC = 1783900800000 epoch millis, GT bumps +1
        assertEquals(1783900800001L, range.min());
        assertEquals(Long.MAX_VALUE, range.max());
    }

    public void testAndWithNonRangeSibling() {
        // @timestamp >= 1000 AND host = 'foo' — equality is skipped, range extracted
        RexNode tsCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RexNode eqCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2),
            rexBuilder.makeLiteral("foo")
        );
        RexNode andCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsCondition, eqCondition);
        RelNode plan = LogicalFilter.create(scan, andCondition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals("@timestamp", range.column());
        assertEquals(1000L, range.min());
    }

    public void testAndWithNestedOrDoesNotClobberSiblingFilters() {
        // @timestamp >= 1000 AND (port = 80 OR port = 443)
        // The OR should be skipped, but the timestamp range should survive
        RexNode tsCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RexNode portEq80 = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(80, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode portEq443 = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(443, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode orCondition = rexBuilder.makeCall(SqlStdOperatorTable.OR, portEq80, portEq443);
        RexNode andCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsCondition, orCondition);
        RelNode plan = LogicalFilter.create(scan, andCondition);

        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(plan);
        // OR is skipped, but @timestamp range should still be extracted
        assertEquals(1, filters.size());
        LongRange range = (LongRange) filters.get(0);
        assertEquals("@timestamp", range.column());
        assertEquals(1000L, range.min());
    }
}
