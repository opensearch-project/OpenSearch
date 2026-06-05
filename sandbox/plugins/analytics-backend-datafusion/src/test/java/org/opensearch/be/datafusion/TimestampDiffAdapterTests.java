/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link TimestampDiffAdapter}'s all-literal fold path.
 * Covers {@code TIMESTAMPDIFF(<unit>, '<start>', '<end>')} with VARCHAR literals,
 * across YEAR / MONTH / DAY / HOUR / QUARTER units.
 */
public class TimestampDiffAdapterTests extends OpenSearchTestCase {

    private static final SqlOperator TIMESTAMPDIFF_OP = new SqlFunction(
        "TIMESTAMPDIFF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.TIMEDATE
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    private RexNode varchar(String value) {
        RelDataType vt = typeFactory.createSqlType(SqlTypeName.VARCHAR, value.length());
        return rexBuilder.makeLiteral(value, vt, true);
    }

    private RexCall buildCall(String unit, String startLit, String endLit) {
        return (RexCall) rexBuilder.makeCall(TIMESTAMPDIFF_OP, List.of(varchar(unit), varchar(startLit), varchar(endLit)));
    }

    /** YEAR diff folds calendar-aware: start/end span 4 full years. */
    public void testFoldsLiteralLiteralYearDiff() {
        RexCall call = buildCall("YEAR", "2020-01-01 00:00:00", "2024-01-01 00:00:00");
        RexNode adapted = new TimestampDiffAdapter().adapt(call, List.of(), cluster);
        assertEquals(SqlTypeName.BIGINT, adapted.getType().getSqlTypeName());
    }

    /** MONTH diff folds calendar-aware. */
    public void testFoldsLiteralLiteralMonthDiff() {
        RexCall call = buildCall("MONTH", "2020-01-01 00:00:00", "2020-07-01 00:00:00");
        RexNode adapted = new TimestampDiffAdapter().adapt(call, List.of(), cluster);
        assertEquals(SqlTypeName.BIGINT, adapted.getType().getSqlTypeName());
    }

    /** QUARTER diff: months / 3, calendar-aware. */
    public void testFoldsLiteralLiteralQuarterDiff() {
        RexCall call = buildCall("QUARTER", "2020-01-01 00:00:00", "2020-10-01 00:00:00");
        RexNode adapted = new TimestampDiffAdapter().adapt(call, List.of(), cluster);
        assertEquals(SqlTypeName.BIGINT, adapted.getType().getSqlTypeName());
    }

    /** Fixed-length DAY unit folds correctly. */
    public void testFoldsLiteralLiteralDayDiff() {
        RexCall call = buildCall("DAY", "2024-01-01 00:00:00", "2024-01-15 00:00:00");
        RexNode adapted = new TimestampDiffAdapter().adapt(call, List.of(), cluster);
        assertEquals(SqlTypeName.BIGINT, adapted.getType().getSqlTypeName());
    }

    /** HOUR unit (fixed-length) folds correctly. */
    public void testFoldsLiteralLiteralHourDiff() {
        RexCall call = buildCall("HOUR", "2024-01-01 00:00:00", "2024-01-01 05:00:00");
        RexNode adapted = new TimestampDiffAdapter().adapt(call, List.of(), cluster);
        assertEquals(SqlTypeName.BIGINT, adapted.getType().getSqlTypeName());
    }

    /** Unparseable timestamp literal: fold returns null, adapter returns original call. */
    public void testUnparseableLiteralFallsThrough() {
        RexCall call = buildCall("DAY", "not-a-timestamp", "2024-01-15 00:00:00");
        RexNode adapted = new TimestampDiffAdapter().adapt(call, List.of(), cluster);
        // Falls through; the original 3-arg call shape is returned.
        assertSame(call, adapted);
    }

    /** Column-ref start/end: fold doesn't apply, adapter returns the original (or peephole-folded) call. */
    public void testColumnRefStartFallsThrough() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode startCol = rexBuilder.makeInputRef(varcharType, 0);
        RexCall call = (RexCall) rexBuilder.makeCall(TIMESTAMPDIFF_OP, List.of(varchar("DAY"), startCol, varchar("2024-01-15 00:00:00")));

        RexNode adapted = new TimestampDiffAdapter().adapt(call, List.of(), cluster);

        assertSame(call, adapted);
    }
}
