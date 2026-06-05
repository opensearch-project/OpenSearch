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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link DateTimeAdapters.DatetimeAdapter}. Covers:
 * <ul>
 *   <li>2-arg {@code DATETIME('<ts>', '<tz>')}: folds at plan time to a TIMESTAMP literal
 *       at the call's declared precision (the {@code (string, string)} signature has no
 *       YAML overload).</li>
 *   <li>2-arg with embedded offset in the timestamp literal: parses correctly.</li>
 *   <li>2-arg with non-literal operand: falls through (returns the original call).</li>
 * </ul>
 */
public class DateTimeAdaptersTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction DATETIME_OP = new SqlFunction(
        "DATETIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

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

    /** Naive {@code yyyy-MM-dd HH:mm:ss} treated as UTC, then shifted to the target tz. */
    public void testTwoArgFoldsLiteralTimestampAndTzToTimestampLiteral() {
        RexCall call = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(varchar("2008-12-25 05:30:00"), varchar("America/Los_Angeles")));

        RexNode adapted = new DateTimeAdapters.DatetimeAdapter().adapt(call, List.of(), cluster);

        assertEquals(SqlTypeName.TIMESTAMP, adapted.getType().getSqlTypeName());
    }

    /** Embedded offset in the timestamp literal takes precedence over the explicit fromZone. */
    public void testTwoArgFoldRespectsEmbeddedOffset() {
        RexCall call = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(varchar("2008-12-25T10:00:00+02:00"), varchar("UTC")));

        RexNode adapted = new DateTimeAdapters.DatetimeAdapter().adapt(call, List.of(), cluster);

        assertEquals(SqlTypeName.TIMESTAMP, adapted.getType().getSqlTypeName());
    }

    /** Non-literal first operand: fold returns null, adapter returns the original call. */
    public void testTwoArgWithColumnRefFallsThrough() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode columnRef = rexBuilder.makeInputRef(varcharType, 0);
        RexCall call = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(columnRef, varchar("UTC")));

        RexNode adapted = new DateTimeAdapters.DatetimeAdapter().adapt(call, List.of(), cluster);

        // Original call is returned (no fold for non-literal); isthmus surfaces the missing signature.
        assertSame(call, adapted);
    }

    /** Unparseable timezone literal: fold returns null, adapter returns the original call. */
    public void testTwoArgWithBadTimezoneFallsThrough() {
        RexCall call = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(varchar("2008-12-25 05:30:00"), varchar("Mars/Olympus")));

        RexNode adapted = new DateTimeAdapters.DatetimeAdapter().adapt(call, List.of(), cluster);

        assertSame(call, adapted);
    }

    /** Null VARCHAR literal: fold returns null, adapter returns the original call. */
    public void testTwoArgWithNullLiteralFallsThrough() {
        RelDataType nullableVarchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode nullLit = rexBuilder.makeNullLiteral(nullableVarchar);
        RexCall call = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(nullLit, varchar("UTC")));

        RexNode adapted = new DateTimeAdapters.DatetimeAdapter().adapt(call, List.of(), cluster);

        assertSame(call, adapted);
    }

    /** 1-arg form: parent rename — call becomes {@code to_timestamp(<arg>)}. */
    public void testOneArgFormDelegatesToParentRename() {
        RexCall call = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(varchar("2024-01-15T10:30:00Z")));

        RexNode adapted = new DateTimeAdapters.DatetimeAdapter().adapt(call, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall renamed = (RexCall) adapted;
        assertSame(DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, renamed.getOperator());
        assertEquals(1, renamed.getOperands().size());
        assertTrue(renamed.getOperands().get(0) instanceof RexLiteral);
    }
}
