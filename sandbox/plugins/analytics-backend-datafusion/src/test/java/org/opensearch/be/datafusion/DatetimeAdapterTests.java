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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Covers DatetimeAdapter: 1-arg VARCHAR strips offset; 1-arg non-string passes through;
 * 2-arg literal-literal folds to a typed TIMESTAMP literal (or NULL on bad input);
 * 2-arg with column input rewrites to convert_tz wrapping.
 */
public class DatetimeAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private DateTimeAdapters.DatetimeAdapter adapter;

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
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        adapter = new DateTimeAdapters.DatetimeAdapter();
    }

    public void testSingleArgVarcharIsWrappedInRegexpReplace() {
        RexNode arg = rexBuilder.makeLiteral("2008-01-01 02:00:00+10:00");
        RexCall original = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(arg));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, call.getOperator());

        RexNode wrapped = call.getOperands().get(0);
        assertTrue(wrapped instanceof RexCall);
        assertSame(SqlLibraryOperators.REGEXP_REPLACE_3, ((RexCall) wrapped).getOperator());
    }

    public void testSingleArgTimestampPassesThroughUnwrapped() {
        RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
        RexNode arg = rexBuilder.makeAbstractCast(timestampType, rexBuilder.makeLiteral("2020-01-01 00:00:00"));
        RexCall original = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(arg));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, call.getOperator());
        // Non-string operand passes through to to_timestamp without regexp_replace.
        assertSame(arg, call.getOperands().get(0));
    }

    public void testTwoArgLiteralLiteralFoldsToTimestampLiteral() {
        // DATETIME('2008-12-25 05:30:00-05:00', '+05:00') = '2008-12-25 15:30:00'.
        RexNode ts = rexBuilder.makeLiteral("2008-12-25 05:30:00-05:00");
        RexNode tz = rexBuilder.makeLiteral("+05:00");
        RexCall original = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(ts, tz));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof org.apache.calcite.rex.RexLiteral);
        assertEquals(SqlTypeName.TIMESTAMP, adapted.getType().getSqlTypeName());
    }

    public void testTwoArgInvalidTzFoldsToNull() {
        // DATETIME(valid-string, '+15:00') — invalid tz → NULL TIMESTAMP literal.
        RexNode ts = rexBuilder.makeLiteral("2008-01-01 02:00:00+10:00");
        RexNode tz = rexBuilder.makeLiteral("+15:00");
        RexCall original = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(ts, tz));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof org.apache.calcite.rex.RexLiteral);
        assertTrue(((org.apache.calcite.rex.RexLiteral) adapted).isNull());
    }

    public void testTwoArgColumnInputRewritesToConvertTz() {
        // DATETIME(column, '+10:00') — non-literal first arg → convert_tz wrap.
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode columnRef = rexBuilder.makeInputRef(varchar, 0);
        RexNode tz = rexBuilder.makeLiteral("+10:00");
        RexCall original = (RexCall) rexBuilder.makeCall(DATETIME_OP, List.of(columnRef, tz));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertSame(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, ((RexCall) adapted).getOperator());
    }
}
