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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/** Adapter-level coverage for {@link TimestampAddAdapter}. */
public class TimestampAddAdapterTests extends OpenSearchTestCase {

    private static final SqlOperator TIMESTAMPADD_OP = new SqlFunction(
        "TIMESTAMPADD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP_NULLABLE,
        null,
        OperandTypes.family(
            org.apache.calcite.sql.type.SqlTypeFamily.ANY,
            org.apache.calcite.sql.type.SqlTypeFamily.ANY,
            org.apache.calcite.sql.type.SqlTypeFamily.ANY
        ),
        SqlFunctionCategory.TIMEDATE
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private TimestampAddAdapter adapter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        adapter = new TimestampAddAdapter();
    }

    public void testFoldYearAdd() {
        // TIMESTAMPADD('YEAR', 15, '2001-03-06 00:00:00') → 2016-03-06 00:00:00 timestamp literal.
        RexCall call = call("YEAR", 15, "2001-03-06 00:00:00");
        RexNode adapted = adapter.adapt(call, List.of(), cluster);
        TimestampString ts = unwrapTimestampLiteral(adapted);
        assertEquals("2016-03-06 00:00:00", ts.toString());
    }

    public void testFoldQuarterAdd() {
        // QUARTER → multiplyExact ×3 MONTHS.
        RexCall call = call("QUARTER", 2, "2001-01-01 00:00:00");
        RexNode adapted = adapter.adapt(call, List.of(), cluster);
        TimestampString ts = unwrapTimestampLiteral(adapted);
        assertEquals("2001-07-01 00:00:00", ts.toString());
    }

    public void testFoldHourAdd() {
        RexCall call = call("HOUR", 5, "2020-01-01 12:00:00");
        RexNode adapted = adapter.adapt(call, List.of(), cluster);
        TimestampString ts = unwrapTimestampLiteral(adapted);
        assertEquals("2020-01-01 17:00:00", ts.toString());
    }

    public void testFoldNegativeShift() {
        RexCall call = call("DAY", -7, "2020-01-08 00:00:00");
        RexNode adapted = adapter.adapt(call, List.of(), cluster);
        TimestampString ts = unwrapTimestampLiteral(adapted);
        assertEquals("2020-01-01 00:00:00", ts.toString());
    }

    public void testNonMatchingOperatorPassesThrough() {
        SqlOperator other = new SqlFunction(
            "OTHER",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.TIMESTAMP_NULLABLE,
            null,
            OperandTypes.family(
                org.apache.calcite.sql.type.SqlTypeFamily.ANY,
                org.apache.calcite.sql.type.SqlTypeFamily.ANY,
                org.apache.calcite.sql.type.SqlTypeFamily.ANY
            ),
            SqlFunctionCategory.TIMEDATE
        );
        RexCall call = (RexCall) rexBuilder.makeCall(other, stringLit("YEAR"), intLit(1), stringLit("2020-01-01 00:00:00"));
        assertSame(call, adapter.adapt(call, List.of(), cluster));
    }

    public void testNonLiteralOperandFallsThrough() {
        // Column ref instead of string literal — fold doesn't apply, original returned.
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexCall call = (RexCall) rexBuilder.makeCall(TIMESTAMPADD_OP, stringLit("YEAR"), intLit(1), rexBuilder.makeInputRef(varchar, 0));
        assertSame(call, adapter.adapt(call, List.of(), cluster));
    }

    public void testUnknownUnitFallsThrough() {
        RexCall call = call("FORTNIGHT", 1, "2020-01-01 00:00:00");
        assertSame(call, adapter.adapt(call, List.of(), cluster));
    }

    public void testMalformedTimestampFallsThrough() {
        RexCall call = call("DAY", 1, "definitely-not-a-timestamp");
        assertSame(call, adapter.adapt(call, List.of(), cluster));
    }

    private RexCall call(String unit, int n, String ts) {
        return (RexCall) rexBuilder.makeCall(TIMESTAMPADD_OP, stringLit(unit), intLit(n), stringLit(ts));
    }

    private RexLiteral stringLit(String s) {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        return (RexLiteral) rexBuilder.makeLiteral(s, varchar, true);
    }

    private RexLiteral intLit(int n) {
        return (RexLiteral) rexBuilder.makeBigintLiteral(BigDecimal.valueOf(n));
    }

    /** Walk past CAST wrappers to the underlying TIMESTAMP literal. */
    private static TimestampString unwrapTimestampLiteral(RexNode node) {
        while (node instanceof RexCall call && call.getKind() == SqlKind.CAST) {
            node = call.getOperands().get(0);
        }
        if (!(node instanceof RexLiteral lit)) {
            throw new AssertionError("expected a RexLiteral after CAST unwrap, got " + node);
        }
        TimestampString ts = lit.getValueAs(TimestampString.class);
        if (ts == null) {
            throw new AssertionError("expected TIMESTAMP literal, got " + lit);
        }
        return ts;
    }
}
