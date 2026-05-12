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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

public class StrftimeFunctionAdapterTests extends OpenSearchTestCase {

    private final StrftimeFunctionAdapter adapter = new StrftimeFunctionAdapter();

    private static final SqlFunction STRFTIME = new SqlFunction(
        "strftime",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** Every numeric-or-string input slot lowers to CAST-to-DOUBLE so the Rust UDF sees one signature. */
    public void testNumericAndStringInputsWidenToDouble() {
        Cluster c = newCluster();
        RexNode[] sources = new RexNode[] {
            c.intLiteral(1521467703),
            c.rexBuilder.makeExactLiteral(BigDecimal.valueOf(1521467703L), c.typeFactory.createSqlType(SqlTypeName.BIGINT)),
            c.rexBuilder.makeExactLiteral(BigDecimal.valueOf(1521467703.123456), c.typeFactory.createSqlType(SqlTypeName.DECIMAL, 20, 6)),
            c.stringLiteral("1521467703"), };
        for (RexNode src : sources) {
            RexCall call = (RexCall) c.rexBuilder.makeCall(STRFTIME, src, c.stringLiteral("%Y-%m-%d"));
            RexCall out = assertStrftimeCall(adapter.adapt(call, List.of(), c.cluster));
            assertEquals(
                "source widened to DOUBLE: " + src.getType().getSqlTypeName(),
                SqlTypeName.DOUBLE,
                out.getOperands().get(0).getType().getSqlTypeName()
            );
        }
    }

    /** DOUBLE/TIMESTAMP/DATE operands forward by identity (Rust coerce_types canonicalizes). */
    public void testDoubleTimestampDateForwardByIdentity() {
        Cluster c = newCluster();
        RexNode dbl = c.rexBuilder.makeApproxLiteral(
            BigDecimal.valueOf(1521467703.123456),
            c.typeFactory.createSqlType(SqlTypeName.DOUBLE)
        );
        RexNode ts = c.rexBuilder.makeInputRef(c.typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), 0);
        RexNode dt = c.rexBuilder.makeInputRef(c.typeFactory.createSqlType(SqlTypeName.DATE), 0);
        for (RexNode src : new RexNode[] { dbl, ts, dt }) {
            RexCall call = (RexCall) c.rexBuilder.makeCall(STRFTIME, src, c.stringLiteral("%Y-%m-%d"));
            RexCall out = assertStrftimeCall(adapter.adapt(call, List.of(), c.cluster));
            assertSame("operand forwarded by identity: " + src.getType().getSqlTypeName(), src, out.getOperands().get(0));
        }
    }

    public void testFormatOperandForwardedVerbatim() {
        Cluster c = newCluster();
        RexNode format = c.stringLiteral("%a, %b %d, %Y %I:%M:%S %p %Z");
        RexCall call = (RexCall) c.rexBuilder.makeCall(STRFTIME, c.intLiteral(1521467703), format);
        RexCall out = assertStrftimeCall(adapter.adapt(call, List.of(), c.cluster));
        assertSame("format literal forwarded by identity", format, out.getOperands().get(1));
    }

    public void testWrongArityPassesThrough() {
        Cluster c = newCluster();
        RexCall call = (RexCall) c.rexBuilder.makeCall(STRFTIME, c.intLiteral(1521467703));
        RexNode out = adapter.adapt(call, List.of(), c.cluster);
        assertSame("single-arg call left unchanged — downstream planning should fail loudly", call, out);
    }

    private static RexCall assertStrftimeCall(RexNode out) {
        assertTrue("expected a RexCall, got " + out.getClass(), out instanceof RexCall);
        RexCall outCall = (RexCall) out;
        assertSame(
            "operator is the synthetic `strftime` that resolves to the Rust UDF",
            StrftimeFunctionAdapter.STRFTIME,
            outCall.getOperator()
        );
        assertEquals("two operands — value + format", 2, outCall.getOperands().size());
        return outCall;
    }

    private static Cluster newCluster() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        return new Cluster(RelOptCluster.create(planner, rexBuilder), typeFactory, rexBuilder);
    }

    private static final class Cluster {
        final RelOptCluster cluster;
        final RelDataTypeFactory typeFactory;
        final RexBuilder rexBuilder;

        Cluster(RelOptCluster cluster, RelDataTypeFactory typeFactory, RexBuilder rexBuilder) {
            this.cluster = cluster;
            this.typeFactory = typeFactory;
            this.rexBuilder = rexBuilder;
        }

        RexNode intLiteral(int value) {
            RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
            return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), intType);
        }

        RexNode stringLiteral(String value) {
            return rexBuilder.makeLiteral(value);
        }
    }
}
