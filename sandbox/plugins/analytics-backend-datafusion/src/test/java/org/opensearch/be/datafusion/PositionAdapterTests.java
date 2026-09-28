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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link PositionAdapter}.
 *
 * <p>Coverage:
 * <ul>
 *   <li>2-arg form: {@code POSITION(substr, str)} swaps operands to
 *       {@code strpos(str, substr)}.</li>
 *   <li>3-arg form: {@code POSITION(substr, str, start)} decomposes into a CASE
 *       expression around {@code substring(str, start)} + {@code strpos} + offset
 *       arithmetic so the 1-indexed {@code start} parameter and the
 *       "{@code 0} on not found" contract both hold.</li>
 *   <li>Malformed arity passes through unchanged (no 0, 1, or 4-arg rewrite).</li>
 * </ul>
 */
public class PositionAdapterTests extends OpenSearchTestCase {

    private static final SqlFunction POSITION = new SqlFunction(
        "POSITION",
        SqlKind.POSITION,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.family(),
        SqlFunctionCategory.STRING
    );

    private final PositionAdapter adapter = new PositionAdapter();

    /** {@code POSITION('U', 'FURNITURE')} → {@code strpos('FURNITURE', 'U')}. */
    public void testTwoArgSwapsOperands() {
        Cluster cluster = newCluster();
        RexNode substr = cluster.stringLiteral("U");
        RexNode str = cluster.stringLiteral("FURNITURE");
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(POSITION, substr, str);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        RexCall outCall = assertStrposCall(out);
        assertEquals("strpos must be (str, substr) — 2 operands", 2, outCall.getOperands().size());
        assertSame("first operand is str (was the second POSITION arg)", str, outCall.getOperands().get(0));
        assertSame("second operand is substr (was the first POSITION arg)", substr, outCall.getOperands().get(1));
    }

    /**
     * {@code POSITION('U', 'FURNITURE', 3)} decomposes to
     * {@code CASE WHEN strpos(substring(str, start), substr) = 0 THEN 0 ELSE strpos(...) + start - 1 END}.
     * This test asserts the outer CASE shape; the inner sub-calls are validated separately.
     */
    public void testThreeArgDecomposesToCaseOfSubstringStrpos() {
        Cluster cluster = newCluster();
        RexNode substr = cluster.stringLiteral("U");
        RexNode str = cluster.stringLiteral("FURNITURE");
        RexNode start = cluster.intLiteral(3);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(POSITION, substr, str, start);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertEquals("3-arg POSITION lowers to CASE", SqlKind.CASE, out.getKind());
        RexCall caseCall = (RexCall) out;
        assertEquals("CASE shape — WHEN cond THEN 0 ELSE adjusted", 3, caseCall.getOperands().size());

        // operand[0]: strpos(substring(str, start), substr) = 0
        RexCall whenCond = (RexCall) caseCall.getOperands().get(0);
        assertEquals("WHEN is an equality test", SqlKind.EQUALS, whenCond.getKind());

        // operand[1]: the THEN value is the literal 0.
        assertEquals(
            "THEN returns 0 when substring didn't contain substr",
            0,
            ((org.apache.calcite.rex.RexLiteral) caseCall.getOperands().get(1)).getValueAs(Integer.class).intValue()
        );

        // operand[2]: the ELSE arm is strpos(...) + start - 1.
        RexCall elseArm = (RexCall) caseCall.getOperands().get(2);
        assertEquals("ELSE performs the final offset subtraction", SqlKind.MINUS, elseArm.getKind());
    }

    public void testThreeArgElseArmBuildsSubstringAndStrpos() {
        Cluster cluster = newCluster();
        RexNode substr = cluster.stringLiteral("U");
        RexNode str = cluster.stringLiteral("FURNITURE");
        RexNode start = cluster.intLiteral(3);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(POSITION, substr, str, start);

        RexCall caseCall = (RexCall) adapter.adapt(call, List.of(), cluster.cluster);

        // ELSE shape: MINUS(PLUS(strpos(substring(str, start), substr), start), 1)
        RexCall minusCall = (RexCall) caseCall.getOperands().get(2);
        RexCall plusCall = (RexCall) minusCall.getOperands().get(0);
        assertEquals(SqlKind.PLUS, plusCall.getKind());
        RexCall strposInElse = (RexCall) plusCall.getOperands().get(0);
        assertSame("ELSE arm's strpos reuses the shared operator", PositionAdapter.STRPOS, strposInElse.getOperator());

        RexCall substringCall = (RexCall) strposInElse.getOperands().get(0);
        assertSame(
            "substring call uses the standard SqlStdOperatorTable.SUBSTRING",
            SqlStdOperatorTable.SUBSTRING,
            substringCall.getOperator()
        );
        assertSame("substring(str, start) — str is the original second POSITION operand", str, substringCall.getOperands().get(0));
        assertSame("substring(str, start) — start is the original third POSITION operand", start, substringCall.getOperands().get(1));
        assertSame("strpos substr is the original first POSITION operand", substr, strposInElse.getOperands().get(1));
    }

    public void testThreeArgWhenConditionMirrorsElseStrpos() {
        Cluster cluster = newCluster();
        RexNode substr = cluster.stringLiteral("U");
        RexNode str = cluster.stringLiteral("FURNITURE");
        RexNode start = cluster.intLiteral(3);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(POSITION, substr, str, start);

        RexCall caseCall = (RexCall) adapter.adapt(call, List.of(), cluster.cluster);

        RexCall whenCond = (RexCall) caseCall.getOperands().get(0);
        // WHEN: strpos(substring(str, start), substr) = 0
        RexCall strposInWhen = (RexCall) whenCond.getOperands().get(0);
        assertSame("WHEN condition's strpos is the shared operator", PositionAdapter.STRPOS, strposInWhen.getOperator());
        RexCall substringInWhen = (RexCall) strposInWhen.getOperands().get(0);
        assertSame(SqlStdOperatorTable.SUBSTRING, substringInWhen.getOperator());
        assertSame(str, substringInWhen.getOperands().get(0));
        assertSame(start, substringInWhen.getOperands().get(1));
    }

    public void testAdaptedStrposIsTheSharedOperatorInstance() {
        Cluster cluster = newCluster();
        RexNode substr = cluster.stringLiteral("a");
        RexNode str = cluster.stringLiteral("abc");
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(POSITION, substr, str);

        RexCall outCall = assertStrposCall(adapter.adapt(call, List.of(), cluster.cluster));

        assertSame(
            "adapter must emit the shared PositionAdapter.STRPOS instance, not a clone",
            PositionAdapter.STRPOS,
            outCall.getOperator()
        );
        assertEquals(
            "operator name is 'strpos' — what DataFusion's substrait consumer expects",
            "strpos",
            PositionAdapter.STRPOS.getName()
        );
    }

    public void testOneArgPassesThrough() {
        Cluster cluster = newCluster();
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(POSITION, cluster.stringLiteral("a"));
        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);
        assertSame("1-arg POSITION is malformed and must pass through", call, out);
    }

    public void testFourArgPassesThrough() {
        Cluster cluster = newCluster();
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(
            POSITION,
            cluster.stringLiteral("a"),
            cluster.stringLiteral("abc"),
            cluster.intLiteral(1),
            cluster.intLiteral(1)
        );
        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);
        assertSame("4-arg POSITION is malformed and must pass through", call, out);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Assert the adapted call is a 2-arg {@code strpos} call routed through the shared operator. */
    private static RexCall assertStrposCall(RexNode out) {
        assertTrue("expected a RexCall, got " + out.getClass(), out instanceof RexCall);
        RexCall outCall = (RexCall) out;
        assertSame(
            "operator is the shared strpos registered against the FunctionMappings.Sig",
            PositionAdapter.STRPOS,
            outCall.getOperator()
        );
        return outCall;
    }

    private static Cluster newCluster() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        return new Cluster(cluster, typeFactory, rexBuilder);
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
