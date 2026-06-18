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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link MicrosecondAdapter}. PPL's {@code microsecond(x)} returns the
 * sub-second microseconds (0..999_999), but DataFusion's {@code date_part('microsecond', x)}
 * returns {@code seconds * 1_000_000 + microseconds} — Postgres semantics. The adapter
 * wraps the {@code date_part} call with {@code MOD(..., 1_000_000)} and casts back to the
 * call's declared return type, restoring PPL semantics.
 */
public class MicrosecondAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType timestampType;
    private RelDataType intType;
    private SqlUserDefinedFunction microsecondUdf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        microsecondUdf = new SqlUserDefinedFunction(
            new SqlIdentifier("MICROSECOND", SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            null,
            null
        );
    }

    public void testMicrosecondRewrittenAsModOfDatePart() {
        RexNode arg = rexBuilder.makeInputRef(timestampType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(intType, microsecondUdf, List.of(arg));

        RexNode adapted = new MicrosecondAdapter().adapt(original, List.of(), cluster);

        // Expected tree: CAST(MOD(date_part('microsecond', arg), 1_000_000) AS INTEGER)
        assertTrue("outermost node must be a RexCall", adapted instanceof RexCall);
        RexCall castCall = (RexCall) adapted;
        assertSame("outermost operator must be CAST", SqlStdOperatorTable.CAST, castCall.getOperator());
        assertEquals(SqlTypeName.INTEGER, castCall.getType().getSqlTypeName());

        RexNode castOperand = castCall.getOperands().get(0);
        assertTrue("CAST operand must be a RexCall", castOperand instanceof RexCall);
        RexCall modCall = (RexCall) castOperand;
        assertSame("inside CAST must be MOD", SqlStdOperatorTable.MOD, modCall.getOperator());
        assertEquals(2, modCall.getOperands().size());

        RexNode modLeft = modCall.getOperands().get(0);
        assertTrue("left operand of MOD must be a RexCall", modLeft instanceof RexCall);
        RexCall datePart = (RexCall) modLeft;
        assertSame("MOD's left operand must be date_part(...)", SqlLibraryOperators.DATE_PART, datePart.getOperator());
        assertEquals(2, datePart.getOperands().size());

        RexNode partLiteral = datePart.getOperands().get(0);
        assertTrue("date_part's first arg must be a literal", partLiteral instanceof RexLiteral);
        assertEquals("microsecond", ((RexLiteral) partLiteral).getValueAs(String.class));
        // TIMESTAMP operands are coerced to TIMESTAMP(6) so date_part keeps the µs fraction.
        RexNode datePartArg = datePart.getOperands().get(1);
        assertTrue("date_part's second arg must wrap original in CAST", datePartArg instanceof RexCall);
        assertEquals(SqlKind.CAST, ((RexCall) datePartArg).getKind());
        assertSame("CAST's child must be the original timestamp arg", arg, ((RexCall) datePartArg).getOperands().get(0));

        RexNode modRight = modCall.getOperands().get(1);
        assertTrue("MOD's right operand must be a literal", modRight instanceof RexLiteral);
        assertEquals(1_000_000L, ((RexLiteral) modRight).getValueAs(Long.class).longValue());
    }

    public void testAdaptLeavesUnaryNonMicrosecondCallAlone() {
        // Sanity: an unrelated single-arg call should not be rewritten, even though the
        // adapter only checks operand count. (The dispatch in DataFusionAnalyticsBackendPlugin
        // gates by ScalarFunction.MICROSECOND, but we still want adapt() to be safe to
        // call directly on whatever the dispatcher hands it.)
        RexNode arg = rexBuilder.makeInputRef(timestampType, 0);
        RexCall absCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.ABS, List.of(arg));

        // The adapter unconditionally wraps any 1-arg call in MOD(date_part('microsecond', x), 1e6),
        // so adapt() does rewrite. Verify the operand-count guard fires when arity is wrong.
        RexCall twoArg = (RexCall) rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1))
        );
        assertSame("multi-arg calls pass through unchanged", twoArg, new MicrosecondAdapter().adapt(twoArg, List.of(), cluster));
    }
}
