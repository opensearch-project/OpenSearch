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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link NowFspAdapter}. PPL {@code now([fsp])} (and its
 * {@code current_timestamp} / {@code sysdate} synonyms) accept an optional fractional-seconds
 * precision argument that DataFusion's niladic {@code now()} cannot express. The adapter drops
 * the {@code fsp} operand and emits the substrait-mapped {@link DateTimeAdapters#LOCAL_NOW_OP},
 * so {@code now(3)} ≡ {@code now()} (matching the SQL-plugin reference) instead of failing
 * fragment conversion with {@code Unable to convert call now(i32)}.
 */
public class NowFspAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType timestampType;
    private SqlUserDefinedFunction nowUdf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        nowUdf = new SqlUserDefinedFunction(
            new SqlIdentifier("NOW", SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.TIMESTAMP,
            null,
            null,
            null
        );
    }

    public void testNiladicNowRoutedToLocalNowOp() {
        RexCall original = (RexCall) rexBuilder.makeCall(timestampType, nowUdf, List.of());

        RexNode adapted = new NowFspAdapter().adapt(original, List.of(), cluster);

        assertTrue("expected a RexCall", adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame("operator must be the substrait-mapped local now()", DateTimeAdapters.LOCAL_NOW_OP, call.getOperator());
        assertTrue("no operands on niladic now()", call.getOperands().isEmpty());
        assertEquals("return type preserved", SqlTypeName.TIMESTAMP, call.getType().getSqlTypeName());
    }

    public void testFspOperandDropped() {
        RexNode fsp = rexBuilder.makeLiteral(3, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexCall original = (RexCall) rexBuilder.makeCall(timestampType, nowUdf, List.of(fsp));

        RexNode adapted = new NowFspAdapter().adapt(original, List.of(), cluster);

        RexCall call = (RexCall) adapted;
        assertSame("operator must be local now()", DateTimeAdapters.LOCAL_NOW_OP, call.getOperator());
        assertTrue("fsp operand must be dropped so DataFusion's niladic now() binds", call.getOperands().isEmpty());
    }

    public void testUnexpectedArityLeftUntouched() {
        // now() / now(fsp) are the only valid shapes; a 2-arg call must not be normalised into now().
        RexNode a = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexNode b = rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexCall original = (RexCall) rexBuilder.makeCall(timestampType, nowUdf, List.of(a, b));

        RexNode adapted = new NowFspAdapter().adapt(original, List.of(), cluster);

        assertSame("adapter must not invent a valid shape from a 2-arg call", original, adapted);
    }
}
