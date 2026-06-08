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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link HyperbolicOperatorAdapter}. PPL's {@code SINH}/{@code COSH}
 * arrive as {@link SqlUserDefinedFunction} UDF calls; the adapter rewrites them to
 * use the Calcite library operator that isthmus {@code FunctionMappings.SCALAR_SIGS}
 * recognises ({@link SqlLibraryOperators#SINH}/{@link SqlLibraryOperators#COSH}),
 * so the plan serialises to the standard Substrait {@code sinh}/{@code cosh}
 * functions that DataFusion's substrait consumer natively evaluates.
 */
public class HyperbolicOperatorAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType doubleType;
    private SqlUserDefinedFunction sinhUdf;
    private SqlUserDefinedFunction coshUdf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        sinhUdf = fakeUdf("SINH");
        coshUdf = fakeUdf("COSH");
    }

    /** Fake PPL-style UDF — same name and kind as PPLBuiltinOperators's SINH/COSH. */
    private SqlUserDefinedFunction fakeUdf(String name) {
        return new SqlUserDefinedFunction(
            new SqlIdentifier(name, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            null,
            null
        );
    }

    public void testSinhUdfRewrittenToLibrarySinhOperator() {
        RexNode arg = rexBuilder.makeInputRef(doubleType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(sinhUdf, List.of(arg));
        assertEquals("SINH", original.getOperator().getName());
        assertFalse("precondition: operator is PPL UDF, not the library operator", original.getOperator() == SqlLibraryOperators.SINH);

        RexNode adapted = new HyperbolicOperatorAdapter(SqlLibraryOperators.SINH).adapt(original, List.of(), cluster);

        assertTrue("expected adapter to produce a RexCall", adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertSame("operator must be SqlLibraryOperators.SINH after adaptation", SqlLibraryOperators.SINH, adaptedCall.getOperator());
        assertEquals("operand count preserved", 1, adaptedCall.getOperands().size());
        assertSame("operand reference preserved", arg, adaptedCall.getOperands().get(0));
    }

    public void testCoshUdfRewrittenToLibraryCoshOperator() {
        RexNode arg = rexBuilder.makeInputRef(doubleType, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(coshUdf, List.of(arg));

        RexNode adapted = new HyperbolicOperatorAdapter(SqlLibraryOperators.COSH).adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertSame(SqlLibraryOperators.COSH, adaptedCall.getOperator());
        assertEquals(1, adaptedCall.getOperands().size());
        assertSame(arg, adaptedCall.getOperands().get(0));
    }

    /**
     * Non-UDF calls (e.g. {@code ABS($0)}) must pass through untouched. Guards
     * against collateral damage if the adapter is registered against a
     * different {@code ScalarFunction} by mistake.
     */
    public void testAdaptPassesThroughUnrelatedCall() {
        RexNode arg = rexBuilder.makeInputRef(doubleType, 0);
        RexCall absCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.ABS, List.of(arg));

        RexNode adapted = new HyperbolicOperatorAdapter(SqlLibraryOperators.SINH).adapt(absCall, List.of(), cluster);

        assertSame("non-SINH call must pass through unmodified", absCall, adapted);
    }
}
