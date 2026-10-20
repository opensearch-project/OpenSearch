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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link EConstantAdapter}. PPL's {@code E()} UDF has zero operands
 * and evaluates to Euler's number. DataFusion has no matching scalar function, but
 * constant folding is cheap on the coordinator — the adapter rewrites the UDF call
 * to a {@code DOUBLE} literal equal to {@link Math#E}, which serialises trivially
 * through Substrait as a literal expression.
 */
public class EConstantAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType doubleType;
    private SqlUserDefinedFunction eUdf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        eUdf = new SqlUserDefinedFunction(
            new SqlIdentifier("E", SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE,
            null,
            null,
            null
        );
    }

    public void testEUdfRewrittenToMathELiteral() {
        RexCall original = (RexCall) rexBuilder.makeCall(eUdf, List.of());

        RexNode adapted = new EConstantAdapter().adapt(original, List.of(), cluster);

        assertTrue("expected adapter to return a literal", adapted instanceof RexLiteral);
        RexLiteral lit = (RexLiteral) adapted;
        BigDecimal value = lit.getValueAs(BigDecimal.class);
        assertNotNull(value);
        assertEquals("literal must carry Math.E", 0, value.compareTo(BigDecimal.valueOf(Math.E)));
        assertEquals("literal type must be DOUBLE", SqlTypeName.DOUBLE, lit.getType().getSqlTypeName());
    }

    public void testAdaptPassesThroughUnrelatedCall() {
        RexNode ref = rexBuilder.makeInputRef(doubleType, 0);
        RexCall absCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.ABS, List.of(ref));

        RexNode adapted = new EConstantAdapter().adapt(absCall, List.of(), cluster);

        assertSame(absCall, adapted);
    }
}
