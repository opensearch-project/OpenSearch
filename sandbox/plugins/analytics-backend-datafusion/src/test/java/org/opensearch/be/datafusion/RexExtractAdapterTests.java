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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RexExtractAdapter} and the shared
 * {@link RexExtractAdapter#validateLiteral} helper used by the multi/offset
 * adapters. Pins the literal-only-pattern contract: a column-valued pattern
 * or group must surface as an {@code IllegalArgumentException} at plan time,
 * not as a silent NULL row at runtime.
 */
public class RexExtractAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType varcharType;

    /**
     * Synthetic SQL plugin operator stand-in. The real
     * {@code PPLBuiltinOperators.REX_EXTRACT} lives in the SQL plugin and isn't
     * a build-time dep of analytics-backend-datafusion — the production
     * adapter is keyed on {@link org.opensearch.analytics.spi.ScalarFunction}
     * (matched by name), so any {@code SqlFunction} named {@code "REX_EXTRACT"}
     * exercises the same code path.
     */
    private static final SqlOperator INCOMING_REX_EXTRACT = new SqlFunction(
        "REX_EXTRACT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.STRING_STRING_STRING,
        SqlFunctionCategory.STRING
    );

    private final RexExtractAdapter adapter = new RexExtractAdapter();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    public void testAdaptRewritesToLocalOpAndPreservesOperands() {
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("(?<g>\\w+)");
        RexNode group = rexBuilder.makeLiteral("g");
        RexCall original = (RexCall) rexBuilder.makeCall(INCOMING_REX_EXTRACT, List.of(field, pattern, group));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue("adapted is a RexCall", adapted instanceof RexCall);
        RexCall result = (RexCall) adapted;
        assertSame(
            "operator rewritten to local rex_extract op so FunctionMappings.s fires",
            RexExtractAdapter.LOCAL_REX_EXTRACT_OP,
            result.getOperator()
        );
        assertEquals("operand count preserved", 3, result.getOperands().size());
        assertSame("input operand passed through", field, result.getOperands().get(0));
        assertSame("pattern literal passed through", pattern, result.getOperands().get(1));
        assertSame("group literal passed through", group, result.getOperands().get(2));
    }

    public void testAdaptRejectsColumnRefPattern() {
        // Column-valued pattern would force per-row regex compilation on the
        // Rust side. The adapter must reject this at plan time.
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode patternColRef = rexBuilder.makeInputRef(varcharType, 1);
        RexNode group = rexBuilder.makeLiteral("g");
        RexCall original = (RexCall) rexBuilder.makeCall(INCOMING_REX_EXTRACT, List.of(field, patternColRef, group));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> adapter.adapt(original, List.of(), cluster));
        assertTrue(
            "error message names the rejected operand: " + e.getMessage(),
            e.getMessage().contains("'pattern' must be a string literal")
        );
    }

    public void testAdaptRejectsColumnRefGroup() {
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("(?<g>\\w+)");
        RexNode groupColRef = rexBuilder.makeInputRef(varcharType, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(INCOMING_REX_EXTRACT, List.of(field, pattern, groupColRef));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> adapter.adapt(original, List.of(), cluster));
        assertTrue(
            "error message names the rejected operand: " + e.getMessage(),
            e.getMessage().contains("'group' must be a string literal")
        );
    }

    public void testAdaptRejectsWrongOperandCount() {
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("(?<g>\\w+)");
        // 2 operands instead of 3 — the SqlFunction signature would normally
        // catch this, but the adapter has its own arity check as a defense.
        RexCall original = (RexCall) rexBuilder.makeCall(INCOMING_REX_EXTRACT, List.of(field, pattern));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> adapter.adapt(original, List.of(), cluster));
        assertTrue(e.getMessage().contains("expected 3 operands"));
    }
}
