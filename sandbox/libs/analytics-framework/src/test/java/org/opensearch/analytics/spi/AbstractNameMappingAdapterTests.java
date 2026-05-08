/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link AbstractNameMappingAdapter}. Covers the basic rename path, the
 * prepend-literal form, and — most importantly — the {@link SqlTypeName#ANY} fallback
 * that kicks in when the incoming PPL UDF declares an indeterminate return type (e.g.
 * PPL's {@code SCALAR_MAX} / {@code SCALAR_MIN}). Without the fallback, Substrait
 * serialisation fails with {@code Unable to convert the type ANY}.
 */
public class AbstractNameMappingAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    private final RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

    /** Minimal concrete subclass for tests — pure rename, no prepend/append. */
    private static final class TestRenameAdapter extends AbstractNameMappingAdapter {
        TestRenameAdapter() {
            super(SqlLibraryOperators.GREATEST, List.of(), List.of());
        }
    }

    private SqlUserDefinedFunction pplUdf(String name, RelDataType returnType) {
        return new SqlUserDefinedFunction(
            new SqlIdentifier(name, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            opBinding -> returnType,
            null,
            null,
            null
        );
    }

    public void testBasicRename() {
        SqlUserDefinedFunction udf = pplUdf("SCALAR_MAX", doubleType);
        RexNode a = rexBuilder.makeInputRef(doubleType, 0);
        RexNode b = rexBuilder.makeInputRef(doubleType, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(udf, List.of(a, b));

        RexNode adapted = new TestRenameAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertSame(SqlLibraryOperators.GREATEST, adaptedCall.getOperator());
        assertEquals(2, adaptedCall.getOperands().size());
        assertSame(a, adaptedCall.getOperands().get(0));
        assertSame(b, adaptedCall.getOperands().get(1));
        assertSame("DOUBLE return type must be preserved", SqlTypeName.DOUBLE, adaptedCall.getType().getSqlTypeName());
    }

    public void testPrependLiteralOperand() {
        SqlFunction yearUdf = new SqlFunction(
            "YEAR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT_NULLABLE,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode ts = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
        RexCall original = (RexCall) rexBuilder.makeCall(yearUdf, List.of(ts));

        AbstractNameMappingAdapter adapter = new AbstractNameMappingAdapter(SqlLibraryOperators.DATE_PART, List.of("year"), List.of()) {
        };
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        RexCall adaptedCall = (RexCall) adapted;
        assertSame(SqlLibraryOperators.DATE_PART, adaptedCall.getOperator());
        assertEquals(2, adaptedCall.getOperands().size());
        assertTrue(adaptedCall.getOperands().get(0) instanceof RexLiteral);
        assertEquals("year", ((RexLiteral) adaptedCall.getOperands().get(0)).getValueAs(String.class));
        assertSame(ts, adaptedCall.getOperands().get(1));
    }

    /**
     * PPL's {@code SCALAR_MAX} / {@code SCALAR_MIN} declare their return type as
     * {@link SqlTypeName#ANY}. Substrait cannot serialise ANY; the adapter must fall back to
     * letting the target operator's own return-type inference run so the rewritten call
     * carries a concrete type derived from the operands.
     */
    public void testAdaptFallsBackToTargetInferenceForAnyReturnType() {
        RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
        SqlUserDefinedFunction udf = pplUdf("SCALAR_MAX", anyType);
        RexNode a = rexBuilder.makeInputRef(doubleType, 0);
        RexNode b = rexBuilder.makeInputRef(doubleType, 1);
        RexNode c = rexBuilder.makeInputRef(doubleType, 2);
        RexCall original = (RexCall) rexBuilder.makeCall(udf, List.of(a, b, c));
        assertSame("precondition: UDF return type must be ANY", SqlTypeName.ANY, original.getType().getSqlTypeName());

        RexNode adapted = new TestRenameAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertSame(SqlLibraryOperators.GREATEST, adaptedCall.getOperator());
        assertSame(
            "ANY return type must be replaced with a concrete operand-derived type after rewrite",
            SqlTypeName.DOUBLE,
            adaptedCall.getType().getSqlTypeName()
        );
    }

    /**
     * Pass-through for SIGN — a standard Calcite operator whose return type is already
     * concrete. The adapter still rewrites to the target operator (SignumFunction lives in
     * the backend; here we use SqlStdOperatorTable.SQRT as a stand-in target with a
     * concrete return type inferrer) and the preserved DOUBLE type proves the happy path.
     */
    public void testSignLikeRewritePreservesConcreteType() {
        RexNode arg = rexBuilder.makeInputRef(doubleType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SIGN, List.of(arg));

        AbstractNameMappingAdapter adapter = new AbstractNameMappingAdapter(SqlStdOperatorTable.SQRT, List.of(), List.of()) {
        };
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        RexCall adaptedCall = (RexCall) adapted;
        assertSame(SqlStdOperatorTable.SQRT, adaptedCall.getOperator());
        assertSame(SqlTypeName.DOUBLE, adaptedCall.getType().getSqlTypeName());
    }
}
