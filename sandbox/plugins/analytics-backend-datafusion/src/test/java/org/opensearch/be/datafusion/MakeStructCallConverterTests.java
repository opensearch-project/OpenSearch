/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.MakeStructFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;

/**
 * Substrait-serialization tests for {@link MakeStructCallConverter} — the layer both struct bugs
 * lived in, and one a plan-shape test can't reach (it stops before Substrait) nor a REST IT cheaply
 * (needs a live cluster plus the Rust native library).
 */
public class MakeStructCallConverterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private MakeStructCallConverter converter;

    /** Converts operands the way isthmus would; enough for asserting arity and shape. */
    private Function<RexNode, Expression> operandConverter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        SimpleExtension.ExtensionCollection extensions = SimpleExtension.load(List.of("/opensearch_struct_functions.yaml"));
        converter = new MakeStructCallConverter(extensions, TypeConverter.DEFAULT);
        // Stand-in for isthmus' recursive conversion; enough to count arguments.
        operandConverter = rex -> Expression.StrLiteral.builder().value(rex.toString()).build();
    }

    /** 100 fields → one invocation with 200 operands. Fails if struct construction is ever routed
     * back through signature matching, which cannot bind a variadic call of mixed operand types. */
    public void testWideStructHasNoArityCeiling() {
        int fieldCount = 100;
        Optional<Expression> converted = converter.convert(makeStructCall(fieldCount), operandConverter);

        assertTrue("converter must handle a 100-field struct", converted.isPresent());
        Expression.ScalarFunctionInvocation invocation = asInvocation(converted.get());
        assertEquals("one argument per name and per value", fieldCount * 2, invocation.arguments().size());
        assertEquals("named_struct", invocation.declaration().name());
    }

    /** The common narrow case still works, and keeps the interleaved (name, value) ordering. */
    public void testNarrowStructKeepsInterleavedOperands() {
        Optional<Expression> converted = converter.convert(makeStructCall(2), operandConverter);

        assertTrue(converted.isPresent());
        assertEquals(4, asInvocation(converted.get()).arguments().size());
    }

    /** Unrelated operators must fall through so other converters (and function matching) still run. */
    public void testDeclinesUnrelatedOperator() {
        RexNode left = rexBuilder.makeLiteral("a");
        RexNode right = rexBuilder.makeLiteral("b");
        RexCall unrelated = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right);

        assertFalse("EQUALS must not be claimed by the struct converter", converter.convert(unrelated, operandConverter).isPresent());
    }

    /** The converter's odd-operand guard is unreachable from our emitter: makeCall rejects
     * mismatched name/value lists first. Asserts that contract, since a hand-rolled odd-arity
     * RexCall isn't constructible under -ea. */
    public void testEmitterRejectsMismatchedNameAndValueCounts() {
        List<String> twoNames = List.of("f0", "f1");
        List<RexNode> oneValue = List.of(rexBuilder.makeLiteral("v0"));

        IllegalArgumentException thrown = expectThrows(
            IllegalArgumentException.class,
            () -> MakeStructFunction.makeCall(rexBuilder, structTypeOf(2), twoNames, oneValue)
        );
        assertTrue(
            "message should name the mismatch, got: " + thrown.getMessage(),
            thrown.getMessage().contains("one value per field name")
        );
    }

    /** No anchor declaration → decline, so it surfaces as "Unable to convert call" rather than an
     * NPE. Guards the coupling to opensearch_struct_functions.yaml being on the classpath. */
    public void testDeclinesWhenAnchorDeclarationMissing() throws Exception {
        MakeStructCallConverter withoutAnchor = new MakeStructCallConverter(
            SimpleExtension.ExtensionCollection.builder().build(),
            TypeConverter.DEFAULT
        );

        assertFalse(withoutAnchor.convert(makeStructCall(2), operandConverter).isPresent());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────

    /** {@code make_struct('f0', 'v0', 'f1', 'v1', …)} with {@code fieldCount} pairs. */
    private RexCall makeStructCall(int fieldCount) {
        List<String> names = new ArrayList<>(fieldCount);
        List<RexNode> values = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            names.add("f" + i);
            values.add(rexBuilder.makeLiteral("v" + i));
        }
        return (RexCall) MakeStructFunction.makeCall(rexBuilder, structTypeOf(fieldCount), names, values);
    }

    /** ROW type with {@code fieldCount} VARCHAR fields. */
    private RelDataType structTypeOf(int fieldCount) {
        List<RelDataType> types = new ArrayList<>(fieldCount);
        List<String> names = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
            names.add("f" + i);
        }
        return typeFactory.createStructType(types, names);
    }

    private static Expression.ScalarFunctionInvocation asInvocation(Expression expression) {
        assertTrue(
            "expected a ScalarFunctionInvocation, got " + expression.getClass().getSimpleName(),
            expression instanceof Expression.ScalarFunctionInvocation
        );
        return (Expression.ScalarFunctionInvocation) expression;
    }
}
