/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;

public class SubstraitPlanPojoRewriterTests extends OpenSearchTestCase {

    private static final TypeCreator R = TypeCreator.of(false);

    public void testTimestampPrecision6ConvertedTo3() {
        long epochMicros = 1704067200000000L; // 2024-01-01T00:00:00Z in micros
        long expectedMillis = 1704067200000L;

        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder()
            .value(epochMicros)
            .precision(6)
            .nullable(false)
            .build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) condition;
        assertEquals(3, pts.precision());
        assertEquals(expectedMillis, pts.value());
    }

    public void testTimestampPrecision9ConvertedTo3() {
        long epochNanos = 1704067200000000000L; // 2024-01-01T00:00:00Z in nanos
        long expectedMillis = 1704067200000L;

        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder().value(epochNanos).precision(9).nullable(false).build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) condition;
        assertEquals(3, pts.precision());
        assertEquals(expectedMillis, pts.value());
    }

    public void testTimestampPrecision3Unchanged() {
        long epochMillis = 1704067200000L;

        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder()
            .value(epochMillis)
            .precision(3)
            .nullable(false)
            .build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) condition;
        assertEquals(3, pts.precision());
        assertEquals(epochMillis, pts.value());
    }

    public void testTimestampInsideScalarFunction() {
        long epochMicros = 1704067200000000L;
        long expectedMillis = 1704067200000L;

        Expression tsLiteral = ImmutableExpression.PrecisionTimestampLiteral.builder()
            .value(epochMicros)
            .precision(6)
            .nullable(false)
            .build();

        FieldReference fieldRef = FieldReference.newRootStructReference(0, R.precisionTimestamp(3));

        SimpleExtension.ExtensionCollection extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        SimpleExtension.ScalarFunctionVariant gtFunc = extensions.getScalarFunction(
            SimpleExtension.FunctionAnchor.of(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "gt:any_any")
        );

        Expression gtCall = Expression.ScalarFunctionInvocation.builder()
            .declaration(gtFunc)
            .addArguments(fieldRef, tsLiteral)
            .outputType(R.BOOLEAN)
            .build();

        Plan plan = buildFilterPlan(gtCall);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.ScalarFunctionInvocation);
        Expression.ScalarFunctionInvocation rewrittenGt = (Expression.ScalarFunctionInvocation) condition;
        Expression arg1 = (Expression) rewrittenGt.arguments().get(1);
        assertTrue(arg1 instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) arg1;
        assertEquals(3, pts.precision());
        assertEquals(expectedMillis, pts.value());
    }

    public void testBareNameUnchanged() {
        NamedScan scan = NamedScan.builder()
            .names(List.of("parquet_dates"))
            .initialSchema(NamedStruct.of(List.of("col0"), R.struct(R.I64)))
            .build();

        Plan plan = buildPlan(scan);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        NamedScan rewrittenScan = (NamedScan) rewritten.getRoots().get(0).getInput();
        assertEquals(List.of("parquet_dates"), rewrittenScan.getNames());
    }

    public void testUnsupportedPrecisionThrows() {
        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder().value(12345L).precision(4).nullable(false).build();

        Plan plan = buildFilterPlan(literal);
        expectThrows(IllegalArgumentException.class, () -> SubstraitPlanPojoRewriter.rewrite(plan));
    }

    // --- VarCharLiteral → StrLiteral tests ---

    public void testVarCharLiteralConvertedToStrLiteralInFilter() {
        Expression varcharLiteral = ImmutableExpression.VarCharLiteral.builder().value("Sum").length(3).nullable(false).build();

        Plan plan = buildFilterPlan(varcharLiteral);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue("Expected StrLiteral, got " + condition.getClass(), condition instanceof Expression.StrLiteral);
        Expression.StrLiteral strLit = (Expression.StrLiteral) condition;
        assertEquals("Sum", strLit.value());
        assertFalse(strLit.nullable());
    }

    public void testVarCharLiteralConvertedToStrLiteralInProject() {
        // Simulates AddTotals label='Sum' scenario where VarCharLiteral appears in Project expressions
        NamedScan scan = NamedScan.builder()
            .names(List.of("test_table"))
            .initialSchema(NamedStruct.of(List.of("col0"), R.struct(R.I64)))
            .build();

        Expression varcharLiteral = ImmutableExpression.VarCharLiteral.builder().value("Total").length(5).nullable(false).build();

        Project project = Project.builder().input(scan).addExpressions(varcharLiteral).build();

        Plan plan = buildPlan(project);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Project rewrittenProject = (Project) rewritten.getRoots().get(0).getInput();
        Expression expr = rewrittenProject.getExpressions().get(0);
        assertTrue("Expected StrLiteral, got " + expr.getClass(), expr instanceof Expression.StrLiteral);
        Expression.StrLiteral strLit = (Expression.StrLiteral) expr;
        assertEquals("Total", strLit.value());
        assertFalse(strLit.nullable());
    }

    public void testNullableVarCharLiteralPreservesNullability() {
        Expression varcharLiteral = ImmutableExpression.VarCharLiteral.builder().value("nullable_value").length(14).nullable(true).build();

        Plan plan = buildFilterPlan(varcharLiteral);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.StrLiteral);
        Expression.StrLiteral strLit = (Expression.StrLiteral) condition;
        assertEquals("nullable_value", strLit.value());
        assertTrue("Nullability should be preserved", strLit.nullable());
    }

    public void testVarCharLiteralInsideScalarFunction() {
        // Tests VarCharLiteral within a function call (e.g., CASE expressions with string constants)
        Expression varcharLiteral = ImmutableExpression.VarCharLiteral.builder().value("label").length(5).nullable(false).build();

        FieldReference fieldRef = FieldReference.newRootStructReference(0, R.STRING);

        SimpleExtension.ExtensionCollection extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        SimpleExtension.ScalarFunctionVariant eqFunc = extensions.getScalarFunction(
            SimpleExtension.FunctionAnchor.of(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "equal:any_any")
        );

        Expression eqCall = Expression.ScalarFunctionInvocation.builder()
            .declaration(eqFunc)
            .addArguments(fieldRef, varcharLiteral)
            .outputType(R.BOOLEAN)
            .build();

        Plan plan = buildFilterPlan(eqCall);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.ScalarFunctionInvocation);
        Expression.ScalarFunctionInvocation rewrittenEq = (Expression.ScalarFunctionInvocation) condition;
        Expression arg1 = (Expression) rewrittenEq.arguments().get(1);
        assertTrue("Expected StrLiteral in function args, got " + arg1.getClass(), arg1 instanceof Expression.StrLiteral);
        Expression.StrLiteral strLit = (Expression.StrLiteral) arg1;
        assertEquals("label", strLit.value());
    }

    public void testMultipleVarCharLiteralsInProject() {
        // Tests multiple VarChar literals in a single Project (e.g., Chart/Trendline with multiple string options)
        NamedScan scan = NamedScan.builder()
            .names(List.of("test_table"))
            .initialSchema(NamedStruct.of(List.of("col0"), R.struct(R.I64)))
            .build();

        Expression varchar1 = ImmutableExpression.VarCharLiteral.builder().value("option1").length(7).nullable(false).build();
        Expression varchar2 = ImmutableExpression.VarCharLiteral.builder().value("option2").length(7).nullable(false).build();
        Expression varchar3 = ImmutableExpression.VarCharLiteral.builder().value("option3").length(7).nullable(true).build();

        Project project = Project.builder().input(scan).addExpressions(varchar1, varchar2, varchar3).build();

        Plan plan = buildPlan(project);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Project rewrittenProject = (Project) rewritten.getRoots().get(0).getInput();
        List<Expression> expressions = rewrittenProject.getExpressions();

        assertEquals(3, expressions.size());

        Expression.StrLiteral str1 = (Expression.StrLiteral) expressions.get(0);
        assertEquals("option1", str1.value());
        assertFalse(str1.nullable());

        Expression.StrLiteral str2 = (Expression.StrLiteral) expressions.get(1);
        assertEquals("option2", str2.value());
        assertFalse(str2.nullable());

        Expression.StrLiteral str3 = (Expression.StrLiteral) expressions.get(2);
        assertEquals("option3", str3.value());
        assertTrue(str3.nullable());
    }

    public void testStrLiteralUnchanged() {
        // Ensures existing StrLiterals are not modified
        Expression strLiteral = ImmutableExpression.StrLiteral.builder().value("already_string").nullable(false).build();

        Plan plan = buildFilterPlan(strLiteral);
        Plan rewritten = SubstraitPlanPojoRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.StrLiteral);
        Expression.StrLiteral strLit = (Expression.StrLiteral) condition;
        assertEquals("already_string", strLit.value());
        assertFalse(strLit.nullable());
    }

    // --- helpers ---

    private static Plan buildFilterPlan(Expression condition) {
        NamedScan scan = NamedScan.builder()
            .names(List.of("test_table"))
            .initialSchema(NamedStruct.of(List.of("col0"), R.struct(R.precisionTimestamp(3))))
            .build();

        Filter filter = Filter.builder().input(scan).condition(condition).build();

        return buildPlan(filter);
    }

    private static Plan buildPlan(io.substrait.relation.Rel rel) {
        Plan.Root root = Plan.Root.builder().input(rel).addNames("col0").build();
        return Plan.builder().addRoots(root).build();
    }

    private static Expression getFilterCondition(Plan plan) {
        Filter filter = (Filter) plan.getRoots().get(0).getInput();
        return filter.getCondition();
    }
}
