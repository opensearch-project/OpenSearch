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
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;

public class SubstraitPlanRewriterTests extends OpenSearchTestCase {

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
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

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
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

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
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

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
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

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
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        NamedScan rewrittenScan = (NamedScan) rewritten.getRoots().get(0).getInput();
        assertEquals(List.of("parquet_dates"), rewrittenScan.getNames());
    }

    public void testUnsupportedPrecisionThrows() {
        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder().value(12345L).precision(4).nullable(false).build();

        Plan plan = buildFilterPlan(literal);
        expectThrows(IllegalArgumentException.class, () -> SubstraitPlanRewriter.rewrite(plan));
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
