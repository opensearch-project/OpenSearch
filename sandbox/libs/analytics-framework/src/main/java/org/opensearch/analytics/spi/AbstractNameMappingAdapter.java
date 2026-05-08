/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Reusable base for {@link ScalarFunctionAdapter}s that rewrite a Calcite call
 * to a different named target, optionally prepending or appending literal
 * operands. Pure shape rewriting — no decomposition into a different semantic
 * function. For that use case (e.g. {@code ILIKE → LIKE(LOWER(a), LOWER(b))})
 * write a dedicated adapter instead.
 *
 * <p>Example use:
 * <pre>
 *   class YearAdapter extends AbstractNameMappingAdapter {
 *       YearAdapter() {
 *           super(SqlLibraryOperators.DATE_PART, List.of("year"), List.of());
 *       }
 *   }
 * </pre>
 * rewrites {@code YEAR(ts)} to {@code date_part('year', ts)}. Paired with the
 * {@code date_part} signature in a backend's extension catalog so the isthmus
 * visitor resolves it against the backend's native date_part.
 *
 * @opensearch.internal
 */
public abstract class AbstractNameMappingAdapter implements ScalarFunctionAdapter {

    private final SqlOperator targetOperator;
    private final List<Object> prependLiterals;
    private final List<Object> appendLiterals;

    /**
     * @param targetOperator  the Calcite {@link SqlOperator} the rewritten call
     *                        will use. The isthmus visitor resolves this to a
     *                        Substrait invocation against the backend's loaded
     *                        extension catalog.
     * @param prependLiterals literals to prepend to the operand list (e.g.
     *                        {@code List.of("year")} to prepend a string literal).
     *                        Currently supports {@link String}, {@link Integer},
     *                        {@link Long}, {@link Double}, {@link Boolean}.
     * @param appendLiterals  literals to append to the operand list.
     */
    protected AbstractNameMappingAdapter(SqlOperator targetOperator, List<Object> prependLiterals, List<Object> appendLiterals) {
        this.targetOperator = targetOperator;
        this.prependLiterals = List.copyOf(prependLiterals);
        this.appendLiterals = List.copyOf(appendLiterals);
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> operands = new ArrayList<>(original.getOperands().size() + prependLiterals.size() + appendLiterals.size());
        for (Object literal : prependLiterals) {
            operands.add(rexBuilder.makeLiteral(literal, inferLiteralType(rexBuilder, literal), true));
        }
        operands.addAll(original.getOperands());
        for (Object literal : appendLiterals) {
            operands.add(rexBuilder.makeLiteral(literal, inferLiteralType(rexBuilder, literal), true));
        }
        // Preserve the original call's return type. The enclosing operator (Project
        // / Filter) caches its rowType from the pre-adaptation expression; if the
        // rewritten call's Calcite-inferred type differs (e.g. PPL YEAR returns
        // INTEGER but SqlLibraryOperators.DATE_PART is SqlExtractFunction → BIGINT),
        // the downstream stripAnnotations path feeds the adapted expr into
        // LogicalProject.create together with the cached rowType, and
        // Project.isValid's compatibleTypes check throws an AssertionError that
        // breaks fragment conversion.
        //
        // Exception: polymorphic PPL UDFs (e.g. SCALAR_MAX, SCALAR_MIN) declare
        // their return type as SqlTypeName.ANY because they accept heterogeneous
        // operand shapes. Substrait cannot serialise ANY, so fall back to the
        // target operator's own return-type inference — the result will be a
        // concrete type derived from operands (DOUBLE for GREATEST(DOUBLE, DOUBLE),
        // etc.) which Substrait can serialise.
        if (original.getType().getSqlTypeName() == SqlTypeName.ANY) {
            return rexBuilder.makeCall(targetOperator, operands);
        }
        return rexBuilder.makeCall(original.getType(), targetOperator, operands);
    }

    private static org.apache.calcite.rel.type.RelDataType inferLiteralType(RexBuilder rexBuilder, Object literal) {
        var typeFactory = rexBuilder.getTypeFactory();
        if (literal instanceof String) return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        if (literal instanceof Integer) return typeFactory.createSqlType(SqlTypeName.INTEGER);
        if (literal instanceof Long) return typeFactory.createSqlType(SqlTypeName.BIGINT);
        if (literal instanceof Double) return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        if (literal instanceof Boolean) return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        throw new IllegalArgumentException("Unsupported literal type: " + literal.getClass());
    }
}
