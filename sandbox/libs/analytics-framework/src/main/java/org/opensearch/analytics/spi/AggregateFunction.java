/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Aggregate functions that a backend may support, categorized by {@link Type}.
 *
 * <p>Note: {@code COUNT} covers both {@code COUNT(*)} and {@code COUNT(DISTINCT x)}.
 * The distinction is on {@code AggregateCall.isDistinct()}, not on SqlKind.
 *
 * @opensearch.internal
 */
public enum AggregateFunction {
    // Simple — fixed-size state per key
    SUM(Type.SIMPLE, SqlKind.SUM),
    SUM0(Type.SIMPLE, SqlKind.SUM0),
    MIN(Type.SIMPLE, SqlKind.MIN),
    MAX(Type.SIMPLE, SqlKind.MAX),
    COUNT(Type.SIMPLE, SqlKind.COUNT, fields(IF("count", new ArrowType.Int(64, true), SUM)), null),
    AVG(
        Type.SIMPLE,
        SqlKind.AVG,
        fields(IF("count", new ArrowType.Int(64, true), SUM), IF("sum", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), SUM)),
        (rb, refs) -> rb.makeCall(SqlStdOperatorTable.DIVIDE, refs.get(1), refs.get(0))
    ),

    // Statistical — fixed-size state, multi-pass or running stats
    STDDEV_POP(Type.STATISTICAL, SqlKind.STDDEV_POP),
    STDDEV_SAMP(Type.STATISTICAL, SqlKind.STDDEV_SAMP),
    VAR_POP(Type.STATISTICAL, SqlKind.VAR_POP),
    VAR_SAMP(Type.STATISTICAL, SqlKind.VAR_SAMP),

    // State-expanding — state grows with input rows per key
    PERCENTILE_CONT(Type.STATE_EXPANDING, SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(Type.STATE_EXPANDING, SqlKind.PERCENTILE_DISC),
    COLLECT(Type.STATE_EXPANDING, SqlKind.COLLECT),
    LISTAGG(Type.STATE_EXPANDING, SqlKind.LISTAGG),

    // Approximate — probabilistic, fixed-size state
    APPROX_COUNT_DISTINCT(
        Type.APPROXIMATE,
        SqlKind.OTHER,
        fields(IF("sketch", new ArrowType.Binary(), null)),  // null reducer = self
        null
    );

    /** Category of aggregate function. Affects execution strategy (shuffle vs map-reduce). */
    public enum Type {
        SIMPLE,
        STATISTICAL,
        STATE_EXPANDING,
        APPROXIMATE
    }

    /** Describes one intermediate field emitted by a partial aggregate. A null reducer means "self" (the owning enum constant). */
    public record IntermediateField(String name, ArrowType arrowType, AggregateFunction reducer) {
    }

    private final Type type;
    private final SqlKind sqlKind;
    private final List<IntermediateField> intermediateFields;
    private final BiFunction<RexBuilder, List<RexNode>, RexNode> finalExpression;

    AggregateFunction(Type type, SqlKind sqlKind) {
        this(type, sqlKind, null, null);
    }

    AggregateFunction(
        Type type,
        SqlKind sqlKind,
        List<IntermediateField> intermediateFields,
        BiFunction<RexBuilder, List<RexNode>, RexNode> finalExpression
    ) {
        this.type = type;
        this.sqlKind = sqlKind;
        this.intermediateFields = intermediateFields;
        this.finalExpression = finalExpression;
    }

    public Type getType() {
        return type;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Returns intermediate fields with null reducers resolved to {@code this}. */
    public List<IntermediateField> intermediateFields() {
        if (intermediateFields == null) return null;
        return intermediateFields.stream()
            .map(f -> f.reducer() == null ? new IntermediateField(f.name(), f.arrowType(), this) : f)
            .toList();
    }

    public BiFunction<RexBuilder, List<RexNode>, RexNode> finalExpression() {
        return finalExpression;
    }

    public boolean hasDecomposition() {
        return intermediateFields != null;
    }

    public boolean hasScalarFinal() {
        return finalExpression != null;
    }

    public boolean hasBinaryIntermediate() {
        return intermediateFields != null && intermediateFields.stream().anyMatch(f -> f.arrowType() instanceof ArrowType.Binary);
    }

    /** Maps a Calcite SqlKind to an AggregateFunction, or null if not recognized. Skips OTHER. */
    public static AggregateFunction fromSqlKind(SqlKind kind) {
        for (AggregateFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /** Maps an aggregate function name to an AggregateFunction. Throws if not recognized. */
    public static AggregateFunction fromNameOrError(String name) {
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized aggregate function [" + name + "]", e);
        }
    }

    // ── Helpers for readable enum-entry literals ──

    private static List<IntermediateField> fields(IntermediateField... fs) {
        return List.of(fs);
    }

    private static IntermediateField IF(String name, ArrowType arrowType, AggregateFunction reducer) {
        return new IntermediateField(name, arrowType, reducer);
    }
}
