/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;

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
    COUNT(Type.SIMPLE, SqlKind.COUNT),
    AVG(Type.SIMPLE, SqlKind.AVG),

    // Statistical — fixed-size state, multi-pass or running stats
    STDDEV_POP(Type.STATISTICAL, SqlKind.STDDEV_POP),
    STDDEV_SAMP(Type.STATISTICAL, SqlKind.STDDEV_SAMP),
    VAR_POP(Type.STATISTICAL, SqlKind.VAR_POP),
    VAR_SAMP(Type.STATISTICAL, SqlKind.VAR_SAMP),

    // Simple — first/last value semantics. PPL emits SqlAggFunction named "first" /
    // "last"; NAME_ALIASES in NameBasedAggregateFunctionConverter rewrites those to
    // DataFusion's "first_value"/"last_value" before substrait emission. Planner-side
    // lookup goes via AggregateFunction.fromNameOrError("FIRST") / ...("LAST").
    FIRST(Type.SIMPLE, SqlKind.OTHER),
    LAST(Type.SIMPLE, SqlKind.OTHER),

    // State-expanding — state grows with input rows per key
    PERCENTILE_CONT(Type.STATE_EXPANDING, SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(Type.STATE_EXPANDING, SqlKind.PERCENTILE_DISC),
    COLLECT(Type.STATE_EXPANDING, SqlKind.COLLECT),
    LISTAGG(Type.STATE_EXPANDING, SqlKind.LISTAGG),
    TAKE(Type.STATE_EXPANDING, SqlKind.OTHER),
    // PPL `list(field)` and `values(field)` — NAME_ALIASES in
    // NameBasedAggregateFunctionConverter rewrites both to DataFusion's native
    // "array_agg" on the substrait wire. Planner-side lookup goes via
    // fromNameOrError("LIST") / ("VALUES"). VALUES additionally gets
    // DISTINCT + ORDER BY forced by AliasConfig; LIST is a pure rename.
    LIST(Type.STATE_EXPANDING, SqlKind.OTHER),
    VALUES(Type.STATE_EXPANDING, SqlKind.OTHER),

    // Approximate — probabilistic, fixed-size state
    APPROX_COUNT_DISTINCT(Type.APPROXIMATE, SqlKind.OTHER);

    /** Category of aggregate function. Affects execution strategy (shuffle vs map-reduce). */
    public enum Type {
        SIMPLE,
        STATISTICAL,
        STATE_EXPANDING,
        APPROXIMATE
    }

    private final Type type;
    private final SqlKind sqlKind;

    AggregateFunction(Type type, SqlKind sqlKind) {
        this.type = type;
        this.sqlKind = sqlKind;
    }

    public Type getType() {
        return type;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
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

    /** Maps an aggregate function name to an AggregateFunction. Throws if not recognized.
     *  Lookup is case-insensitive — Calcite SqlAggFunction names are lowercase
     *  while enum constants follow Java convention (uppercase). */
    public static AggregateFunction fromNameOrError(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized aggregate function [" + name + "]", e);
        }
    }
}
