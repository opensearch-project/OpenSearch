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
 * Aggregate functions that a backend may support.
 * Used by the aggregate rule to verify the backend can handle
 * every {@link org.apache.calcite.rel.core.AggregateCall} in the plan.
 *
 * <p>Note: {@code COUNT} covers both {@code COUNT(*)} and {@code COUNT(DISTINCT x)}.
 * The distinction is on {@code AggregateCall.isDistinct()}, not on SqlKind.
 * Backends that only support non-distinct count should check distinctness
 * separately during fragment conversion.
 *
 * @opensearch.internal
 */
public enum AggregateFunction {
    SUM(SqlKind.SUM),
    SUM0(SqlKind.SUM0),
    MIN(SqlKind.MIN),
    MAX(SqlKind.MAX),
    COUNT(SqlKind.COUNT),
    APPROX_COUNT_DISTINCT(SqlKind.OTHER),
    AVG(SqlKind.AVG),
    STDDEV_POP(SqlKind.STDDEV_POP),
    STDDEV_SAMP(SqlKind.STDDEV_SAMP),
    VAR_POP(SqlKind.VAR_POP),
    VAR_SAMP(SqlKind.VAR_SAMP),
    PERCENTILE_CONT(SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(SqlKind.PERCENTILE_DISC),
    COLLECT(SqlKind.COLLECT),
    LISTAGG(SqlKind.LISTAGG);

    private final SqlKind sqlKind;

    AggregateFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
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

    /** Maps an aggregate function name to an AggregateFunction. Throws if not recognized. */
    public static AggregateFunction fromNameOrError(String name) {
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized aggregate function [" + name + "]", e);
        }
    }
}
