/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/**
 * Window functions a backend may support. Covers SUM/AVG/COUNT/MIN/MAX (PPL {@code eventstats}),
 * ROW_NUMBER (PPL {@code dedup} and {@code streamstats … by} helper), NTH_VALUE, plus the PPL-form
 * aggregates {@link #ARG_MIN} / {@link #ARG_MAX} ({@code earliest}/{@code latest}) and
 * {@link #DISTINCT_COUNT_APPROX} ({@code dc}/{@code distinct_count}) that backends rewrite via
 * {@link WindowFunctionAdapter} before substrait emission, and PATTERN (PPL {@code patterns}).
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    SUM(SqlKind.SUM),
    AVG(SqlKind.AVG),
    COUNT(SqlKind.COUNT),
    MIN(SqlKind.MIN),
    MAX(SqlKind.MAX),
    ARG_MIN(SqlKind.ARG_MIN),
    ARG_MAX(SqlKind.ARG_MAX),
    DISTINCT_COUNT_APPROX(SqlKind.OTHER_FUNCTION),
    ROW_NUMBER(SqlKind.ROW_NUMBER),
    NTH_VALUE(SqlKind.NTH_VALUE),
    PATTERN(SqlKind.OTHER);

    private final SqlKind sqlKind;

    WindowFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Resolves by SqlKind, falling back to operator name for generic OTHER / OTHER_FUNCTION kinds. */
    public static WindowFunction resolveFunction(SqlOperator operator) {
        WindowFunction fn = fromSqlKind(operator.getKind());
        if (fn != null) {
            return fn;
        }
        return fromName(operator.getName());
    }

    public static WindowFunction fromSqlKind(SqlKind sqlKind) {
        if (sqlKind == SqlKind.OTHER || sqlKind == SqlKind.OTHER_FUNCTION) {
            return null;
        }
        for (WindowFunction fn : values()) {
            if (fn.sqlKind == sqlKind) return fn;
        }
        return null;
    }

    public static WindowFunction fromName(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
