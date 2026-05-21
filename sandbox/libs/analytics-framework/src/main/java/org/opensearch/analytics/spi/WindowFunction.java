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
 * Window functions a backend may support. Covers aggregate-as-window
 * (SUM/AVG/COUNT/MIN/MAX over a frame, what PPL {@code eventstats} lowers to)
 * plus ROW_NUMBER, used by PPL {@code dedup} (ROW_NUMBER OVER PARTITION BY … &lt;= N)
 * and emitted by PPL {@code streamstats … by …} as the helper sequence column
 * {@code __row_number_for_streamstats__}.
 *
 * <p>PARTITION BY is allowed: {@code OpenSearchProject}'s cost gate forces SINGLETON
 * input on any RexOver-bearing project, so the coordinator's {@code WindowAggExec}
 * sees the full partition regardless of how partition keys span shards.
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    SUM(SqlKind.SUM),
    AVG(SqlKind.AVG),
    COUNT(SqlKind.COUNT),
    MIN(SqlKind.MIN),
    MAX(SqlKind.MAX),
    ROW_NUMBER(SqlKind.ROW_NUMBER),
    /**
     * PPL {@code patterns ... method=brain mode=label}. Emits the best-matching
     * BRAIN-derived wildcard pattern for each row in the partition, broadcast
     * window-style. Resolves through PPL's {@code BuiltinFunctionName.INTERNAL_PATTERN}
     * with {@link SqlKind#OTHER}, so backends look it up via {@link #fromName} (the
     * operator name {@code "pattern"} is the canonical match).
     */
    PATTERN(SqlKind.OTHER);

    private final SqlKind sqlKind;

    WindowFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Returns the {@link WindowFunction} for {@code sqlKind}, or {@code null} if unsupported. */
    public static WindowFunction fromSqlKind(SqlKind sqlKind) {
        if (sqlKind == SqlKind.OTHER || sqlKind == SqlKind.OTHER_FUNCTION) {
            // Name-based lookup is required for OTHER kinds; the caller (e.g.
            // {@code OpenSearchProjectRule.collectWindowFunctions}) should fall
            // through to {@link #fromName} on null here.
            return null;
        }
        for (WindowFunction fn : values()) {
            if (fn.sqlKind == sqlKind) return fn;
        }
        return null;
    }

    /**
     * Returns the {@link WindowFunction} for a case-insensitive operator name
     * (e.g. {@code "pattern"} → {@link #PATTERN}), or {@code null} if unsupported.
     * PPL emits some window operators with {@link SqlKind#OTHER}, so callers
     * fall back here when {@link #fromSqlKind} returns null.
     */
    public static WindowFunction fromName(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
