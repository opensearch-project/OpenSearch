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
 * (SUM/AVG/COUNT/MIN/MAX over a frame) — what PPL {@code eventstats} lowers to —
 * plus ROW_NUMBER, which PPL {@code streamstats … by …} emits as the helper
 * sequence column {@code __row_number_for_streamstats__}.
 *
 * <p>PARTITION BY is now allowed by the planner: {@code OpenSearchProject}'s
 * cost gate already forces SINGLETON input on any RexOver-bearing project, so the
 * coordinator's {@code WindowAggExec} sees the entire partition regardless of
 * whether partition keys span shards. HASH-shuffle parallel execution is a future
 * strict improvement, not a correctness prerequisite.
 *
 * <p>Other ranking functions (RANK / DENSE_RANK) are not yet added — PPL doesn't
 * lower to them on the eventstats / streamstats paths exercised by analytics-engine
 * today. Add as needed.
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    SUM(SqlKind.SUM),
    AVG(SqlKind.AVG),
    COUNT(SqlKind.COUNT),
    MIN(SqlKind.MIN),
    MAX(SqlKind.MAX),
    ROW_NUMBER(SqlKind.ROW_NUMBER);

    private final SqlKind sqlKind;

    WindowFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Returns the {@link WindowFunction} for {@code sqlKind}, or {@code null} if unsupported. */
    public static WindowFunction fromSqlKind(SqlKind sqlKind) {
        for (WindowFunction fn : values()) {
            if (fn.sqlKind == sqlKind) return fn;
        }
        return null;
    }
}
