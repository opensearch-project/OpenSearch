/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlKind;

/**
 * Window functions a backend may support. Covers aggregate-as-window
 * (SUM/AVG/COUNT/MIN/MAX over a frame, what PPL {@code eventstats} lowers to)
 * plus ROW_NUMBER, used by PPL {@code dedup} (ROW_NUMBER OVER PARTITION BY … &lt;= N)
 * and emitted by PPL {@code streamstats … by …} as the helper sequence column
 * {@code __row_number_for_streamstats__}, plus three PPL-form aggregates that
 * {@code BackendPlanAdapter} rewrites into DataFusion's expected shape before
 * substrait emission:
 * <ul>
 *   <li>{@link #ARG_MIN} / {@link #ARG_MAX} — PPL {@code earliest()} / {@code latest()};
 *       adapter rewrites {@code ARG_MIN(value, ts)} to {@code FIRST_VALUE(value) ORDER BY ts ASC}
 *       and {@code ARG_MAX(value, ts)} to {@code LAST_VALUE(value) ORDER BY ts ASC}, both of
 *       which are DataFusion built-ins.</li>
 *   <li>{@link #DISTINCT_COUNT_APPROX} — PPL {@code dc()} / {@code distinct_count()};
 *       sql-plugin advertises this as a single user-defined aggregate function whose substrait
 *       shape DataFusion does not understand. Adapter rewrites it to standard {@code COUNT} with
 *       the {@code DISTINCT} flag set, which DataFusion's count.rs handles via per-type
 *       {@code DistinctCountAccumulator}.</li>
 * </ul>
 *
 * <p>PARTITION BY is allowed: {@code OpenSearchProject}'s cost gate forces SINGLETON
 * input on any RexOver-bearing project, so the coordinator's {@code WindowAggExec}
 * sees the full partition regardless of how partition keys span shards. Per-partition
 * the rewritten {@code count(distinct)} sees the full value set so its hash-based
 * deduplication is exact, and {@code first_value / last_value} see all rows so their
 * ORDER-BY-driven pick is deterministic.
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    SUM(SqlKind.SUM, "SUM"),
    AVG(SqlKind.AVG, "AVG"),
    COUNT(SqlKind.COUNT, "COUNT"),
    MIN(SqlKind.MIN, "MIN"),
    MAX(SqlKind.MAX, "MAX"),
    ARG_MIN(SqlKind.ARG_MIN, "ARG_MIN"),
    ARG_MAX(SqlKind.ARG_MAX, "ARG_MAX"),
    DISTINCT_COUNT_APPROX(SqlKind.OTHER_FUNCTION, "DISTINCT_COUNT_APPROX"),
    ROW_NUMBER(SqlKind.ROW_NUMBER, "ROW_NUMBER");

    private final SqlKind sqlKind;
    private final String operatorName;

    WindowFunction(SqlKind sqlKind, String operatorName) {
        this.sqlKind = sqlKind;
        this.operatorName = operatorName;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    public String getOperatorName() {
        return operatorName;
    }

    /**
     * Returns the {@link WindowFunction} for {@code sqlKind}, or {@code null} if unsupported.
     *
     * <p>Cannot tell {@link #DISTINCT_COUNT_APPROX} apart from any other UDAF (they all share
     * {@link SqlKind#OTHER_FUNCTION}), so callers that may see a UDAF should use
     * {@link #fromRexOver(RexOver)}.
     */
    public static WindowFunction fromSqlKind(SqlKind sqlKind) {
        if (sqlKind == SqlKind.OTHER_FUNCTION) {
            // Ambiguous: many UDAFs share OTHER_FUNCTION. Caller must use fromRexOver.
            return null;
        }
        for (WindowFunction fn : values()) {
            if (fn.sqlKind == sqlKind) return fn;
        }
        return null;
    }

    /**
     * Returns the {@link WindowFunction} for a {@link RexOver}. Resolves
     * {@link SqlKind#OTHER_FUNCTION} (UDAFs) by operator name so e.g. PPL's
     * {@code DISTINCT_COUNT_APPROX} UDAF maps to its enum entry.
     */
    public static WindowFunction fromRexOver(RexOver over) {
        SqlKind kind = over.getAggOperator().getKind();
        if (kind == SqlKind.OTHER_FUNCTION) {
            String name = over.getAggOperator().getName();
            for (WindowFunction fn : values()) {
                if (fn.sqlKind == SqlKind.OTHER_FUNCTION && fn.operatorName.equals(name)) {
                    return fn;
                }
            }
            return null;
        }
        return fromSqlKind(kind);
    }
}
