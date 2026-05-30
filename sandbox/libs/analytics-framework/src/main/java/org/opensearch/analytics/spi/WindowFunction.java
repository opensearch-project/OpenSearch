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

    /**
     * Resolves a {@link WindowFunction} from a {@link SqlOperator}. Falls back to
     * {@link #fromName(String)} when the operator's {@link SqlKind} is generic
     * ({@link SqlKind#OTHER} / {@link SqlKind#OTHER_FUNCTION}) so PPL UDAFs like
     * {@code DISTINCT_COUNT_APPROX} and {@code PATTERN} resolve by operator name.
     */
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
