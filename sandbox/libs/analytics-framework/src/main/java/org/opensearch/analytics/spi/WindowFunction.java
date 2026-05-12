/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.HashMap;
import java.util.Map;

/**
 * Window functions a backend may support — distinct from {@link AggregateFunction}
 * because {@link org.apache.calcite.rex.RexOver} occurrences are evaluated by the
 * backend's window operator (Substrait inline {@code WindowFunctionInvocation}),
 * not as scalar projections or rolled-up aggregates.
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    /** Sequence number per window partition — backs PPL dedup's row-number filter. */
    ROW_NUMBER(SqlStdOperatorTable.ROW_NUMBER);

    private static final Map<SqlOperator, WindowFunction> OPERATOR_INDEX = new HashMap<>();

    static {
        for (WindowFunction fn : values()) {
            OPERATOR_INDEX.put(fn.operator, fn);
        }
    }

    private final SqlOperator operator;

    WindowFunction(SqlOperator operator) {
        this.operator = operator;
    }

    public SqlOperator operator() {
        return operator;
    }

    /** Resolves a Calcite {@link SqlOperator} to a {@link WindowFunction}, or {@code null} if unsupported. */
    public static WindowFunction fromOperator(SqlOperator operator) {
        return OPERATOR_INDEX.get(operator);
    }
}
